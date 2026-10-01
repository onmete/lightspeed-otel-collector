package agenticexporter

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	recoveryInitialBackoff = time.Second
	recoveryMaxBackoff     = 30 * time.Second
	maxShutdownFlush       = 30 * time.Second
)

type streamState int64

const (
	streamUnavailable streamState = 0
	streamHealthy     streamState = 1
	streamDegraded    streamState = 2
)

type fileOperation string

const (
	opMkdir        fileOperation = "mkdir"
	opOpen         fileOperation = "open"
	opRead         fileOperation = "read"
	opWrite        fileOperation = "write"
	opClose        fileOperation = "close"
	opRename       fileOperation = "rename"
	opRemoveTmp    fileOperation = "remove_tmp"
	opRemoveSource fileOperation = "remove_source"
)

type queuedRecord struct {
	data []byte
}

type streamSnapshot struct {
	state streamState

	queueHighWaterBytes int64
	unpublishedRecords  int64
	unpublishedBytes    int64
	openBatchRecords    int64
	openBatchBytes      int64
	openBatchAge        time.Duration

	readyFilesCreated   int64
	readyRecordsCreated int64
	readyBytesCreated   int64

	operationFailures map[fileOperation]int64
}

type streamCallbacks struct {
	stateChanged     func(streamState, streamState, fileOperation)
	operationFailed  func(fileOperation)
	published        func(int64, int64)
	shutdownDeadline func(int64, int64)
}

type fileOps interface {
	mkdirAll(string, fs.FileMode) error
	openExclusive(string, fs.FileMode) (batchFile, error)
	openRead(string) (io.ReadCloser, error)
	chmod(string, fs.FileMode) error
	rename(string, string) error
	remove(string) error
	readDir(string) ([]fs.DirEntry, error)
	stat(string) (fs.FileInfo, error)
}

type batchFile interface {
	Write([]byte) (int, error)

	Close() error
}
type managedBatchFile struct {
	batchFile
	once sync.Once
}

func (f *managedBatchFile) Close() error {
	var err error
	closed := false
	f.once.Do(func() {
		closed = true
		err = f.batchFile.Close()
	})
	if !closed {
		return nil
	}
	return err
}

type managedReadCloser struct {
	io.ReadCloser
	once sync.Once
}

func (r *managedReadCloser) Close() error {
	var err error
	closed := false
	r.once.Do(func() {
		closed = true
		err = r.ReadCloser.Close()
	})
	if !closed {
		return nil
	}
	return err
}

type shutdownAwareReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r shutdownAwareReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return 0, ctxErr
	}
	return n, err
}

type shutdownAwareWriter struct {
	ctx    context.Context
	writer io.Writer
}

func (w shutdownAwareWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := w.writer.Write(p)
	if ctxErr := w.ctx.Err(); ctxErr != nil {
		return n, ctxErr
	}
	return n, err
}

type osFileOps struct{}

func (osFileOps) mkdirAll(path string, mode fs.FileMode) error {
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return nil
	}
	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}
	return os.Chmod(path, mode)
}
func (osFileOps) openExclusive(path string, mode fs.FileMode) (batchFile, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
}
func (osFileOps) openRead(path string) (io.ReadCloser, error) { return os.Open(path) }
func (osFileOps) chmod(path string, mode fs.FileMode) error   { return os.Chmod(path, mode) }
func (osFileOps) rename(oldPath, newPath string) error        { return os.Rename(oldPath, newPath) }
func (osFileOps) remove(path string) error                    { return os.Remove(path) }
func (osFileOps) stat(path string) (fs.FileInfo, error)       { return os.Lstat(path) }
func (osFileOps) readDir(path string) ([]fs.DirEntry, error) {
	return os.ReadDir(path)
}

type writerTimer interface {
	C() <-chan time.Time
	Reset(time.Duration)
	Stop()
}

type writerClock interface {
	now() time.Time
	newTimer(time.Duration) writerTimer
}

type realWriterClock struct{}

type realWriterTimer struct{ timer *time.Timer }

func (realWriterClock) now() time.Time { return time.Now() }
func (realWriterClock) newTimer(d time.Duration) writerTimer {
	return &realWriterTimer{timer: time.NewTimer(d)}
}
func (t *realWriterTimer) C() <-chan time.Time { return t.timer.C }
func (t *realWriterTimer) Reset(d time.Duration) {
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
	t.timer.Reset(d)
}
func (t *realWriterTimer) Stop() {
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
}

var (
	canonicalSourceTmpName = regexp.MustCompile(`^\.[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.jsonl\.tmp$`)
	canonicalArrayTmpName  = regexp.MustCompile(`^\.[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.json\.tmp$`)
)

type shutdownRequest struct {
	ctx  context.Context
	done chan struct{}
}

type streamWriter struct {
	stagingDirectory  string
	readyDirectory    string
	maxBacklogBytes   int64
	maxFileBytes      int64
	maxPublishedBytes int64
	maxBatchAge       time.Duration

	ops        fileOps
	clock      writerClock
	callbacks  streamCallbacks
	workCtx    context.Context
	workCancel context.CancelFunc

	callbackMu    sync.Mutex
	publicationMu sync.Mutex
	handleMu      sync.Mutex
	handlesClosed bool

	mu                  sync.Mutex
	queued              []queuedRecord
	active              []queuedRecord
	unpublishedRecords  int64
	unpublishedBytes    int64
	highWaterBytes      int64
	state               streamState
	operationFailures   map[fileOperation]int64
	readyFilesCreated   int64
	readyRecordsCreated int64
	readyBytesCreated   int64
	activeBytes         int64
	activeStarted       time.Time
	accepting           bool
	started             bool
	shutdownReported    bool

	notify           chan struct{}
	shutdownRequests chan shutdownRequest

	sourceFile           batchFile
	sourceReader         io.ReadCloser
	arrayFile            batchFile
	sourceTempPath       string
	sourcePath           string
	arrayTempPath        string
	finalPath            string
	sourceClosed         bool
	sourceSealed         bool
	arrayClosed          bool
	publicationCommitted bool
	arrayBytes           int64
	written              int
	failedOp             fileOperation
	failedPath           string
	preflightOnRecovery  bool
	backoff              time.Duration
}

func newStreamWriter(stagingDirectory, readyDirectory string, maxBacklogBytes int64) *streamWriter {
	workCtx, workCancel := context.WithCancel(context.Background())
	return &streamWriter{
		stagingDirectory:  stagingDirectory,
		readyDirectory:    readyDirectory,
		maxBacklogBytes:   maxBacklogBytes,
		maxFileBytes:      maxFileBytes,
		maxPublishedBytes: maxPublishedFileBytes,
		maxBatchAge:       maxBatchAge,
		ops:               osFileOps{},
		clock:             realWriterClock{},
		workCtx:           workCtx,
		workCancel:        workCancel,
		state:             streamUnavailable,
		operationFailures: make(map[fileOperation]int64),
		notify:            make(chan struct{}, 1),
		shutdownRequests:  make(chan shutdownRequest, 1),
		accepting:         true,
		backoff:           recoveryInitialBackoff,
	}
}

func (w *streamWriter) registerSourceFile(file batchFile) batchFile {
	handle := &managedBatchFile{batchFile: file}
	w.handleMu.Lock()
	if !w.handlesClosed {
		w.sourceFile = handle
		w.handleMu.Unlock()
		return handle
	}
	w.handleMu.Unlock()
	_ = handle.Close()
	return nil
}

func (w *streamWriter) registerArrayFile(file batchFile) batchFile {
	handle := &managedBatchFile{batchFile: file}
	w.handleMu.Lock()
	if !w.handlesClosed {
		w.arrayFile = handle
		w.handleMu.Unlock()
		return handle
	}
	w.handleMu.Unlock()
	_ = handle.Close()
	return nil
}

func (w *streamWriter) registerSourceReader(reader io.ReadCloser) io.ReadCloser {
	handle := &managedReadCloser{ReadCloser: reader}
	w.handleMu.Lock()
	if !w.handlesClosed {
		w.sourceReader = handle
		w.handleMu.Unlock()
		return handle
	}
	w.handleMu.Unlock()
	_ = handle.Close()
	return nil
}

func (w *streamWriter) sourceFileHandle() batchFile {
	w.handleMu.Lock()
	defer w.handleMu.Unlock()
	return w.sourceFile
}

func (w *streamWriter) arrayFileHandle() batchFile {
	w.handleMu.Lock()
	defer w.handleMu.Unlock()
	return w.arrayFile
}

func (w *streamWriter) clearSourceFileHandle() {
	w.handleMu.Lock()
	w.sourceFile = nil
	w.handleMu.Unlock()
}

func (w *streamWriter) clearArrayFileHandle() {
	w.handleMu.Lock()
	w.arrayFile = nil
	w.handleMu.Unlock()
}

func (w *streamWriter) clearSourceReaderHandle() {
	w.handleMu.Lock()
	w.sourceReader = nil
	w.handleMu.Unlock()
}

func (w *streamWriter) clearPrivateHandles() {
	w.handleMu.Lock()
	w.sourceFile = nil
	w.sourceReader = nil
	w.arrayFile = nil
	w.handleMu.Unlock()
}

func (w *streamWriter) closePrivateHandles() {
	w.handleMu.Lock()
	w.handlesClosed = true
	sourceFile, sourceReader, arrayFile := w.sourceFile, w.sourceReader, w.arrayFile
	w.handleMu.Unlock()

	if sourceFile != nil {
		if err := sourceFile.Close(); err != nil {
			w.recordFailure(opClose)
		}
	}
	if sourceReader != nil {
		if err := sourceReader.Close(); err != nil {
			w.recordFailure(opClose)
		}
	}
	if arrayFile != nil {
		if err := arrayFile.Close(); err != nil {
			w.recordFailure(opClose)
		}
	}
}

// On success, tryEnqueue takes immutable ownership of record. On failure,
// ownership remains with the caller.
func (w *streamWriter) tryEnqueue(record []byte) bool {
	if len(record) == 0 || record[len(record)-1] != '\n' {
		return false
	}

	w.mu.Lock()
	if !w.accepting || (w.unpublishedBytes != 0 && (int64(len(record)) > w.maxBacklogBytes-w.unpublishedBytes)) {
		w.mu.Unlock()
		return false
	}
	w.queued = append(w.queued, queuedRecord{data: record})
	w.unpublishedRecords++
	w.unpublishedBytes += int64(len(record))
	if w.unpublishedBytes > w.highWaterBytes {
		w.highWaterBytes = w.unpublishedBytes
	}
	notify := w.state == streamHealthy
	w.mu.Unlock()

	if !notify {
		return true
	}

	select {
	case w.notify <- struct{}{}:
	default:
	}
	return true
}

func (w *streamWriter) start(context.Context) error {
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return nil
	}
	w.started = true
	w.mu.Unlock()

	w.preflight()
	go w.run()
	return nil
}

func (w *streamWriter) shutdownWriter(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	flushCtx := ctx
	cancel := func() {}
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		flushCtx, cancel = context.WithCancel(ctx)
	} else {
		flushCtx, cancel = context.WithTimeout(ctx, maxShutdownFlush)
	}
	defer cancel()

	w.mu.Lock()
	w.accepting = false
	started := w.started
	w.mu.Unlock()
	if !started {
		w.workCancel()
		w.reportShutdownDeadline()
		return
	}

	req := shutdownRequest{ctx: flushCtx, done: make(chan struct{})}
	// The channel is buffered and lifecycle shutdown is single-shot. Always
	// hand the request to the worker, even when the deadline has already
	// expired, so it exits without leaking a goroutine or an open handle.
	w.shutdownRequests <- req
	select {
	case <-req.done:
		w.workCancel()
	case <-flushCtx.Done():
		w.stopAtShutdownDeadline()
		// A synchronous filesystem call already in progress may not be
		// interruptible. Keep ownership ordered before exporter teardown rather
		// than abandoning the worker to publish or call back after shutdown.
		<-req.done
	}
}

func (w *streamWriter) shutdown(ctx context.Context) { w.shutdownWriter(ctx) }

func (w *streamWriter) snapshot() streamSnapshot {
	w.mu.Lock()
	defer w.mu.Unlock()
	failures := make(map[fileOperation]int64, len(w.operationFailures))
	for operation, count := range w.operationFailures {
		failures[operation] = count
	}
	age := time.Duration(0)
	if !w.activeStarted.IsZero() {
		age = w.clock.now().Sub(w.activeStarted)
		if age < 0 {
			age = 0
		}
	}
	return streamSnapshot{
		state:               w.state,
		queueHighWaterBytes: w.highWaterBytes,
		unpublishedRecords:  w.unpublishedRecords,
		unpublishedBytes:    w.unpublishedBytes,
		openBatchRecords:    int64(len(w.active)),
		openBatchBytes:      w.activeBytes,
		openBatchAge:        age,
		readyFilesCreated:   w.readyFilesCreated,
		readyRecordsCreated: w.readyRecordsCreated,
		readyBytesCreated:   w.readyBytesCreated,
		operationFailures:   failures,
	}
}

func (w *streamWriter) run() {
	timer := w.clock.newTimer(time.Hour)
	timer.Stop()
	var timerC <-chan time.Time
	for {
		// Give shutdown requests precedence over an already-due batch-age timer.
		select {
		case req := <-w.shutdownRequests:
			timer.Stop()
			w.flushForShutdown(req.ctx)
			close(req.done)
			return
		default:
		}
		w.mu.Lock()
		state := w.state
		w.mu.Unlock()
		if state == streamHealthy {
			w.processHealthy(false)
		}
		delay, scheduled := w.nextDelay()
		if scheduled {
			timer.Reset(delay)
			timerC = timer.C()
		} else {
			timer.Stop()
			timerC = nil
		}

		if timerC != nil {
			select {
			case <-timerC:
				w.handleTimer()
				continue
			default:
			}
		}

		select {
		case <-w.notify:
		case <-timerC:
			w.handleTimer()
		case req := <-w.shutdownRequests:
			timer.Stop()
			w.flushForShutdown(req.ctx)
			close(req.done)
			return
		}
	}
}

func (w *streamWriter) handleTimer() {
	w.mu.Lock()
	degraded := w.state == streamDegraded
	w.mu.Unlock()
	if degraded {
		w.probeRecovery()
	} else {
		w.processHealthy(false)
	}
}

func (w *streamWriter) nextDelay() (time.Duration, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state == streamDegraded {
		return w.backoff, true
	}
	if w.state == streamHealthy && !w.activeStarted.IsZero() {
		d := w.maxBatchAge - w.clock.now().Sub(w.activeStarted)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}

func (w *streamWriter) processHealthy(force bool) {
	for {
		if w.workCtx.Err() != nil {
			return
		}

		w.mu.Lock()
		if w.state != streamHealthy {
			w.mu.Unlock()
			return
		}
		if len(w.active) == 0 && len(w.queued) != 0 {
			w.active = append(w.active, w.queued[0])
			w.queued[0] = queuedRecord{}
			w.queued = w.queued[1:]
		}
		hasActive := len(w.active) != 0
		w.mu.Unlock()
		if !hasActive {
			return
		}
		if !w.writePending() {
			return
		}
		if w.workCtx.Err() != nil {
			return
		}

		w.mu.Lock()
		publish := w.activeBytes >= w.maxFileBytes || (force && len(w.queued) == 0)
		if !publish && !w.activeStarted.IsZero() && w.clock.now().Sub(w.activeStarted) >= w.maxBatchAge {
			publish = true
		}
		if !publish && len(w.queued) != 0 {
			nextBytes := int64(len(w.queued[0].data))
			if !w.arrayProjectionFits(w.activeBytes, nextBytes) {
				publish = true
			} else {
				w.active = append(w.active, w.queued[0])
				w.queued[0] = queuedRecord{}
				w.queued = w.queued[1:]
			}
		}
		w.mu.Unlock()
		if publish {
			if !w.publishActive() {
				return
			}
			continue
		}
		w.mu.Lock()
		more := w.written < len(w.active)
		w.mu.Unlock()
		if !more {
			return
		}
	}
}

func (w *streamWriter) arrayProjectionFits(sourceBytes, additionalBytes int64) bool {
	remaining := w.maxPublishedBytes - 1 - sourceBytes
	return remaining > additionalBytes
}

func (w *streamWriter) writePending() bool {
	if w.workCtx.Err() != nil {
		return false
	}
	if w.sourceFileHandle() == nil {
		if !w.openSourceTemp() {
			return false
		}
	}
	for {
		if w.workCtx.Err() != nil {
			return false
		}
		w.mu.Lock()
		if w.written >= len(w.active) {
			w.mu.Unlock()
			return true
		}
		record := w.active[w.written].data
		w.mu.Unlock()
		sourceFile := w.sourceFileHandle()
		if sourceFile == nil {
			return false
		}
		n, err := (shutdownAwareWriter{ctx: w.workCtx, writer: sourceFile}).Write(record)
		if err == nil && n != len(record) {
			err = io.ErrShortWrite
		}
		if err != nil {
			if w.workCtx.Err() != nil {
				return false
			}
			w.fail(opWrite)
			w.discardBrokenSourceTemp(false)
			return false
		}
		w.mu.Lock()
		w.written++
		w.activeBytes += int64(len(record))
		if w.activeStarted.IsZero() {
			w.activeStarted = w.clock.now()
		}
		w.mu.Unlock()
	}
}

func (w *streamWriter) openSourceTemp() bool {
	if w.workCtx.Err() != nil {
		return false
	}
	id := uuid.NewString()
	sourceTempPath := filepath.Join(w.stagingDirectory, "."+id+".jsonl.tmp")
	sourcePath := filepath.Join(w.stagingDirectory, id+".jsonl")
	arrayTempPath := filepath.Join(w.stagingDirectory, "."+id+".json.tmp")
	finalPath := filepath.Join(w.readyDirectory, id+".json")
	file, err := w.ops.openExclusive(sourceTempPath, 0o660)
	if err != nil {
		if w.workCtx.Err() != nil {
			return false
		}
		w.fail(opOpen)
		return false
	}
	file = w.registerSourceFile(file)
	if file == nil || w.workCtx.Err() != nil {
		return false
	}
	if err := w.ops.chmod(sourceTempPath, 0o660); err != nil {
		closeErr := file.Close()
		w.clearSourceFileHandle()
		if w.workCtx.Err() != nil {
			return false
		}
		if closeErr != nil {
			w.recordFailure(opClose)
		}
		if removeErr := w.ops.remove(sourceTempPath); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			w.sourceTempPath = sourceTempPath
			w.failedPath = sourceTempPath
			w.fail(opRemoveTmp)
		} else {
			w.fail(opOpen)
		}
		return false
	}
	if w.workCtx.Err() != nil {
		return false
	}
	w.sourceTempPath = sourceTempPath
	w.sourcePath = sourcePath
	w.arrayTempPath = arrayTempPath
	w.finalPath = finalPath
	w.sourceClosed = false
	w.sourceSealed = false
	w.arrayClosed = false
	w.publicationCommitted = false
	w.arrayBytes = 0
	return true
}

func (w *streamWriter) publishActive() bool {
	if w.workCtx.Err() != nil {
		return false
	}
	if len(w.active) == 0 {
		return true
	}
	if w.written != len(w.active) {
		return false
	}
	if !w.sourceClosed {
		sourceFile := w.sourceFileHandle()
		if sourceFile == nil {
			return false
		}
		if err := sourceFile.Close(); err != nil {
			w.clearSourceFileHandle()
			if w.workCtx.Err() != nil {
				return false
			}
			w.fail(opClose)
			w.discardBrokenSourceTemp(true)
			return false
		}
		w.clearSourceFileHandle()
		w.sourceClosed = true
		if w.workCtx.Err() != nil {
			return false
		}
	}
	if !w.sourceSealed {
		if w.workCtx.Err() != nil {
			return false
		}
		exists, err := w.pathExists(w.sourcePath)
		if w.workCtx.Err() != nil {
			return false
		}
		if err != nil || exists {
			w.fail(opRename)
			return false
		}
		if w.workCtx.Err() != nil {
			return false
		}
		if err := w.ops.rename(w.sourceTempPath, w.sourcePath); err != nil {
			if w.workCtx.Err() != nil {
				return false
			}
			w.fail(opRename)
			return false
		}
		w.sourceTempPath = ""
		w.sourceSealed = true
		if w.workCtx.Err() != nil {
			return false
		}
	}
	if !w.arrayClosed && !w.convertSealedSource() {
		return false
	}
	if w.workCtx.Err() != nil {
		return false
	}

	w.publicationMu.Lock()
	if w.workCtx.Err() != nil {
		w.publicationMu.Unlock()
		return false
	}
	exists, err := w.pathExists(w.finalPath)
	if w.workCtx.Err() != nil {
		w.publicationMu.Unlock()
		return false
	}
	if err != nil || exists {
		w.publicationMu.Unlock()
		w.fail(opRename)
		return false
	}
	if w.workCtx.Err() != nil {
		w.publicationMu.Unlock()
		return false
	}
	if err := w.ops.rename(w.arrayTempPath, w.finalPath); err != nil {
		w.publicationMu.Unlock()
		if w.workCtx.Err() == nil {
			w.fail(opRename)
		}
		return false
	}
	w.arrayTempPath = ""
	w.arrayClosed = false
	w.publicationCommitted = true
	w.commitPublication()
	w.publicationMu.Unlock()

	if w.workCtx.Err() != nil {
		return false
	}
	if err := w.ops.remove(w.sourcePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		if w.workCtx.Err() != nil {
			return false
		}
		w.failedPath = w.sourcePath
		w.fail(opRemoveSource)
		return false
	}
	w.resetPublishedBatch()
	return true
}

func (w *streamWriter) convertSealedSource() bool {
	if w.workCtx.Err() != nil {
		return false
	}
	reader, err := w.ops.openRead(w.sourcePath)
	if err != nil {
		if w.workCtx.Err() != nil {
			return false
		}
		w.fail(opRead)
		return false
	}
	reader = w.registerSourceReader(reader)
	if reader == nil || w.workCtx.Err() != nil {
		return false
	}
	arrayFile, err := w.ops.openExclusive(w.arrayTempPath, 0o660)
	if err != nil {
		readerCloseErr := reader.Close()
		w.clearSourceReaderHandle()
		if w.workCtx.Err() != nil {
			return false
		}
		if readerCloseErr != nil {
			w.recordFailure(opClose)
		}
		w.fail(opOpen)
		return false
	}
	arrayFile = w.registerArrayFile(arrayFile)
	if arrayFile == nil || w.workCtx.Err() != nil {
		if readerCloseErr := reader.Close(); readerCloseErr != nil {
			w.recordFailure(opClose)
		}
		w.clearSourceReaderHandle()
		return false
	}
	if err := w.ops.chmod(w.arrayTempPath, 0o660); err != nil {
		if w.workCtx.Err() != nil {
			return false
		}
		arrayCloseErr := arrayFile.Close()
		w.clearArrayFileHandle()
		readerCloseErr := reader.Close()
		w.clearSourceReaderHandle()
		if w.workCtx.Err() != nil {
			return false
		}
		if arrayCloseErr != nil {
			w.recordFailure(opClose)
		}
		if readerCloseErr != nil {
			w.recordFailure(opClose)
		}
		w.fail(opOpen)
		w.discardBrokenArrayTemp(true)
		return false
	}
	if w.workCtx.Err() != nil {
		return false
	}

	arrayBytes, copyErr := copyJSONLAsArray(
		shutdownAwareReader{ctx: w.workCtx, reader: reader},
		shutdownAwareWriter{ctx: w.workCtx, writer: arrayFile},
		w.maxPublishedBytes,
	)
	readerCloseErr := reader.Close()
	arrayCloseErr := arrayFile.Close()
	w.clearSourceReaderHandle()
	w.clearArrayFileHandle()
	if w.workCtx.Err() != nil {
		return false
	}

	var operation fileOperation
	if copyErr != nil {
		operation = opWrite
		var readErr jsonArrayReadError
		if errors.As(copyErr, &readErr) || errors.Is(copyErr, io.ErrUnexpectedEOF) {
			operation = opRead
		}
		w.fail(operation)
	} else if readerCloseErr != nil || arrayCloseErr != nil {
		operation = opClose
		w.fail(operation)
	}
	if copyErr != nil || readerCloseErr != nil || arrayCloseErr != nil {
		if readerCloseErr != nil && operation != opClose {
			w.recordFailure(opClose)
		}
		if arrayCloseErr != nil && (operation != opClose || readerCloseErr != nil) {
			w.recordFailure(opClose)
		}
		w.discardBrokenArrayTemp(true)
		return false
	}

	w.arrayClosed = true
	w.arrayBytes = arrayBytes
	return true
}

func (w *streamWriter) pathExists(path string) (bool, error) {
	_, err := w.ops.stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func (w *streamWriter) commitPublication() {
	records := int64(len(w.active))
	sourceBytes := w.activeBytes
	arrayBytes := w.arrayBytes
	w.mu.Lock()
	w.unpublishedRecords -= records
	w.unpublishedBytes -= sourceBytes
	w.readyFilesCreated++
	w.readyRecordsCreated += records
	w.readyBytesCreated += arrayBytes
	w.active = nil
	w.activeBytes = 0
	w.activeStarted = time.Time{}
	w.written = 0
	w.mu.Unlock()
	w.invokeCallback(func() {
		if w.callbacks.published != nil {
			w.callbacks.published(records, arrayBytes)
		}
	})
}

func (w *streamWriter) discardBrokenSourceTemp(alreadyClosed bool) {
	sourceFile := w.sourceFileHandle()
	if sourceFile != nil && !alreadyClosed {
		if err := sourceFile.Close(); err != nil {
			w.recordFailure(opClose)
		}
	}
	w.clearSourceFileHandle()
	if w.sourceTempPath != "" {
		if err := w.ops.remove(w.sourceTempPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			w.failedPath = w.sourceTempPath
			w.fail(opRemoveTmp)
			return
		}
	}
	w.resetSourceForRebuild()
}

func (w *streamWriter) discardBrokenArrayTemp(alreadyClosed bool) {
	arrayFile := w.arrayFileHandle()
	if arrayFile != nil && !alreadyClosed {
		if err := arrayFile.Close(); err != nil {
			w.recordFailure(opClose)
		}
	}
	w.clearArrayFileHandle()
	if w.arrayTempPath != "" {
		if err := w.ops.remove(w.arrayTempPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			w.failedPath = w.arrayTempPath
			w.fail(opRemoveTmp)
			return
		}
	}
	w.arrayClosed = false
	w.arrayBytes = 0
}

func (w *streamWriter) resetSourceForRebuild() {
	w.clearPrivateHandles()
	w.sourceTempPath = ""
	w.sourcePath = ""
	w.arrayTempPath = ""
	w.finalPath = ""
	w.sourceClosed = false
	w.sourceSealed = false
	w.arrayClosed = false
	w.publicationCommitted = false
	w.arrayBytes = 0
	w.mu.Lock()
	w.written = 0
	w.activeBytes = 0
	w.mu.Unlock()
}
func (w *streamWriter) resetPublishedBatch() {
	w.clearPrivateHandles()
	w.sourceTempPath = ""
	w.sourcePath = ""
	w.arrayTempPath = ""
	w.finalPath = ""
	w.sourceClosed = false
	w.sourceSealed = false
	w.arrayClosed = false
	w.publicationCommitted = false
	w.arrayBytes = 0
	w.failedPath = ""
}
func (w *streamWriter) preflight() bool {
	if err := w.ops.mkdirAll(w.stagingDirectory, 0o770|fs.ModeSetgid); err != nil {
		w.fail(opMkdir)
		w.preflightOnRecovery = true
		return false
	}
	if err := w.ops.mkdirAll(w.readyDirectory, 0o770|fs.ModeSetgid); err != nil {
		w.fail(opMkdir)
		w.preflightOnRecovery = true
		return false
	}
	entries, err := w.ops.readDir(w.stagingDirectory)
	if err != nil {
		w.fail(opMkdir)
		w.preflightOnRecovery = true
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() || (!canonicalSourceTmpName.MatchString(entry.Name()) &&
			!canonicalArrayTmpName.MatchString(entry.Name())) {
			continue
		}
		path := filepath.Join(w.stagingDirectory, entry.Name())
		if err := w.ops.remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			w.failedPath = path
			w.preflightOnRecovery = true
			w.fail(opRemoveTmp)
			return false
		}
	}
	w.preflightOnRecovery = false
	w.recover()
	return true
}

func (w *streamWriter) probeRecovery() bool {
	switch w.failedOp {
	case opMkdir:
		if !w.preflight() {
			w.increaseBackoff()
			return false
		}
		return true
	case opRemoveTmp:
		path := w.failedPath
		if err := w.ops.remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			w.recordFailure(opRemoveTmp)
			w.increaseBackoff()
			return false
		}
		w.failedPath = ""
		if w.preflightOnRecovery {
			if !w.preflight() {
				w.increaseBackoff()
				return false
			}
			return true
		}
		if path == w.sourceTempPath {
			w.resetSourceForRebuild()
		} else if path == w.arrayTempPath {
			w.arrayClosed = false
			w.arrayBytes = 0
		}
	case opRemoveSource:
		if err := w.ops.remove(w.sourcePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			w.recordFailure(opRemoveSource)
			w.increaseBackoff()
			return false
		}
		w.resetPublishedBatch()
		w.recover()
		w.processHealthy(false)
		return w.isHealthy()
	}

	if w.sourceClosed || w.sourceSealed {
		if !w.publishActive() {
			w.increaseBackoff()
			return false
		}
	} else if !w.writePending() {
		w.increaseBackoff()
		return false
	}
	w.recover()
	w.processHealthy(false)
	return w.isHealthy()
}

func (w *streamWriter) isHealthy() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.state == streamHealthy
}
func (w *streamWriter) invokeCallback(callback func()) {
	if callback == nil {
		return
	}
	w.callbackMu.Lock()
	defer w.callbackMu.Unlock()
	w.mu.Lock()
	shutdownReported := w.shutdownReported
	w.mu.Unlock()
	if !shutdownReported {
		callback()
	}
}

func (w *streamWriter) fail(operation fileOperation) {
	w.failedOp = operation
	w.recordFailure(operation)
	w.mu.Lock()
	from := w.state
	if from != streamDegraded {
		w.state = streamDegraded
	}
	w.mu.Unlock()
	if from != streamDegraded {
		select {
		case <-w.notify:
		default:
		}
	}
	if from != streamDegraded {
		w.invokeCallback(func() {
			if w.callbacks.stateChanged != nil {
				w.callbacks.stateChanged(from, streamDegraded, operation)
			}
		})
	}
}

func (w *streamWriter) recordFailure(operation fileOperation) {
	w.mu.Lock()
	w.operationFailures[operation]++
	w.mu.Unlock()
	w.invokeCallback(func() {
		if w.callbacks.operationFailed != nil {
			w.callbacks.operationFailed(operation)
		}
	})
}

func (w *streamWriter) recover() {
	w.mu.Lock()
	from := w.state
	w.state = streamHealthy
	w.backoff = recoveryInitialBackoff
	w.mu.Unlock()
	w.failedOp = ""
	if from != streamHealthy {
		w.invokeCallback(func() {
			if w.callbacks.stateChanged != nil {
				w.callbacks.stateChanged(from, streamHealthy, "")
			}
		})
	}
}

func (w *streamWriter) increaseBackoff() {
	w.mu.Lock()
	if w.backoff < recoveryMaxBackoff {
		w.backoff *= 2
		if w.backoff > recoveryMaxBackoff {
			w.backoff = recoveryMaxBackoff
		}
	}
	w.mu.Unlock()
}
func (w *streamWriter) flushForShutdown(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	flushCtx := ctx
	cancel := func() {}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		flushCtx, cancel = context.WithTimeout(ctx, maxShutdownFlush)
	}
	defer cancel()

	for {
		w.mu.Lock()
		done := w.unpublishedRecords == 0 && !w.publicationCommitted
		state := w.state
		w.mu.Unlock()
		if done {
			return
		}
		select {
		case <-flushCtx.Done():
			w.stopAtShutdownDeadline()
			return
		default:
		}
		if state == streamHealthy {
			w.processHealthy(true)
			continue
		}
		if w.probeRecovery() {
			continue
		}
		w.mu.Lock()
		delay := w.backoff
		w.mu.Unlock()
		timer := w.clock.newTimer(delay)
		select {
		case <-timer.C():
		case <-flushCtx.Done():
			timer.Stop()
			w.stopAtShutdownDeadline()
			return
		}
	}
}

func (w *streamWriter) stopAtShutdownDeadline() {
	w.workCancel()
	w.closePrivateHandles()
	w.publicationMu.Lock()
	w.reportShutdownDeadline()
	w.publicationMu.Unlock()
}

func (w *streamWriter) reportShutdownDeadline() {
	w.callbackMu.Lock()
	defer w.callbackMu.Unlock()
	w.mu.Lock()
	records, bytes := w.unpublishedRecords, w.unpublishedBytes
	alreadyReported := w.shutdownReported
	w.shutdownReported = true
	w.mu.Unlock()
	if !alreadyReported && records != 0 && w.callbacks.shutdownDeadline != nil {
		w.callbacks.shutdownDeadline(records, bytes)
	}
}

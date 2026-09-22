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
)

type streamState int64

const (
	streamUnavailable streamState = 0
	streamHealthy     streamState = 1
	streamDegraded    streamState = 2
)

type fileOperation string

const (
	opMkdir     fileOperation = "mkdir"
	opOpen      fileOperation = "open"
	opWrite     fileOperation = "write"
	opClose     fileOperation = "close"
	opRename    fileOperation = "rename"
	opRemoveTmp fileOperation = "remove_tmp"
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
	readyBacklogFiles   int64
	readyBacklogBytes   int64

	operationFailures map[fileOperation]int64
}

type streamCallbacks struct {
	stateChanged     func(candidateType, streamState, streamState, fileOperation)
	operationFailed  func(candidateType, fileOperation)
	published        func(candidateType, int64, int64)
	shutdownDeadline func(candidateType, int64, int64)
}

type fileOps interface {
	mkdirAll(string, fs.FileMode) error
	openExclusive(string, fs.FileMode) (batchFile, error)
	rename(string, string) error
	remove(string) error
	readDir(string) ([]fs.DirEntry, error)
}

type batchFile interface {
	Write([]byte) (int, error)
	Close() error
}

type osFileOps struct{}

func (osFileOps) mkdirAll(path string, mode fs.FileMode) error { return os.MkdirAll(path, mode) }
func (osFileOps) openExclusive(path string, mode fs.FileMode) (batchFile, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
}
func (osFileOps) rename(oldPath, newPath string) error { return os.Rename(oldPath, newPath) }
func (osFileOps) remove(path string) error             { return os.Remove(path) }
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
	canonicalTmpName   = regexp.MustCompile(`^\.[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.tmp$`)
	canonicalReadyName = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.jsonl$`)
)

type shutdownRequest struct {
	ctx  context.Context
	done chan struct{}
}

type streamWriter struct {
	candidate       candidateType
	directory       string
	maxBacklogBytes int64
	maxFileBytes    int64
	maxBatchAge     time.Duration

	ops       fileOps
	clock     writerClock
	callbacks streamCallbacks

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
	readyBacklogFiles   int64
	readyBacklogBytes   int64
	activeBytes         int64
	activeStarted       time.Time
	accepting           bool
	started             bool
	shutdownReported    bool

	notify           chan struct{}
	shutdownRequests chan shutdownRequest

	file                batchFile
	tempPath            string
	readyPath           string
	closed              bool
	written             int
	failedOp            fileOperation
	failedPath          string
	preflightOnRecovery bool
	backoff             time.Duration
}

func newStreamWriter(candidate candidateType, directory string, maxBacklogBytes int64) *streamWriter {
	return &streamWriter{
		candidate: candidate, directory: directory,
		maxBacklogBytes: maxBacklogBytes,
		maxFileBytes:    maxFileBytes,
		maxBatchAge:     maxBatchAge,
		ops:             osFileOps{}, clock: realWriterClock{},
		state:             streamUnavailable,
		operationFailures: make(map[fileOperation]int64),
		notify:            make(chan struct{}, 1), shutdownRequests: make(chan shutdownRequest, 1),
		accepting: true,
		backoff:   recoveryInitialBackoff,
	}
}

func (w *streamWriter) tryEnqueue(record []byte) bool {
	if len(record) == 0 || record[len(record)-1] != '\n' {
		return false
	}

	w.mu.Lock()
	if !w.accepting || (w.unpublishedBytes != 0 && (int64(len(record)) > w.maxBacklogBytes-w.unpublishedBytes)) {
		w.mu.Unlock()
		return false
	}
	copyOfRecord := append([]byte(nil), record...)
	w.queued = append(w.queued, queuedRecord{data: copyOfRecord})
	w.unpublishedRecords++
	w.unpublishedBytes += int64(len(copyOfRecord))
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
	w.mu.Lock()
	w.accepting = false
	started := w.started
	records, bytes := w.unpublishedRecords, w.unpublishedBytes
	w.mu.Unlock()
	if !started {
		if records != 0 && w.callbacks.shutdownDeadline != nil {
			w.callbacks.shutdownDeadline(w.candidate, records, bytes)
		}
		return
	}

	req := shutdownRequest{ctx: ctx, done: make(chan struct{})}
	// The channel is buffered and lifecycle shutdown is single-shot. Always
	// hand the request to the worker, even when the deadline has already
	// expired, so it exits without leaking a goroutine or an open handle.
	w.shutdownRequests <- req
	select {
	case <-req.done:
	case <-ctx.Done():
		w.reportShutdownDeadline()
	}
}

func (w *streamWriter) shutdown(ctx context.Context) { w.shutdownWriter(ctx) }

func (w *streamWriter) snapshot() streamSnapshot {
	w.refreshReadyBacklog()
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
		unpublishedRecords:  w.unpublishedRecords, unpublishedBytes: w.unpublishedBytes,
		openBatchRecords: int64(len(w.active)), openBatchBytes: w.activeBytes, openBatchAge: age,
		readyFilesCreated: w.readyFilesCreated, readyRecordsCreated: w.readyRecordsCreated,
		readyBytesCreated: w.readyBytesCreated, readyBacklogFiles: w.readyBacklogFiles,
		readyBacklogBytes: w.readyBacklogBytes, operationFailures: failures,
	}
}

func (w *streamWriter) run() {
	timer := w.clock.newTimer(time.Hour)
	timer.Stop()
	var timerC <-chan time.Time
	for {
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
	w.refreshReadyBacklog()
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

		w.mu.Lock()
		publish := w.activeBytes >= w.maxFileBytes || (force && len(w.queued) == 0)
		if !publish && !w.activeStarted.IsZero() && w.clock.now().Sub(w.activeStarted) >= w.maxBatchAge {
			publish = true
		}
		if !publish && len(w.queued) != 0 {
			w.active = append(w.active, w.queued[0])
			w.queued[0] = queuedRecord{}
			w.queued = w.queued[1:]
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

func (w *streamWriter) writePending() bool {
	if w.file == nil {
		if !w.openTemp() {
			return false
		}
	}
	for {
		w.mu.Lock()
		if w.written >= len(w.active) {
			w.mu.Unlock()
			return true
		}
		record := w.active[w.written].data
		w.mu.Unlock()
		n, err := w.file.Write(record)
		if err == nil && n != len(record) {
			err = io.ErrShortWrite
		}
		if err != nil {
			w.fail(opWrite)
			w.discardBrokenTemp(false)
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

func (w *streamWriter) openTemp() bool {
	id := uuid.NewString()
	tempPath := filepath.Join(w.directory, "."+id+".tmp")
	file, err := w.ops.openExclusive(tempPath, 0o600)
	if err != nil {
		w.fail(opOpen)
		return false
	}
	w.file = file
	w.tempPath = tempPath
	w.readyPath = filepath.Join(w.directory, id+".jsonl")
	w.closed = false
	return true
}

func (w *streamWriter) publishActive() bool {
	if w.file == nil || w.written == 0 {
		return true
	}
	if !w.closed {
		if err := w.file.Close(); err != nil {
			w.fail(opClose)
			w.file = nil
			w.discardBrokenTemp(true)
			return false
		}
		w.file = nil
		w.closed = true
	}
	if w.readyPathVisible() {
		w.fail(opRename)
		return false
	}
	if err := w.ops.rename(w.tempPath, w.readyPath); err != nil {
		w.fail(opRename)
		return false
	}

	w.mu.Lock()
	records := int64(len(w.active))
	bytes := w.activeBytes
	w.unpublishedRecords -= records
	w.unpublishedBytes -= bytes
	w.readyFilesCreated++
	w.readyRecordsCreated += records
	w.readyBytesCreated += bytes
	w.active = nil
	w.activeBytes = 0
	w.activeStarted = time.Time{}
	w.written = 0
	w.mu.Unlock()
	w.file, w.tempPath, w.readyPath, w.closed = nil, "", "", false
	w.refreshReadyBacklog()
	if w.callbacks.published != nil {
		w.callbacks.published(w.candidate, records, bytes)
	}
	return true
}

func (w *streamWriter) discardBrokenTemp(alreadyClosed bool) {
	if w.file != nil && !alreadyClosed {
		if err := w.file.Close(); err != nil {
			w.recordFailure(opClose)
		}
	}
	w.file = nil
	if w.tempPath != "" {
		if err := w.ops.remove(w.tempPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			w.failedPath = w.tempPath
			w.fail(opRemoveTmp)
			return
		}
	}
	w.resetTempForRebuild()
}

func (w *streamWriter) resetTempForRebuild() {
	w.file, w.tempPath, w.readyPath, w.closed = nil, "", "", false
	w.mu.Lock()
	w.written = 0
	w.activeBytes = 0
	w.activeStarted = time.Time{}
	w.mu.Unlock()
}

func (w *streamWriter) preflight() bool {
	if err := w.ops.mkdirAll(w.directory, 0o750); err != nil {
		w.fail(opMkdir)
		w.preflightOnRecovery = true
		return false
	}
	entries, err := w.ops.readDir(w.directory)
	if err != nil {
		w.fail(opMkdir)
		w.preflightOnRecovery = true
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() || !canonicalTmpName.MatchString(entry.Name()) {
			continue
		}
		path := filepath.Join(w.directory, entry.Name())
		if err := w.ops.remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			w.failedPath = path
			w.preflightOnRecovery = true
			w.fail(opRemoveTmp)
			return false
		}
	}
	w.preflightOnRecovery = false
	w.recover()
	w.refreshReadyBacklog()
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
		if err := w.ops.remove(w.failedPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
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
		w.resetTempForRebuild()
	case opRename:
		if w.readyPathVisible() {
			w.recordFailure(opRename)
			w.increaseBackoff()
			return false
		}
		if err := w.ops.rename(w.tempPath, w.readyPath); err != nil {
			w.recordFailure(opRename)
			w.increaseBackoff()
			return false
		}
		w.mu.Lock()
		records, bytes := int64(len(w.active)), w.activeBytes
		w.unpublishedRecords -= records
		w.unpublishedBytes -= bytes
		w.readyFilesCreated++
		w.readyRecordsCreated += records
		w.readyBytesCreated += bytes
		w.active = nil
		w.activeBytes = 0
		w.activeStarted = time.Time{}
		w.written = 0
		w.mu.Unlock()
		w.tempPath, w.readyPath, w.closed = "", "", false
		w.recover()
		w.refreshReadyBacklog()
		if w.callbacks.published != nil {
			w.callbacks.published(w.candidate, records, bytes)
		}
		return true
	}

	// Rebuild the retained active records while still degraded. Recovery is
	// observable only after the failed operation has genuinely succeeded.
	if !w.writePending() {
		w.increaseBackoff()
		return false
	}
	w.recover()
	w.processHealthy(false)
	w.mu.Lock()
	healthy := w.state == streamHealthy
	w.mu.Unlock()
	return healthy
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
	if from != streamDegraded && w.callbacks.stateChanged != nil {
		w.callbacks.stateChanged(w.candidate, from, streamDegraded, operation)
	}
}

func (w *streamWriter) recordFailure(operation fileOperation) {
	w.mu.Lock()
	w.operationFailures[operation]++
	w.mu.Unlock()
	if w.callbacks.operationFailed != nil {
		w.callbacks.operationFailed(w.candidate, operation)
	}
}

func (w *streamWriter) recover() {
	w.mu.Lock()
	from := w.state
	w.state = streamHealthy
	w.backoff = recoveryInitialBackoff
	w.mu.Unlock()
	w.failedOp = ""
	if from != streamHealthy && w.callbacks.stateChanged != nil {
		w.callbacks.stateChanged(w.candidate, from, streamHealthy, "")
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
	for {
		w.mu.Lock()
		done := w.unpublishedRecords == 0
		state := w.state
		w.mu.Unlock()
		if done {
			return
		}
		select {
		case <-ctx.Done():
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
		case <-ctx.Done():
			timer.Stop()
			w.stopAtShutdownDeadline()
			return
		}
	}
}

func (w *streamWriter) stopAtShutdownDeadline() {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			w.recordFailure(opClose)
		}
		w.file = nil
	}
	w.reportShutdownDeadline()
}

func (w *streamWriter) reportShutdownDeadline() {
	w.mu.Lock()
	records, bytes := w.unpublishedRecords, w.unpublishedBytes
	if w.shutdownReported || records == 0 {
		w.mu.Unlock()
		return
	}
	w.shutdownReported = true
	w.mu.Unlock()
	if w.callbacks.shutdownDeadline != nil {
		w.callbacks.shutdownDeadline(w.candidate, records, bytes)
	}
}

func (w *streamWriter) readyPathVisible() bool {
	entries, err := w.ops.readDir(w.directory)
	if err != nil {
		return false
	}
	name := filepath.Base(w.readyPath)
	for _, entry := range entries {
		if entry.Name() == name {
			return true
		}
	}
	return false
}

func (w *streamWriter) refreshReadyBacklog() {
	entries, err := w.ops.readDir(w.directory)
	if err != nil {
		return
	}
	var files, bytes int64
	for _, entry := range entries {
		if entry.IsDir() || !canonicalReadyName.MatchString(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files++
		bytes += info.Size()
	}
	w.mu.Lock()
	w.readyBacklogFiles, w.readyBacklogBytes = files, bytes
	w.mu.Unlock()
}

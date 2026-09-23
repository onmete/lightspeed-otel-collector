package agenticexporter

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestStreamWriterPublishCleanupAndImmutableInput(t *testing.T) {
	dir := t.TempDir()
	stale := ".00000000-0000-4000-8000-000000000000.tmp"
	untouched := []string{
		".00000000-0000-4000-8000-000000000000.jsonl",
		".00000000-0000-4000-7000-000000000000.tmp",
		".00000000-0000-4000-8000-000000000000.TMP",
		".AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA.tmp",
		".not-a-uuid.tmp",
		"notes",
	}
	mustWriteFile(t, filepath.Join(dir, stale), []byte("stale"))
	for _, name := range untouched {
		mustWriteFile(t, filepath.Join(dir, name), []byte("keep"))
	}

	published := make(chan struct{}, 1)
	w := newStreamWriter(candidateAction, dir, 64)
	w.maxFileBytes = 8
	w.callbacks.published = func(candidateType, int64, int64) { published <- struct{}{} }
	if err := w.start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, stale)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("canonical stale temp remains: %v", err)
	}
	for _, name := range untouched {
		if got, err := os.ReadFile(filepath.Join(dir, name)); err != nil || string(got) != "keep" {
			t.Fatalf("startup changed %q: %q, %v", name, got, err)
		}
	}

	record := []byte("{\"a\":1}\n")
	want := append([]byte(nil), record...)
	if !w.tryEnqueue(record) {
		t.Fatal("record rejected")
	}
	for i := range record {
		record[i] = 'x'
	}
	waitSignal(t, published)

	ready := readyFiles(t, dir)
	if len(ready) != 1 {
		t.Fatalf("ready files = %v", ready)
	}
	got, err := os.ReadFile(filepath.Join(dir, ready[0]))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("ready content = %q, want %q", got, want)
	}
	if !canonicalReadyName.MatchString(ready[0]) {
		t.Fatalf("noncanonical ready name %q", ready[0])
	}
	mustWriteFile(t, filepath.Join(dir, "other.jsonl"), []byte("unrelated"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w.shutdown(ctx)
	if got, _ := os.ReadFile(filepath.Join(dir, ready[0])); string(got) != string(want) {
		t.Fatalf("ready file changed after publication: %q", got)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "other.jsonl")); string(got) != "unrelated" {
		t.Fatalf("unrelated ready file changed: %q", got)
	}
}
func TestStreamWriterSharedSpoolPermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "stream")
	published := make(chan struct{}, 1)
	w := newStreamWriter(candidateAction, dir, 64)
	w.maxFileBytes = 1
	w.callbacks.published = func(candidateType, int64, int64) { published <- struct{}{} }
	if err := w.start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	directoryInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("Stat(directory): %v", err)
	}
	if got := directoryInfo.Mode().Perm(); got != 0o770 {
		t.Fatalf("directory permissions = %o, want 770", got)
	}
	if directoryInfo.Mode()&fs.ModeSetgid == 0 {
		t.Fatal("spool directory is not setgid")
	}

	if !w.tryEnqueue([]byte("record\n")) {
		t.Fatal("record rejected")
	}
	waitSignal(t, published)
	ready := readyFiles(t, dir)
	if len(ready) != 1 {
		t.Fatalf("ready files = %v", ready)
	}
	fileInfo, err := os.Stat(filepath.Join(dir, ready[0]))
	if err != nil {
		t.Fatalf("Stat(ready file): %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o660 {
		t.Fatalf("ready file permissions = %o, want 660", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w.shutdown(ctx)
}

func TestOSFileOpsMkdirAllPreservesExistingDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "stream")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	if err := (osFileOps{}).mkdirAll(dir, 0o770|fs.ModeSetgid); err != nil {
		t.Fatalf("mkdirAll: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("existing directory permissions = %o, want 700", got)
	}
	if info.Mode()&fs.ModeSetgid != 0 {
		t.Fatal("existing directory unexpectedly changed to setgid")
	}
}

func TestStreamWriterAdmissionAndOversizedException(t *testing.T) {
	w := newStreamWriter(candidateAction, t.TempDir(), 8)
	if !w.tryEnqueue([]byte("1234567\n")) {
		t.Fatal("exact budget fit rejected")
	}
	if w.tryEnqueue([]byte("\n")) {
		t.Fatal("one-byte overflow accepted")
	}
	if w.tryEnqueue(nil) || w.tryEnqueue([]byte("not terminated")) {
		t.Fatal("invalid record accepted")
	}

	oversized := newStreamWriter(candidateTranscript, t.TempDir(), 4)
	if !oversized.tryEnqueue([]byte("oversized\n")) {
		t.Fatal("single oversized record rejected on empty stream")
	}
	if oversized.tryEnqueue([]byte("\n")) {
		t.Fatal("second record accepted behind oversized record")
	}
	if got := oversized.snapshot(); got.unpublishedRecords != 1 || got.unpublishedBytes != 10 || got.queueHighWaterBytes != 10 {
		t.Fatalf("oversized snapshot = %+v", got)
	}
}

func TestStreamWriterTimerShutdownAndEmptySuppression(t *testing.T) {
	dir := t.TempDir()
	clock := newManualClock()
	published := make(chan struct{}, 2)
	w := newStreamWriter(candidateTranscript, dir, 64)
	w.clock = clock
	w.maxFileBytes = 64
	w.maxBatchAge = 30 * time.Second
	w.callbacks.published = func(candidateType, int64, int64) { published <- struct{}{} }
	_ = w.start(context.Background())
	if !w.tryEnqueue([]byte("one\n")) {
		t.Fatal("enqueue rejected")
	}
	waitUntil(t, func() bool { return w.snapshot().openBatchRecords == 1 && clock.hasActiveTimer() })
	advanceClockUntilSignal(t, clock, published, time.Second)

	if !w.tryEnqueue([]byte("two\n")) {
		t.Fatal("enqueue rejected")
	}
	waitUntil(t, func() bool { return w.snapshot().openBatchRecords == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w.shutdown(ctx)
	waitSignal(t, published)
	if files := readyFiles(t, dir); len(files) != 2 {
		t.Fatalf("ready count = %d, want 2", len(files))
	}

	emptyDir := t.TempDir()
	empty := newStreamWriter(candidateAction, emptyDir, 64)
	_ = empty.start(context.Background())
	empty.shutdown(context.Background())
	if files := readyFiles(t, emptyDir); len(files) != 0 {
		t.Fatalf("empty writer created files: %v", files)
	}
}

func TestStreamWriterFailureRecoveryMatrix(t *testing.T) {
	for _, operation := range []fileOperation{opMkdir, opOpen, opWrite, opClose, opRename} {
		t.Run(string(operation), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "stream")
			clock := newManualClock()
			ops := &faultFileOps{base: osFileOps{}, failures: map[fileOperation]int{operation: 1}}
			transitions := make(chan stateEdge, 4)
			published := make(chan struct{}, 1)
			w := newStreamWriter(candidateAction, dir, 64)
			w.ops, w.clock, w.maxFileBytes = ops, clock, 4
			w.callbacks.stateChanged = func(_ candidateType, from, to streamState, op fileOperation) {
				transitions <- stateEdge{from: from, to: to, operation: op}
			}
			w.callbacks.published = func(candidateType, int64, int64) { published <- struct{}{} }
			_ = w.start(context.Background())
			if operation != opMkdir && !w.tryEnqueue([]byte("abc\n")) {
				t.Fatal("enqueue rejected")
			}
			if operation == opMkdir {
				if !w.tryEnqueue([]byte("abc\n")) {
					t.Fatal("enqueue rejected while unavailable")
				}
			}
			waitUntil(t, func() bool { return w.snapshot().state == streamDegraded && clock.hasActiveTimer() })
			snap := w.snapshot()
			if snap.operationFailures[operation] != 1 || snap.unpublishedBytes != 4 {
				t.Fatalf("degraded snapshot = %+v", snap)
			}
			clock.advance(recoveryInitialBackoff)
			waitSignal(t, published)
			waitUntil(t, func() bool { return w.snapshot().state == streamHealthy })
			if got := w.snapshot(); got.unpublishedBytes != 0 || got.readyFilesCreated != 1 {
				t.Fatalf("recovered snapshot = %+v", got)
			}
			close(transitions)
			var degraded, recovered int
			for edge := range transitions {
				if edge.to == streamDegraded {
					degraded++
					if edge.operation != operation {
						t.Fatalf("degraded operation = %q", edge.operation)
					}
				} else if edge.from == streamDegraded && edge.to == streamHealthy {
					recovered++
				}
			}
			if degraded != 1 || recovered != 1 {
				t.Fatalf("state edges degraded=%d recovered=%d", degraded, recovered)
			}
			w.shutdown(context.Background())
		})
	}
}

func TestStreamWriterChmodFailureRemovesOwnedTemp(t *testing.T) {
	dir := t.TempDir()
	ops := &faultFileOps{base: osFileOps{}, chmodFailures: 1, failures: map[fileOperation]int{}}
	w := newStreamWriter(candidateAction, dir, 64)
	w.ops = ops
	_ = w.start(context.Background())
	if !w.tryEnqueue([]byte("abc\n")) {
		t.Fatal("enqueue rejected")
	}
	waitUntil(t, func() bool {
		snapshot := w.snapshot()
		return snapshot.state == streamDegraded && snapshot.operationFailures[opOpen] == 1
	})
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if canonicalTmpName.MatchString(entry.Name()) {
			t.Fatalf("owned temp file remained after chmod failure: %s", entry.Name())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.shutdown(ctx)
}

func TestStreamWriterRemoveFailureBlocksNewTemp(t *testing.T) {
	dir := t.TempDir()
	clock := newManualClock()
	ops := &faultFileOps{base: osFileOps{}, failures: map[fileOperation]int{opWrite: 1, opRemoveTmp: 1}}
	w := newStreamWriter(candidateAction, dir, 64)
	w.ops, w.clock, w.maxFileBytes = ops, clock, 4
	_ = w.start(context.Background())
	if !w.tryEnqueue([]byte("abc\n")) {
		t.Fatal("enqueue rejected")
	}
	waitUntil(t, func() bool { return w.snapshot().operationFailures[opRemoveTmp] == 1 && clock.hasActiveTimer() })
	if got := ops.openCount(); got != 1 {
		t.Fatalf("open count after failed removal = %d", got)
	}
	clock.advance(recoveryInitialBackoff)
	waitUntil(t, func() bool { return w.snapshot().state == streamHealthy })
	if got := ops.openCount(); got != 2 {
		t.Fatalf("open count after recovery = %d, want 2", got)
	}
	w.shutdown(context.Background())
}

func TestStreamWriterRenameRetriesSameClosedTemp(t *testing.T) {
	dir := t.TempDir()
	clock := newManualClock()
	ops := &faultFileOps{base: osFileOps{}, failures: map[fileOperation]int{opRename: 1}}
	w := newStreamWriter(candidateAction, dir, 64)
	w.ops, w.clock, w.maxFileBytes = ops, clock, 4
	_ = w.start(context.Background())
	_ = w.tryEnqueue([]byte("abc\n"))
	waitUntil(t, func() bool { return w.snapshot().state == streamDegraded && clock.hasActiveTimer() })
	first := ops.renameSources()
	if len(first) != 1 {
		t.Fatalf("rename attempts = %v", first)
	}
	clock.advance(recoveryInitialBackoff)
	waitUntil(t, func() bool { return w.snapshot().unpublishedBytes == 0 })
	attempts := ops.renameSources()
	if len(attempts) != 2 || attempts[0] != attempts[1] {
		t.Fatalf("rename did not retry same temp: %v", attempts)
	}
	if got := ops.closeCount(); got != 1 {
		t.Fatalf("file closed %d times", got)
	}
	w.shutdown(context.Background())
}

func TestStreamWriterShutdownDeadlineAndIndependentProgress(t *testing.T) {
	blockedOps := &faultFileOps{base: osFileOps{}, failures: map[fileOperation]int{opRename: 100}}
	blocked := newStreamWriter(candidateAction, t.TempDir(), 64)
	blocked.ops, blocked.maxFileBytes, blocked.maxBatchAge = blockedOps, 4, time.Hour
	deadline := make(chan [2]int64, 1)
	blocked.callbacks.shutdownDeadline = func(_ candidateType, records, bytes int64) { deadline <- [2]int64{records, bytes} }
	_ = blocked.start(context.Background())
	_ = blocked.tryEnqueue([]byte("abc\n"))
	waitUntil(t, func() bool { return blocked.snapshot().state == streamDegraded })

	otherPublished := make(chan struct{}, 1)
	other := newStreamWriter(candidateTranscript, t.TempDir(), 64)
	other.maxFileBytes = 4
	other.callbacks.published = func(candidateType, int64, int64) { otherPublished <- struct{}{} }
	_ = other.start(context.Background())
	_ = other.tryEnqueue([]byte("xyz\n"))
	waitSignal(t, otherPublished)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	blocked.shutdown(ctx)
	select {
	case got := <-deadline:
		if got != [2]int64{1, 4} {
			t.Fatalf("deadline counts = %v", got)
		}
	default:
		t.Fatal("shutdown deadline was not reported")
	}
	if got := blocked.snapshot(); got.unpublishedBytes != 4 {
		t.Fatalf("blocked reservation released: %+v", got)
	}
	other.shutdown(context.Background())
}

func TestStreamWriterRecoveryBackoffCapsAndSuppressesRepeatedEdges(t *testing.T) {
	var degradedEdges int
	w := newStreamWriter(candidateAction, t.TempDir(), 64)
	w.callbacks.stateChanged = func(_ candidateType, _ streamState, to streamState, _ fileOperation) {
		if to == streamDegraded {
			degradedEdges++
		}
	}
	w.fail(opOpen)
	for range 16 {
		w.fail(opOpen)
		w.increaseBackoff()
	}
	w.mu.Lock()
	backoff := w.backoff
	w.mu.Unlock()
	if backoff != recoveryMaxBackoff {
		t.Fatalf("recovery backoff = %v, want cap %v", backoff, recoveryMaxBackoff)
	}
	if degradedEdges != 1 {
		t.Fatalf("degraded state edges = %d, want 1", degradedEdges)
	}
}
func TestStreamWriterRecoveryPreservesBatchAge(t *testing.T) {
	dir := t.TempDir()
	clock := newManualClock()
	ops := &faultFileOps{base: osFileOps{}, failures: map[fileOperation]int{}}
	published := make(chan struct{}, 1)
	w := newStreamWriter(candidateAction, dir, 64)
	w.ops, w.clock, w.maxFileBytes, w.maxBatchAge = ops, clock, 64, 30*time.Second
	w.callbacks.published = func(candidateType, int64, int64) { published <- struct{}{} }
	if err := w.start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !w.tryEnqueue([]byte("one\n")) {
		t.Fatal("first record rejected")
	}
	waitUntil(t, func() bool { return w.snapshot().openBatchBytes == 4 })
	clock.advance(20 * time.Second)
	ops.mu.Lock()
	ops.failures[opWrite] = 1
	ops.mu.Unlock()
	beforeResets := clock.resetCount()
	if !w.tryEnqueue([]byte("two\n")) {
		t.Fatal("second record rejected")
	}
	waitUntil(t, func() bool {
		return w.snapshot().state == streamDegraded && clock.resetCount() > beforeResets
	})
	if got := w.snapshot().openBatchAge; got < 20*time.Second {
		t.Fatalf("batch age after failure = %v, want at least 20s", got)
	}

	deadline := time.Now().Add(time.Second)
	for {
		snapshot := w.snapshot()
		if snapshot.state == streamHealthy && snapshot.openBatchBytes == 8 {
			break
		}
		clock.advance(recoveryInitialBackoff)
		runtime.Gosched()
		if time.Now().After(deadline) {
			t.Fatalf("recovery did not become healthy: %+v", snapshot)
		}
	}
	advanceClockUntilSignal(t, clock, published, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w.shutdown(ctx)
}

func TestStreamWriterReadyBacklogNoticesExternalDeletion(t *testing.T) {
	dir := t.TempDir()
	published := make(chan struct{}, 1)
	w := newStreamWriter(candidateAction, dir, 64)
	w.maxFileBytes = 4
	w.callbacks.published = func(candidateType, int64, int64) { published <- struct{}{} }
	_ = w.start(context.Background())
	_ = w.tryEnqueue([]byte("abc\n"))
	waitSignal(t, published)
	ready := readyFiles(t, dir)
	if got := w.snapshot(); got.readyBacklogFiles != 1 || got.readyBacklogBytes != 4 {
		t.Fatalf("backlog = %+v", got)
	}
	if err := os.Remove(filepath.Join(dir, ready[0])); err != nil {
		t.Fatal(err)
	}
	if got := w.snapshot(); got.readyBacklogFiles != 0 || got.readyBacklogBytes != 0 {
		t.Fatalf("backlog after external deletion = %+v", got)
	}
	w.shutdown(context.Background())
}

type stateEdge struct {
	from, to  streamState
	operation fileOperation
}

type faultFileOps struct {
	base          fileOps
	mu            sync.Mutex
	failures      map[fileOperation]int
	chmodFailures int
	opens, closes int
	renames       []string
}

func (o *faultFileOps) take(operation fileOperation) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.failures[operation] == 0 {
		return false
	}
	o.failures[operation]--
	return true
}
func (o *faultFileOps) mkdirAll(path string, mode fs.FileMode) error {
	if o.take(opMkdir) {
		return errors.New("injected mkdir")
	}
	return o.base.mkdirAll(path, mode)
}
func (o *faultFileOps) openExclusive(path string, mode fs.FileMode) (batchFile, error) {
	if o.take(opOpen) {
		return nil, errors.New("injected open")
	}
	file, err := o.base.openExclusive(path, mode)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	o.opens++
	o.mu.Unlock()
	return &faultBatchFile{BatchFile: file, owner: o}, nil
}
func (o *faultFileOps) chmod(path string, mode fs.FileMode) error {
	o.mu.Lock()
	if o.chmodFailures != 0 {
		o.chmodFailures--
		o.mu.Unlock()
		return errors.New("injected chmod")
	}
	o.mu.Unlock()
	return o.base.chmod(path, mode)
}
func (o *faultFileOps) rename(oldPath, newPath string) error {
	o.mu.Lock()
	o.renames = append(o.renames, oldPath)
	o.mu.Unlock()
	if o.take(opRename) {
		return errors.New("injected rename")
	}
	return o.base.rename(oldPath, newPath)
}
func (o *faultFileOps) remove(path string) error {
	if o.take(opRemoveTmp) {
		return errors.New("injected remove")
	}
	return o.base.remove(path)
}
func (o *faultFileOps) readDir(path string) ([]fs.DirEntry, error) { return o.base.readDir(path) }
func (o *faultFileOps) stat(path string) (fs.FileInfo, error)      { return o.base.stat(path) }
func (o *faultFileOps) openCount() int                             { o.mu.Lock(); defer o.mu.Unlock(); return o.opens }
func (o *faultFileOps) closeCount() int                            { o.mu.Lock(); defer o.mu.Unlock(); return o.closes }
func (o *faultFileOps) renameSources() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.renames...)
}

type faultBatchFile struct {
	BatchFile batchFile
	owner     *faultFileOps
}

func (f *faultBatchFile) Write(data []byte) (int, error) {
	if f.owner.take(opWrite) {
		if len(data) == 1 {
			return 0, nil
		}
		return len(data) / 2, nil
	}
	return f.BatchFile.Write(data)
}
func (f *faultBatchFile) Close() error {
	f.owner.mu.Lock()
	f.owner.closes++
	f.owner.mu.Unlock()
	if f.owner.take(opClose) {
		_ = f.BatchFile.Close()
		return errors.New("injected close")
	}
	return f.BatchFile.Close()
}

type manualClock struct {
	mu          sync.Mutex
	nowTime     time.Time
	timers      []*manualTimer
	timerResets int
}

type manualTimer struct {
	clock  *manualClock
	ch     chan time.Time
	due    time.Time
	active bool
}

func newManualClock() *manualClock    { return &manualClock{nowTime: time.Unix(1_700_000_000, 0)} }
func (c *manualClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.nowTime }
func (c *manualClock) newTimer(d time.Duration) writerTimer {
	t := &manualTimer{clock: c, ch: make(chan time.Time, 1)}
	c.mu.Lock()
	c.timers = append(c.timers, t)
	t.due = c.nowTime.Add(d)
	t.active = true
	c.mu.Unlock()
	return t
}
func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	c.nowTime = c.nowTime.Add(d)
	now := c.nowTime
	for _, timer := range c.timers {
		if timer.active && !timer.due.After(now) {
			timer.active = false
			select {
			case timer.ch <- now:
			default:
			}
		}
	}
	c.mu.Unlock()
}
func (c *manualClock) hasActiveTimer() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, timer := range c.timers {
		if timer.active {
			return true
		}
	}
	return false
}
func (c *manualClock) resetCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timerResets
}
func (t *manualTimer) C() <-chan time.Time { return t.ch }
func (t *manualTimer) Reset(d time.Duration) {
	t.clock.mu.Lock()
	select {
	case <-t.ch:
	default:
	}
	t.due = t.clock.nowTime.Add(d)
	t.active = true
	t.clock.timerResets++
	t.clock.mu.Unlock()
}
func (t *manualTimer) Stop() { t.clock.mu.Lock(); t.active = false; t.clock.mu.Unlock() }

func mustWriteFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
func readyFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		if canonicalReadyName.MatchString(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}
func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for callback")
	}
}
func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		runtime.Gosched()
	}
}
func advanceClockUntilSignal(t *testing.T, clock *manualClock, published <-chan struct{}, step time.Duration) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		select {
		case <-published:
			return
		default:
		}
		clock.advance(step)
		runtime.Gosched()
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for manual-clock callback")
		}
	}
}

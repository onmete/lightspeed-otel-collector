package agenticexporter

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	canonicalSealedName = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.jsonl$`)
	canonicalReadyName  = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.json$`)
)

func TestStreamWriterPublishesRecordAndKeepsReadyFilesImmutable(t *testing.T) {
	root := t.TempDir()
	paths := testWriterPaths(root)
	if err := os.MkdirAll(paths.staging, 0o770); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(paths.ready, 0o770); err != nil {
		t.Fatal(err)
	}
	staleSource := ".00000000-0000-4000-8000-000000000000.jsonl.tmp"
	staleArray := ".00000000-0000-4000-8000-000000000001.json.tmp"
	sealedSource := "00000000-0000-4000-8000-000000000002.jsonl"
	untouched := []string{
		".00000000-0000-4000-8000-000000000003.jsonl",
		".00000000-0000-4000-8000-000000000004.tmp",
		".00000000-0000-4000-8000-000000000005.JSONL.tmp",
		".not-a-uuid.jsonl.tmp",
		"notes",
	}
	mustWriteFile(t, filepath.Join(paths.staging, staleSource), []byte("incomplete source"))
	mustWriteFile(t, filepath.Join(paths.staging, staleArray), []byte("incomplete array"))
	mustWriteFile(t, filepath.Join(paths.staging, sealedSource), []byte("{\"leftover\":true}\n"))
	for _, name := range untouched {
		mustWriteFile(t, filepath.Join(paths.staging, name), []byte("keep"))
	}
	mustWriteFile(t, filepath.Join(paths.ready, "legacy.jsonl"), []byte("unrelated"))

	ops := &faultFileOps{base: osFileOps{}, failures: map[fileOperation]int{}}
	published := make(chan [2]int64, 1)
	w := newTestStreamWriter(root, 64)
	w.ops, w.maxFileBytes = ops, 8
	w.callbacks.published = func(records, bytes int64) {
		published <- [2]int64{records, bytes}
	}
	if err := w.start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	for _, name := range []string{staleSource, staleArray} {
		if _, err := os.Stat(filepath.Join(paths.staging, name)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("incomplete temp %q remains after startup: %v", name, err)
		}
	}
	for _, name := range append(untouched, sealedSource) {
		got, err := os.ReadFile(filepath.Join(paths.staging, name))
		if err != nil || (name == sealedSource && string(got) != "{\"leftover\":true}\n") ||
			(name != sealedSource && string(got) != "keep") {
			t.Fatalf("startup changed staging file %q: %q, %v", name, got, err)
		}
	}
	if got := w.snapshot(); got.unpublishedRecords != 0 || got.readyFilesCreated != 0 {
		t.Fatalf("startup replayed a sealed source: %+v", got)
	}
	if directories := ops.readDirPaths(); len(directories) != 1 || directories[0] != paths.staging {
		t.Fatalf("startup inspected directories %v, want only staging", directories)
	}

	record := []byte("{\"a\":1}\n")
	if !w.tryEnqueue(record) {
		t.Fatal("record rejected")
	}
	var publication [2]int64
	select {
	case publication = <-published:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for publication")
	}
	ready := readyFiles(t, paths.ready)
	if len(ready) != 1 {
		t.Fatalf("ready files = %v", ready)
	}
	readyPath := filepath.Join(paths.ready, ready[0])
	got, err := os.ReadFile(readyPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "[{\"a\":1}]" {
		t.Fatalf("ready content = %q, want complete array", got)
	}
	if publication != [2]int64{1, int64(len(record) + 1)} {
		t.Fatalf("publication callback = %v, want one record and final array bytes %d", publication, len(record)+1)
	}
	waitUntil(t, func() bool {
		sealed := sealedFiles(t, paths.staging)
		return len(sealed) == 1 && sealed[0] == sealedSource
	})
	if got := w.snapshot(); got.unpublishedBytes != 0 || got.readyBytesCreated != int64(len(record)+1) {
		t.Fatalf("publication accounting = %+v", got)
	}
	assertPostCommitPrivate(t, ops, paths.staging)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w.shutdown(ctx)
	if got, _ := os.ReadFile(readyPath); string(got) != "[{\"a\":1}]" {
		t.Fatalf("ready file changed after publication: %q", got)
	}
	if got, _ := os.ReadFile(filepath.Join(paths.ready, "legacy.jsonl")); string(got) != "unrelated" {
		t.Fatalf("unrelated ready file changed: %q", got)
	}
}

func TestStreamWriterSharedSpoolPermissions(t *testing.T) {
	root := t.TempDir()
	paths := testWriterPaths(root)
	published := make(chan struct{}, 1)
	w := newTestStreamWriter(root, 64)
	w.maxFileBytes = 1
	w.callbacks.published = func(int64, int64) { published <- struct{}{} }
	if err := w.start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	for _, directory := range []string{paths.staging, paths.ready} {
		directoryInfo, err := os.Stat(directory)
		if err != nil {
			t.Fatalf("Stat(directory): %v", err)
		}
		if got := directoryInfo.Mode().Perm(); got != 0o770 {
			t.Fatalf("directory permissions = %o, want 770", got)
		}
		if directoryInfo.Mode()&fs.ModeSetgid == 0 {
			t.Fatal("spool directory is not setgid")
		}
	}

	if !w.tryEnqueue([]byte("record\n")) {
		t.Fatal("record rejected")
	}
	waitSignal(t, published)
	ready := readyFiles(t, paths.ready)
	if len(ready) != 1 {
		t.Fatalf("ready files = %v", ready)
	}
	fileInfo, err := os.Stat(filepath.Join(paths.ready, ready[0]))
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
	w := newTestStreamWriter(t.TempDir(), 8)
	if !w.tryEnqueue([]byte("1234567\n")) {
		t.Fatal("exact budget fit rejected")
	}
	if w.tryEnqueue([]byte("\n")) {
		t.Fatal("one-byte overflow accepted")
	}
	if w.tryEnqueue(nil) || w.tryEnqueue([]byte("not terminated")) {
		t.Fatal("invalid record accepted")
	}

	oversized := newTestStreamWriter(t.TempDir(), 4)
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
	root := t.TempDir()
	paths := testWriterPaths(root)
	clock := newManualClock()
	published := make(chan struct{}, 2)
	w := newTestStreamWriter(root, 64)
	w.clock = clock
	w.maxFileBytes = 64
	w.maxBatchAge = 30 * time.Second
	w.callbacks.published = func(int64, int64) { published <- struct{}{} }
	_ = w.start(context.Background())
	if !w.tryEnqueue([]byte("one\n")) {
		t.Fatal("enqueue rejected")
	}
	waitUntil(t, func() bool { return w.snapshot().openBatchRecords == 1 && clock.hasActiveTimer() })
	if files := readyFiles(t, paths.ready); len(files) != 0 {
		t.Fatalf("active batch became public before age trigger: %v", files)
	}
	startedAt := clock.now()
	clock.advance(29 * time.Second)
	runtime.Gosched()
	select {
	case <-published:
		t.Fatal("batch published before 30 seconds from its first complete write")
	default:
	}
	if files := readyFiles(t, paths.ready); len(files) != 0 {
		t.Fatalf("batch became public before the 30-second trigger: %v", files)
	}
	clock.advance(time.Second)
	waitSignal(t, published)
	if elapsed := clock.now().Sub(startedAt); elapsed != 30*time.Second {
		t.Fatalf("time trigger elapsed = %v, want exactly 30s", elapsed)
	}
	first := readyFiles(t, paths.ready)
	if len(first) != 1 {
		t.Fatalf("time trigger ready files = %v", first)
	}
	if got, err := os.ReadFile(filepath.Join(paths.ready, first[0])); err != nil || string(got) != "[one]" {
		t.Fatalf("time-triggered array = %q, %v", got, err)
	}

	if !w.tryEnqueue([]byte("two\n")) {
		t.Fatal("enqueue rejected")
	}
	waitUntil(t, func() bool { return w.snapshot().openBatchRecords == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w.shutdown(ctx)
	waitSignal(t, published)
	if files := readyFiles(t, paths.ready); len(files) != 2 {
		t.Fatalf("ready count = %d, want 2", len(files))
	}

	emptyRoot := t.TempDir()
	empty := newTestStreamWriter(emptyRoot, 64)
	_ = empty.start(context.Background())
	empty.shutdown(context.Background())
	if files := readyFiles(t, testWriterPaths(emptyRoot).ready); len(files) != 0 {
		t.Fatalf("empty writer created files: %v", files)
	}
}
func TestStreamWriterProjectsExclusiveArrayLimitBeforeAddingRecord(t *testing.T) {
	root := t.TempDir()
	paths := testWriterPaths(root)
	published := make(chan [2]int64, 3)
	w := newTestStreamWriter(root, 64)
	w.maxFileBytes = 100
	w.maxBatchAge = time.Hour
	w.maxPublishedBytes = 7
	w.callbacks.published = func(records, bytes int64) {
		published <- [2]int64{records, bytes}
	}
	_ = w.start(context.Background())
	for range 3 {
		if !w.tryEnqueue([]byte("{}\n")) {
			t.Fatal("record rejected")
		}
	}
	waitUntil(t, func() bool {
		snapshot := w.snapshot()
		return snapshot.readyFilesCreated == 2 && snapshot.openBatchRecords == 1
	})
	if files := readyFiles(t, paths.ready); len(files) != 2 {
		t.Fatalf("records were not split before the exact array limit: %v", files)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w.shutdown(ctx)
	for range 3 {
		if got := waitForPublication(t, published); got != [2]int64{1, 4} {
			t.Fatalf("publication = %v, want one record and four array bytes", got)
		}
	}
	files := readyFiles(t, paths.ready)
	if len(files) != 3 {
		t.Fatalf("ready files = %v, want one array per record", files)
	}
	for _, name := range files {
		got, err := os.ReadFile(filepath.Join(paths.ready, name))
		if err != nil || string(got) != "[{}]" || len(got) >= int(w.maxPublishedBytes) {
			t.Fatalf("array %q = %q, %v", name, got, err)
		}
	}
	if snapshot := w.snapshot(); snapshot.readyRecordsCreated != 3 ||
		snapshot.readyBytesCreated != 12 || snapshot.unpublishedBytes != 0 {
		t.Fatalf("split publication accounting = %+v", snapshot)
	}
}

func TestStreamWriterFailureRecoveryMatrix(t *testing.T) {
	for _, operation := range []fileOperation{opMkdir, opOpen, opRead, opWrite, opClose, opRename} {
		t.Run(string(operation), func(t *testing.T) {
			root := t.TempDir()
			paths := testWriterPaths(root)
			clock := newManualClock()
			ops := &faultFileOps{base: osFileOps{}, failures: map[fileOperation]int{operation: 1}}
			transitions := make(chan stateEdge, 4)
			published := make(chan [2]int64, 1)
			w := newTestStreamWriter(root, 64)
			w.ops, w.clock, w.maxFileBytes = ops, clock, 4
			w.callbacks.stateChanged = func(from, to streamState, op fileOperation) {
				transitions <- stateEdge{from: from, to: to, operation: op}
			}
			w.callbacks.published = func(records, bytes int64) {
				published <- [2]int64{records, bytes}
			}
			_ = w.start(context.Background())
			if !w.tryEnqueue([]byte("abc\n")) {
				t.Fatal("enqueue rejected")
			}
			waitUntil(t, func() bool { return w.snapshot().state == streamDegraded && clock.hasActiveTimer() })
			snap := w.snapshot()
			if snap.operationFailures[operation] != 1 || snap.unpublishedBytes != 4 {
				t.Fatalf("degraded snapshot = %+v", snap)
			}
			clock.advance(recoveryInitialBackoff)
			if got := waitForPublication(t, published); got != [2]int64{1, 5} {
				t.Fatalf("publication callback = %v, want one record and five array bytes", got)
			}
			waitUntil(t, func() bool { return w.snapshot().state == streamHealthy })
			if got := w.snapshot(); got.unpublishedBytes != 0 || got.readyFilesCreated != 1 || got.readyBytesCreated != 5 {
				t.Fatalf("recovered snapshot = %+v", got)
			}
			if files := readyFiles(t, paths.ready); len(files) != 1 {
				t.Fatalf("recovered ready files = %v", files)
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
	root := t.TempDir()
	paths := testWriterPaths(root)
	ops := &faultFileOps{base: osFileOps{}, chmodFailures: 1, failures: map[fileOperation]int{}}
	w := newTestStreamWriter(root, 64)
	w.ops = ops
	_ = w.start(context.Background())
	if !w.tryEnqueue([]byte("abc\n")) {
		t.Fatal("enqueue rejected")
	}
	waitUntil(t, func() bool {
		snapshot := w.snapshot()
		return snapshot.state == streamDegraded && snapshot.operationFailures[opOpen] == 1
	})
	entries, err := os.ReadDir(paths.staging)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if canonicalSourceTmpName.MatchString(entry.Name()) || canonicalArrayTmpName.MatchString(entry.Name()) {
			t.Fatalf("owned temp file remained after chmod failure: %s", entry.Name())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.shutdown(ctx)
}

func TestStreamWriterRemoveFailureBlocksNewTemp(t *testing.T) {
	root := t.TempDir()
	clock := newManualClock()
	ops := &faultFileOps{base: osFileOps{}, failures: map[fileOperation]int{opWrite: 1, opRemoveTmp: 1}}
	w := newTestStreamWriter(root, 64)
	w.ops, w.clock, w.maxFileBytes = ops, clock, 4
	_ = w.start(context.Background())
	if !w.tryEnqueue([]byte("abc\n")) {
		t.Fatal("enqueue rejected")
	}
	waitUntil(t, func() bool { return w.snapshot().operationFailures[opRemoveTmp] == 1 && clock.hasActiveTimer() })
	if got := ops.sourceOpenCount(); got != 1 {
		t.Fatalf("source open count after failed removal = %d", got)
	}
	clock.advance(recoveryInitialBackoff)
	waitUntil(t, func() bool { return w.snapshot().state == streamHealthy && w.snapshot().unpublishedBytes == 0 })
	if got := ops.sourceOpenCount(); got != 2 {
		t.Fatalf("source open count after recovery = %d, want 2", got)
	}
	w.shutdown(context.Background())
}

func TestStreamWriterRenameRetriesSameClosedArray(t *testing.T) {
	root := t.TempDir()
	paths := testWriterPaths(root)
	clock := newManualClock()
	ops := &faultFileOps{base: osFileOps{}, finalRenameFailures: 1, finalRenameErr: syscall.EXDEV}
	published := make(chan [2]int64, 1)
	w := newTestStreamWriter(root, 64)
	w.ops, w.clock, w.maxFileBytes = ops, clock, 4
	w.callbacks.published = func(records, bytes int64) {
		published <- [2]int64{records, bytes}
	}
	_ = w.start(context.Background())
	_ = w.tryEnqueue([]byte("abc\n"))
	waitUntil(t, func() bool { return w.snapshot().state == streamDegraded && clock.hasActiveTimer() })
	if files := readyFiles(t, paths.ready); len(files) != 0 {
		t.Fatalf("array became public after failed final rename: %v", files)
	}
	if sealed := sealedFiles(t, paths.staging); len(sealed) != 1 {
		t.Fatalf("cross-device rename did not retain the sealed source: %v", sealed)
	}
	if arrays := arrayTemps(t, paths.staging); len(arrays) != 1 {
		t.Fatalf("cross-device rename did not retain the closed array: %v", arrays)
	}
	first := ops.finalRenameSources()
	if len(first) != 1 {
		t.Fatalf("final rename attempts = %v", first)
	}
	clock.advance(recoveryInitialBackoff)
	if got := waitForPublication(t, published); got != [2]int64{1, 5} {
		t.Fatalf("publication callback = %v, want one record and five array bytes", got)
	}
	attempts := ops.finalRenameSources()
	if len(attempts) != 2 || attempts[0] != attempts[1] {
		t.Fatalf("final rename did not retry the same closed array: %v", attempts)
	}
	if got := ops.arrayCloseCount(); got != 1 {
		t.Fatalf("array closed %d times, want one close across rename retry", got)
	}
	assertPostCommitPrivate(t, ops, paths.staging)
	w.shutdown(context.Background())
}

func TestStreamWriterShutdownDeadlinePreservesSealedSource(t *testing.T) {
	root := t.TempDir()
	paths := testWriterPaths(root)
	blockedOps := &faultFileOps{base: osFileOps{}, finalRenameFailures: 100}
	blocked := newTestStreamWriter(root, 64)
	blocked.ops, blocked.maxFileBytes, blocked.maxBatchAge = blockedOps, 4, time.Hour
	deadline := make(chan [2]int64, 1)
	blocked.callbacks.shutdownDeadline = func(records, bytes int64) {
		deadline <- [2]int64{records, bytes}
	}
	_ = blocked.start(context.Background())
	_ = blocked.tryEnqueue([]byte("abc\n"))
	waitUntil(t, func() bool {
		return blocked.snapshot().state == streamDegraded && len(sealedFiles(t, paths.staging)) == 1
	})
	if files := readyFiles(t, paths.ready); len(files) != 0 {
		t.Fatalf("array became public before shutdown recovery: %v", files)
	}

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
	if got := blocked.snapshot(); got.unpublishedRecords != 1 || got.unpublishedBytes != 4 {
		t.Fatalf("blocked reservation released: %+v", got)
	}
	if len(sealedFiles(t, paths.staging)) != 1 {
		t.Fatal("complete sealed source was not retained at shutdown deadline")
	}
	if len(arrayTemps(t, paths.staging)) != 1 {
		t.Fatal("closed candidate array was not retained at shutdown deadline")
	}
}

func TestStreamWriterShutdownDeadlineCancelsInFlightConversion(t *testing.T) {
	for _, blockedIO := range []struct {
		name      string
		blockRead bool
	}{
		{name: "reader", blockRead: true},
		{name: "array writer"},
	} {
		t.Run(blockedIO.name, func(t *testing.T) {
			root := t.TempDir()
			paths := testWriterPaths(root)
			ioStarted := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			ops := &faultFileOps{base: osFileOps{}, failures: map[fileOperation]int{}}
			if blockedIO.blockRead {
				ops.blockedReadStarted = ioStarted
				ops.blockedReadRelease = release
			} else {
				ops.blockedArrayWriteStarted = ioStarted
				ops.blockedArrayWriteRelease = release
			}
			w := newTestStreamWriter(root, 64)
			clock := newManualClock()
			w.ops, w.clock, w.maxFileBytes, w.maxBatchAge = ops, clock, 1, 30*time.Second
			if err := w.start(context.Background()); err != nil {
				t.Fatalf("start: %v", err)
			}
			published := make(chan struct{}, 1)
			otherCallbacks := make(chan struct{}, 4)
			deadline := make(chan [2]int64, 1)
			w.callbacks.published = func(int64, int64) { published <- struct{}{} }
			w.callbacks.operationFailed = func(fileOperation) { otherCallbacks <- struct{}{} }
			w.callbacks.stateChanged = func(streamState, streamState, fileOperation) {
				otherCallbacks <- struct{}{}
			}
			w.callbacks.shutdownDeadline = func(records, bytes int64) {
				deadline <- [2]int64{records, bytes}
			}
			record := []byte("{\"value\":1}\n")
			if !w.tryEnqueue(record) {
				t.Fatal("record rejected")
			}
			select {
			case <-ioStarted:
			case <-time.After(time.Second):
				t.Fatal("conversion did not enter blocked I/O")
			}
			clock.advance(w.maxBatchAge + time.Second)
			if age := w.snapshot().openBatchAge; age <= w.maxBatchAge {
				t.Fatalf("blocked conversion age = %v, want it expired past %v", age, w.maxBatchAge)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			shutdownDone := make(chan struct{})
			go func() {
				w.shutdown(ctx)
				close(shutdownDone)
			}()
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("shutdown deadline did not expire")
			}
			select {
			case <-shutdownDone:
			case <-time.After(time.Second):
				t.Fatal("shutdown returned without resolving the blocked conversion")
			}
			if clock.hasActiveTimer() {
				t.Fatal("writer timer remained active after shutdown")
			}
			select {
			case counts := <-deadline:
				if counts != [2]int64{1, int64(len(record))} {
					t.Fatalf("deadline counts = %v", counts)
				}
			default:
				t.Fatal("shutdown deadline was not reported")
			}
			select {
			case <-published:
				t.Fatal("in-flight conversion published after the shutdown deadline")
			default:
			}
			select {
			case <-otherCallbacks:
				t.Fatal("in-flight conversion emitted a failure/state callback")
			default:
			}
			if files := readyFiles(t, paths.ready); len(files) != 0 {
				t.Fatalf("in-flight conversion published ready files: %v", files)
			}
			if attempts := ops.finalRenameSources(); len(attempts) != 0 {
				t.Fatalf("in-flight conversion attempted final rename: %v", attempts)
			}
			sealed := sealedFiles(t, paths.staging)
			if len(sealed) != 1 {
				t.Fatalf("sealed sources = %v, want one retained complete source", sealed)
			}
			if got, err := os.ReadFile(filepath.Join(paths.staging, sealed[0])); err != nil || string(got) != string(record) {
				t.Fatalf("retained source = %q, %v", got, err)
			}
			if arrays := arrayTemps(t, paths.staging); len(arrays) != 1 {
				t.Fatalf("closed candidate arrays = %v, want one retained private array", arrays)
			}
			if got := ops.readerCloseCount(); got != 1 {
				t.Fatalf("source reader close count = %d, want 1", got)
			}
			if got := ops.arrayCloseCount(); got != 1 {
				t.Fatalf("array writer close count = %d, want 1", got)
			}
			snapshot := w.snapshot()
			if snapshot.unpublishedRecords != 1 || snapshot.unpublishedBytes != int64(len(record)) ||
				snapshot.readyFilesCreated != 0 || snapshot.readyBytesCreated != 0 {
				t.Fatalf("deadline accounting = %+v", snapshot)
			}
		})
	}
}

func TestStreamWriterShutdownCancellationCancelsForcedFlushConversion(t *testing.T) {
	root := t.TempDir()
	paths := testWriterPaths(root)
	ioStarted := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	ops := &faultFileOps{
		base:                     osFileOps{},
		failures:                 map[fileOperation]int{},
		blockedArrayWriteStarted: ioStarted,
		blockedArrayWriteRelease: release,
	}
	w := newTestStreamWriter(root, 64)
	clock := newManualClock()
	w.ops, w.clock, w.maxFileBytes, w.maxBatchAge = ops, clock, 64, time.Hour
	if err := w.start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	published := make(chan struct{}, 1)
	otherCallbacks := make(chan struct{}, 4)
	deadline := make(chan [2]int64, 1)
	w.callbacks.published = func(int64, int64) { published <- struct{}{} }
	w.callbacks.operationFailed = func(fileOperation) { otherCallbacks <- struct{}{} }
	w.callbacks.stateChanged = func(streamState, streamState, fileOperation) {
		otherCallbacks <- struct{}{}
	}
	w.callbacks.shutdownDeadline = func(records, bytes int64) {
		deadline <- [2]int64{records, bytes}
	}
	record := []byte("{\"value\":1}\n")
	if !w.tryEnqueue(record) {
		t.Fatal("record rejected")
	}
	waitUntil(t, func() bool { return w.snapshot().openBatchBytes == int64(len(record)) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdownDone := make(chan struct{})
	go func() {
		w.shutdown(ctx)
		close(shutdownDone)
	}()
	select {
	case <-ioStarted:
	case <-time.After(time.Second):
		t.Fatal("forced shutdown flush did not enter blocked conversion")
	}
	cancel()
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown returned without resolving the forced conversion")
	}
	if clock.hasActiveTimer() {
		t.Fatal("writer timer remained active after shutdown")
	}
	select {
	case counts := <-deadline:
		if counts != [2]int64{1, int64(len(record))} {
			t.Fatalf("deadline counts = %v", counts)
		}
	default:
		t.Fatal("shutdown deadline was not reported")
	}
	select {
	case <-published:
		t.Fatal("forced conversion published after the shutdown deadline")
	default:
	}
	select {
	case <-otherCallbacks:
		t.Fatal("forced conversion emitted a failure/state callback")
	default:
	}
	if files := readyFiles(t, paths.ready); len(files) != 0 {
		t.Fatalf("forced conversion published ready files: %v", files)
	}
	if attempts := ops.finalRenameSources(); len(attempts) != 0 {
		t.Fatalf("forced conversion attempted final rename: %v", attempts)
	}
	sealed := sealedFiles(t, paths.staging)
	if len(sealed) != 1 {
		t.Fatalf("sealed sources = %v, want one retained complete source", sealed)
	}
	if got, err := os.ReadFile(filepath.Join(paths.staging, sealed[0])); err != nil || string(got) != string(record) {
		t.Fatalf("retained source = %q, %v", got, err)
	}
	if arrays := arrayTemps(t, paths.staging); len(arrays) != 1 {
		t.Fatalf("closed candidate arrays = %v, want one retained private array", arrays)
	}
	if got := ops.readerCloseCount(); got != 1 {
		t.Fatalf("source reader close count = %d, want 1", got)
	}
	if got := ops.arrayCloseCount(); got != 1 {
		t.Fatalf("array writer close count = %d, want 1", got)
	}
	snapshot := w.snapshot()
	if snapshot.unpublishedRecords != 1 || snapshot.unpublishedBytes != int64(len(record)) ||
		snapshot.readyFilesCreated != 0 || snapshot.readyBytesCreated != 0 {
		t.Fatalf("deadline accounting = %+v", snapshot)
	}
}

func TestStreamWriterRecoveryBackoffCapsAndSuppressesRepeatedEdges(t *testing.T) {
	var degradedEdges int
	w := newTestStreamWriter(t.TempDir(), 64)
	w.callbacks.stateChanged = func(_ streamState, to streamState, _ fileOperation) {
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
	w := newTestStreamWriter(dir, 64)
	w.ops, w.clock, w.maxFileBytes, w.maxBatchAge = ops, clock, 64, 30*time.Second
	w.callbacks.published = func(int64, int64) { published <- struct{}{} }
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

func TestStreamWriterConversionFailurePreservesSourceAndRecovers(t *testing.T) {
	tests := []struct {
		name      string
		operation fileOperation
		configure func(*faultFileOps)
	}{
		{name: "source open read", operation: opRead, configure: func(ops *faultFileOps) {
			ops.failures[opRead] = 1
		}},
		{name: "source read", operation: opRead, configure: func(ops *faultFileOps) {
			ops.readFailures = 1
		}},
		{name: "array open", operation: opOpen, configure: func(ops *faultFileOps) {
			ops.arrayOpenFailures = 1
		}},
		{name: "array write", operation: opWrite, configure: func(ops *faultFileOps) {
			ops.arrayWriteFailures = 1
		}},
		{name: "source reader close", operation: opClose, configure: func(ops *faultFileOps) {
			ops.readerCloseFailures = 1
		}},
		{name: "array close", operation: opClose, configure: func(ops *faultFileOps) {
			ops.arrayCloseFailures = 1
		}},
		{name: "array chmod", operation: opOpen, configure: func(ops *faultFileOps) {
			ops.arrayChmodFailures = 1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			paths := testWriterPaths(root)
			clock := newManualClock()
			ops := &faultFileOps{base: osFileOps{}, failures: map[fileOperation]int{}}
			test.configure(ops)
			published := make(chan [2]int64, 1)
			w := newTestStreamWriter(root, 64)
			w.ops, w.clock, w.maxFileBytes = ops, clock, 4
			w.callbacks.published = func(records, bytes int64) {
				published <- [2]int64{records, bytes}
			}
			_ = w.start(context.Background())
			record := []byte("{\"x\":1}\n")
			if !w.tryEnqueue(record) {
				t.Fatal("enqueue rejected")
			}
			waitUntil(t, func() bool {
				return w.snapshot().state == streamDegraded &&
					w.snapshot().operationFailures[test.operation] > 0
			})
			sealed := sealedFiles(t, paths.staging)
			if len(sealed) != 1 {
				t.Fatalf("sealed sources after conversion failure = %v", sealed)
			}
			if got, err := os.ReadFile(filepath.Join(paths.staging, sealed[0])); err != nil || string(got) != string(record) {
				t.Fatalf("sealed source after conversion failure = %q, %v", got, err)
			}
			if files := readyFiles(t, paths.ready); len(files) != 0 {
				t.Fatalf("failed conversion exposed public files: %v", files)
			}
			if temps := arrayTemps(t, paths.staging); len(temps) != 0 {
				t.Fatalf("failed conversion retained broken candidate arrays: %v", temps)
			}

			clock.advance(recoveryInitialBackoff)
			if got := waitForPublication(t, published); got != [2]int64{1, int64(len(record) + 1)} {
				t.Fatalf("publication callback = %v, want one record and %d array bytes", got, len(record)+1)
			}
			waitUntil(t, func() bool {
				return w.snapshot().state == streamHealthy && len(sealedFiles(t, paths.staging)) == 0
			})
			ready := readyFiles(t, paths.ready)
			if len(ready) != 1 {
				t.Fatalf("recovered ready files = %v", ready)
			}
			got, err := os.ReadFile(filepath.Join(paths.ready, ready[0]))
			if err != nil || string(got) != "[{\"x\":1}]" {
				t.Fatalf("recovered array = %q, %v", got, err)
			}
			if snapshot := w.snapshot(); snapshot.readyFilesCreated != 1 ||
				snapshot.readyRecordsCreated != 1 || snapshot.readyBytesCreated != int64(len(record)+1) ||
				snapshot.unpublishedRecords != 0 || snapshot.unpublishedBytes != 0 {
				t.Fatalf("recovered publication accounting = %+v", snapshot)
			}
			assertPostCommitPrivate(t, ops, paths.staging)
			w.shutdown(context.Background())
		})
	}
}

func TestStreamWriterArrayTempCleanupRetriesBeforeConversion(t *testing.T) {
	root := t.TempDir()
	paths := testWriterPaths(root)
	clock := newManualClock()
	ops := &faultFileOps{
		base:               osFileOps{},
		failures:           map[fileOperation]int{opRemoveTmp: 1},
		arrayWriteFailures: 1,
	}
	published := make(chan [2]int64, 1)
	w := newTestStreamWriter(root, 64)
	w.ops, w.clock, w.maxFileBytes = ops, clock, 4
	w.callbacks.published = func(records, bytes int64) {
		published <- [2]int64{records, bytes}
	}
	_ = w.start(context.Background())
	record := []byte("{\"x\":1}\n")
	if !w.tryEnqueue(record) {
		t.Fatal("enqueue rejected")
	}
	waitUntil(t, func() bool {
		snapshot := w.snapshot()
		return snapshot.state == streamDegraded &&
			snapshot.operationFailures[opWrite] == 1 &&
			snapshot.operationFailures[opRemoveTmp] == 1 &&
			clock.hasActiveTimer()
	})
	sealed := sealedFiles(t, paths.staging)
	if len(sealed) != 1 {
		t.Fatalf("sealed sources during array cleanup failure = %v", sealed)
	}
	if got, err := os.ReadFile(filepath.Join(paths.staging, sealed[0])); err != nil || string(got) != string(record) {
		t.Fatalf("sealed source during array cleanup failure = %q, %v", got, err)
	}
	if arrays := arrayTemps(t, paths.staging); len(arrays) != 1 {
		t.Fatalf("broken array temp was not retained for cleanup retry: %v", arrays)
	}
	if files := readyFiles(t, paths.ready); len(files) != 0 {
		t.Fatalf("array cleanup failure exposed public files: %v", files)
	}

	clock.advance(recoveryInitialBackoff)
	if got := waitForPublication(t, published); got != [2]int64{1, int64(len(record) + 1)} {
		t.Fatalf("publication callback = %v, want one record and %d array bytes", got, len(record)+1)
	}
	waitUntil(t, func() bool {
		return w.snapshot().state == streamHealthy &&
			len(sealedFiles(t, paths.staging)) == 0 &&
			len(arrayTemps(t, paths.staging)) == 0
	})
	ready := readyFiles(t, paths.ready)
	if len(ready) != 1 {
		t.Fatalf("recovered ready files = %v", ready)
	}
	if got, err := os.ReadFile(filepath.Join(paths.ready, ready[0])); err != nil || string(got) != "[{\"x\":1}]" {
		t.Fatalf("recovered array = %q, %v", got, err)
	}
	if snapshot := w.snapshot(); snapshot.readyFilesCreated != 1 ||
		snapshot.readyBytesCreated != int64(len(record)+1) ||
		snapshot.unpublishedRecords != 0 || snapshot.unpublishedBytes != 0 {
		t.Fatalf("array cleanup recovery accounting = %+v", snapshot)
	}
	assertPostCommitPrivate(t, ops, paths.staging)
	w.shutdown(context.Background())
}

func TestStreamWriterPublishedSourceCleanupRetriesOnlyCleanup(t *testing.T) {
	root := t.TempDir()
	paths := testWriterPaths(root)
	clock := newManualClock()
	ops := &faultFileOps{base: osFileOps{}, failures: map[fileOperation]int{opRemoveSource: 1}}
	published := make(chan [2]int64, 2)
	w := newTestStreamWriter(root, 64)
	w.ops, w.clock, w.maxFileBytes = ops, clock, 4
	w.callbacks.published = func(records, bytes int64) {
		published <- [2]int64{records, bytes}
	}
	_ = w.start(context.Background())
	_ = w.tryEnqueue([]byte("abc\n"))
	waitUntil(t, func() bool {
		snapshot := w.snapshot()
		return snapshot.state == streamDegraded && snapshot.readyFilesCreated == 1
	})
	if got := waitForPublication(t, published); got != [2]int64{1, 5} {
		t.Fatalf("publication callback = %v, want one record and five array bytes", got)
	}
	if got := w.snapshot(); got.unpublishedRecords != 0 || got.unpublishedBytes != 0 ||
		got.readyFilesCreated != 1 || got.readyBytesCreated != 5 {
		t.Fatalf("committed batch accounting = %+v", got)
	}
	if len(sealedFiles(t, paths.staging)) != 1 {
		t.Fatal("sealed source was not retained after cleanup failure")
	}
	if files := readyFiles(t, paths.ready); len(files) != 1 {
		t.Fatalf("committed public array = %v", files)
	}
	finalRenames := ops.finalRenameSources()
	if len(finalRenames) != 1 {
		t.Fatalf("final rename attempts before cleanup recovery = %v", finalRenames)
	}

	clock.advance(recoveryInitialBackoff)
	waitUntil(t, func() bool {
		return w.snapshot().state == streamHealthy && len(sealedFiles(t, paths.staging)) == 0
	})
	if attempts := ops.finalRenameSources(); len(attempts) != 1 {
		t.Fatalf("cleanup retry republished array: final rename attempts = %v", attempts)
	}
	if got := w.snapshot(); got.readyFilesCreated != 1 || got.readyRecordsCreated != 1 || got.readyBytesCreated != 5 {
		t.Fatalf("cleanup retry re-accounted publication: %+v", got)
	}
	select {
	case got := <-published:
		t.Fatalf("cleanup retry invoked publication callback again: %v", got)
	default:
	}
	assertPostCommitPrivate(t, ops, paths.staging)
	w.shutdown(context.Background())
}

type stateEdge struct {
	from, to  streamState
	operation fileOperation
}

type faultFileOps struct {
	base fileOps
	mu   sync.Mutex

	failures                  map[fileOperation]int
	chmodFailures             int
	arrayChmodFailures        int
	arrayOpenFailures         int
	arrayWriteFailures        int
	arrayCloseFailures        int
	readFailures              int
	readerCloseFailures       int
	finalRenameFailures       int
	finalRenameErr            error
	blockedReadStarted        chan struct{}
	blockedReadRelease        <-chan struct{}
	blockedArrayWriteStarted  chan struct{}
	blockedArrayWriteRelease  <-chan struct{}
	blockedFinalRenameStarted chan struct{}
	blockedFinalRenameRelease <-chan struct{}
	readerCloses              int

	opens, closes             int
	sourceOpens, arrayOpens   int
	sourceCloses, arrayCloses int
	renames                   []renameCall
	readDirectories           []string
	openReadPaths             []string
	statPaths                 []string
	removePaths               []string
	pathsAccessedAfterCommit  []string
	postCommitPublicAccesses  []string
	committedTargets          map[string]bool
	committedDirectories      map[string]bool
}

type renameCall struct {
	source      string
	destination string
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
func (o *faultFileOps) takeArrayOpenFailure() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.arrayOpenFailures == 0 {
		return false
	}
	o.arrayOpenFailures--
	return true
}
func (o *faultFileOps) takeArrayWriteFailure() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.arrayWriteFailures == 0 {
		return false
	}
	o.arrayWriteFailures--
	return true
}
func (o *faultFileOps) takeArrayCloseFailure() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.arrayCloseFailures == 0 {
		return false
	}
	o.arrayCloseFailures--
	return true
}
func (o *faultFileOps) takeReadFailure() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.readFailures == 0 {
		return false
	}
	o.readFailures--
	return true
}
func (o *faultFileOps) takeReaderCloseFailure() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.readerCloseFailures == 0 {
		return false
	}
	o.readerCloseFailures--
	return true
}
func (o *faultFileOps) notePath(path string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.committedTargets) != 0 {
		o.pathsAccessedAfterCommit = append(o.pathsAccessedAfterCommit, path)
	}
	if o.committedTargets[path] {
		o.postCommitPublicAccesses = append(o.postCommitPublicAccesses, path)
	}
}
func (o *faultFileOps) mkdirAll(path string, mode fs.FileMode) error {
	o.notePath(path)
	if o.take(opMkdir) {
		return errors.New("injected mkdir")
	}
	return o.base.mkdirAll(path, mode)
}
func (o *faultFileOps) openExclusive(path string, mode fs.FileMode) (batchFile, error) {
	o.notePath(path)
	array := canonicalArrayTmpName.MatchString(filepath.Base(path))
	if array && o.takeArrayOpenFailure() {
		return nil, errors.New("injected array open")
	}
	if o.take(opOpen) {
		return nil, errors.New("injected open")
	}
	file, err := o.base.openExclusive(path, mode)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	o.opens++
	var blockStarted chan struct{}
	var blockRelease <-chan struct{}
	if array {
		o.arrayOpens++
		blockStarted = o.blockedArrayWriteStarted
		blockRelease = o.blockedArrayWriteRelease
	} else {
		o.sourceOpens++
	}
	o.mu.Unlock()
	return &faultBatchFile{
		BatchFile:    file,
		owner:        o,
		array:        array,
		blockStarted: blockStarted,
		blockRelease: blockRelease,
		closed:       make(chan struct{}),
	}, nil
}
func (o *faultFileOps) openRead(path string) (io.ReadCloser, error) {
	o.notePath(path)
	o.mu.Lock()
	o.openReadPaths = append(o.openReadPaths, path)
	blockStarted := o.blockedReadStarted
	blockRelease := o.blockedReadRelease
	o.mu.Unlock()
	if o.take(opRead) {
		return nil, errors.New("injected source open read")
	}
	reader, err := o.base.openRead(path)
	if err != nil {
		return nil, err
	}
	return &faultReadCloser{
		ReadCloser:   reader,
		owner:        o,
		blockStarted: blockStarted,
		blockRelease: blockRelease,
		closed:       make(chan struct{}),
	}, nil
}
func (o *faultFileOps) chmod(path string, mode fs.FileMode) error {
	o.notePath(path)
	array := canonicalArrayTmpName.MatchString(filepath.Base(path))
	o.mu.Lock()
	if array && o.arrayChmodFailures != 0 {
		o.arrayChmodFailures--
		o.mu.Unlock()
		return errors.New("injected array chmod")
	}
	if !array && o.chmodFailures != 0 {
		o.chmodFailures--
		o.mu.Unlock()
		return errors.New("injected chmod")
	}
	o.mu.Unlock()
	return o.base.chmod(path, mode)
}
func (o *faultFileOps) rename(oldPath, newPath string) error {
	o.notePath(oldPath)
	o.notePath(newPath)
	final := filepath.Ext(newPath) == ".json"
	var blockedStarted chan struct{}
	var blockedRelease <-chan struct{}
	o.mu.Lock()
	o.renames = append(o.renames, renameCall{source: oldPath, destination: newPath})
	if final && o.finalRenameFailures != 0 {
		o.finalRenameFailures--
		failure := o.finalRenameErr
		o.mu.Unlock()
		if failure != nil {
			return failure
		}
		return errors.New("injected final rename")
	}
	if final {
		blockedStarted = o.blockedFinalRenameStarted
		blockedRelease = o.blockedFinalRenameRelease
	}
	o.mu.Unlock()
	if blockedStarted != nil && blockedRelease != nil {
		blockedStarted <- struct{}{}
		<-blockedRelease
	}
	if o.take(opRename) {
		return errors.New("injected rename")
	}
	if err := o.base.rename(oldPath, newPath); err != nil {
		return err
	}
	if final {
		o.mu.Lock()
		if o.committedTargets == nil {
			o.committedTargets = make(map[string]bool)
			o.committedDirectories = make(map[string]bool)
		}
		o.committedTargets[newPath] = true
		o.committedDirectories[filepath.Dir(newPath)] = true
		o.mu.Unlock()
	}
	return nil
}
func (o *faultFileOps) remove(path string) error {
	o.notePath(path)
	o.mu.Lock()
	o.removePaths = append(o.removePaths, path)
	o.mu.Unlock()
	operation := opRemoveTmp
	if canonicalSealedName.MatchString(filepath.Base(path)) {
		operation = opRemoveSource
	}
	if o.take(operation) {
		return errors.New("injected remove")
	}
	return o.base.remove(path)
}
func (o *faultFileOps) readDir(path string) ([]fs.DirEntry, error) {
	o.notePath(path)
	o.mu.Lock()
	o.readDirectories = append(o.readDirectories, path)
	if o.committedDirectories[path] {
		o.postCommitPublicAccesses = append(o.postCommitPublicAccesses, path)
	}
	o.mu.Unlock()
	return o.base.readDir(path)
}
func (o *faultFileOps) stat(path string) (fs.FileInfo, error) {
	o.notePath(path)
	o.mu.Lock()
	o.statPaths = append(o.statPaths, path)
	o.mu.Unlock()
	return o.base.stat(path)
}
func (o *faultFileOps) readDirPaths() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.readDirectories...)
}
func (o *faultFileOps) sourceOpenCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.sourceOpens
}
func (o *faultFileOps) arrayCloseCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.arrayCloses
}
func (o *faultFileOps) readerCloseCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.readerCloses
}
func (o *faultFileOps) finalRenameSources() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	var sources []string
	for _, call := range o.renames {
		if filepath.Ext(call.destination) == ".json" {
			sources = append(sources, call.source)
		}
	}
	return sources
}
func (o *faultFileOps) pathsAfterCommit() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.pathsAccessedAfterCommit...)
}
func (o *faultFileOps) publicAccessesAfterCommit() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.postCommitPublicAccesses...)
}

type faultBatchFile struct {
	BatchFile    batchFile
	owner        *faultFileOps
	array        bool
	blockStarted chan struct{}
	blockRelease <-chan struct{}
	closed       chan struct{}
	blockOnce    sync.Once
	closeOnce    sync.Once
}

func (f *faultBatchFile) Write(data []byte) (int, error) {
	if f.array && f.blockStarted != nil {
		f.blockOnce.Do(func() { close(f.blockStarted) })
		select {
		case <-f.blockRelease:
		case <-f.closed:
			return 0, io.ErrClosedPipe
		}
	}
	if f.array && f.owner.takeArrayWriteFailure() {
		return len(data) / 2, nil
	}
	if !f.array && f.owner.take(opWrite) {
		if len(data) == 1 {
			return 0, nil
		}
		return len(data) / 2, nil
	}
	return f.BatchFile.Write(data)
}
func (f *faultBatchFile) Close() error {
	f.closeOnce.Do(func() { close(f.closed) })
	f.owner.mu.Lock()
	f.owner.closes++
	if f.array {
		f.owner.arrayCloses++
	} else {
		f.owner.sourceCloses++
	}
	f.owner.mu.Unlock()
	if f.array && f.owner.takeArrayCloseFailure() {
		_ = f.BatchFile.Close()
		return errors.New("injected array close")
	}
	if f.owner.take(opClose) {
		_ = f.BatchFile.Close()
		return errors.New("injected close")
	}
	return f.BatchFile.Close()
}

type faultReadCloser struct {
	io.ReadCloser
	owner        *faultFileOps
	blockStarted chan struct{}
	blockRelease <-chan struct{}
	closed       chan struct{}
	blockOnce    sync.Once
	closeOnce    sync.Once
}

func (r *faultReadCloser) Read(data []byte) (int, error) {
	if r.blockStarted != nil {
		r.blockOnce.Do(func() { close(r.blockStarted) })
		select {
		case <-r.blockRelease:
		case <-r.closed:
			return 0, io.ErrClosedPipe
		}
	}
	if r.owner.takeReadFailure() {
		return 0, errors.New("injected source read")
	}
	return r.ReadCloser.Read(data)
}
func (r *faultReadCloser) Close() error {
	r.closeOnce.Do(func() { close(r.closed) })
	err := r.ReadCloser.Close()
	r.owner.mu.Lock()
	r.owner.readerCloses++
	r.owner.mu.Unlock()
	if r.owner.takeReaderCloseFailure() {
		return errors.New("injected source close")
	}
	return err
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
	now := t.clock.nowTime
	t.due = now.Add(d)
	t.active = true
	if !t.due.After(now) {
		t.active = false
		select {
		case t.ch <- now:
		default:
		}
	}
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

type writerTestPaths struct {
	staging string
	ready   string
}

func testWriterPaths(root string) writerTestPaths {
	return writerTestPaths{
		staging: filepath.Join(root, "staging"),
		ready:   filepath.Join(root, "export", "traces", "v1"),
	}
}

func newTestStreamWriter(root string, maxBacklogBytes int64) *streamWriter {
	paths := testWriterPaths(root)
	return newStreamWriter(paths.staging, paths.ready, maxBacklogBytes)
}

func sealedFiles(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		if canonicalSealedName.MatchString(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}

func arrayTemps(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		if canonicalArrayTmpName.MatchString(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}

func waitForPublication(t *testing.T, ch <-chan [2]int64) [2]int64 {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for publication")
		return [2]int64{}
	}
}

func assertPostCommitPrivate(t *testing.T, ops *faultFileOps, stagingDirectory string) {
	t.Helper()
	for _, path := range ops.pathsAfterCommit() {
		relative, err := filepath.Rel(stagingDirectory, path)
		if err != nil || !filepath.IsLocal(relative) {
			t.Errorf("post-commit operation accessed outside staging: %q", path)
		}
	}
	if accesses := ops.publicAccessesAfterCommit(); len(accesses) != 0 {
		t.Errorf("writer accessed public files after commit: %v", accesses)
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

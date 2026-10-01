package agenticexporter

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestCopyJSONLAsArrayPreservesObjects(t *testing.T) {
	source := "{\"value\":\"line\\ntext\"}\n{\"number\":42}\n"
	want := "[{\"value\":\"line\\ntext\"},{\"number\":42}]"
	var output bytes.Buffer
	n, err := copyJSONLAsArray(strings.NewReader(source), &output, int64(len(want)+1))
	if err != nil {
		t.Fatalf("copyJSONLAsArray() error = %v", err)
	}
	if got := output.String(); got != want {
		t.Fatalf("array = %q, want %q", got, want)
	}
	if n != int64(len(want)) || n != int64(len(source)+1) {
		t.Fatalf("written bytes = %d, want final array size %d and source size plus one %d", n, len(want), len(source)+1)
	}
}

func TestCopyJSONLAsArrayRejectsLimitEquality(t *testing.T) {
	var output bytes.Buffer
	n, err := copyJSONLAsArray(strings.NewReader("{}\n"), &output, 4)
	if !errors.Is(err, errArrayTooLarge) {
		t.Fatalf("copyJSONLAsArray() error = %v, want %v", err, errArrayTooLarge)
	}
	if output.Len() >= 4 {
		t.Fatalf("rejected output length = %d, want less than exclusive limit", output.Len())
	}
	if n != int64(output.Len()) {
		t.Fatalf("reported bytes = %d, actual bytes = %d", n, output.Len())
	}
}

func TestCopyJSONLAsArrayStreamsRecordLargerThanBuffer(t *testing.T) {
	longRecord := "{\"value\":\"" + strings.Repeat("x", 64*1024) + "\\nend\"}"
	source := longRecord + "\n{\"next\":true}\n"
	want := "[" + longRecord + ",{\"next\":true}]"
	var output bytes.Buffer
	n, err := copyJSONLAsArray(strings.NewReader(source), &output, int64(len(want)+1))
	if err != nil {
		t.Fatalf("copyJSONLAsArray() error = %v", err)
	}
	if got := output.String(); got != want {
		t.Fatal("long array conversion changed record bytes")
	}
	if n != int64(len(want)) || n != int64(len(source)+1) {
		t.Fatalf("written bytes = %d, want %d", n, len(want))
	}
}

func TestCopyJSONLAsArrayReportsShortWrites(t *testing.T) {
	output := &shortJSONArrayWriter{}
	n, err := copyJSONLAsArray(strings.NewReader("{}\n"), output, 16)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("copyJSONLAsArray() error = %v, want %v", err, io.ErrShortWrite)
	}
	if n != int64(output.Len()) || output.String() != "[{" {
		t.Fatalf("reported/output bytes = %d/%q, want actual short output 2/\"[{\"", n, output.String())
	}
}

func TestCopyJSONLAsArrayPropagatesReadErrors(t *testing.T) {
	readErr := errors.New("source read failed")
	var output bytes.Buffer
	n, err := copyJSONLAsArray(errorJSONArrayReader{err: readErr}, &output, 16)
	if !errors.Is(err, readErr) {
		t.Fatalf("copyJSONLAsArray() error = %v, want source read error", err)
	}
	if n != int64(output.Len()) || output.String() != "[" {
		t.Fatalf("reported/output bytes = %d/%q, want one written bracket", n, output.String())
	}
}

func TestCopyJSONLAsArrayRejectsEmptyAndUnterminatedSources(t *testing.T) {
	for _, source := range []string{"", "{}\n{\"missingLF\":true}"} {
		var output bytes.Buffer
		_, err := copyJSONLAsArray(strings.NewReader(source), &output, 128)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("copyJSONLAsArray(%q) error = %v, want %v", source, err, io.ErrUnexpectedEOF)
		}
	}
}

type shortJSONArrayWriter struct {
	bytes.Buffer
	calls int
}

func (w *shortJSONArrayWriter) Write(data []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		return w.Buffer.Write(data)
	}
	return w.Buffer.Write(data[:len(data)-1])
}

type errorJSONArrayReader struct {
	err error
}

func (r errorJSONArrayReader) Read([]byte) (int, error) { return 0, r.err }

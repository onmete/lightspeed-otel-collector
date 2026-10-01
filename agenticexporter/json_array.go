package agenticexporter

import (
	"bufio"
	"errors"
	"io"
)

var errArrayTooLarge = errors.New("published array exceeds size limit")

var (
	jsonArrayOpen  = [...]byte{'['}
	jsonArrayClose = [...]byte{']'}
	jsonArrayComma = [...]byte{','}
)

type jsonArrayReadError struct {
	err error
}

func (e jsonArrayReadError) Error() string { return e.err.Error() }
func (e jsonArrayReadError) Unwrap() error { return e.err }

// copyJSONLAsArray streams complete LF-terminated JSONL records into one JSON
// array without parsing or retaining a whole record or file.
func copyJSONLAsArray(src io.Reader, dst io.Writer, exclusiveLimit int64) (int64, error) {
	var written int64
	write := func(p []byte) error {
		if written >= exclusiveLimit || int64(len(p)) >= exclusiveLimit-written {
			return errArrayTooLarge
		}
		n, err := dst.Write(p)
		written += int64(n)
		if err == nil && n != len(p) {
			return io.ErrShortWrite
		}
		return err
	}

	if err := write(jsonArrayOpen[:]); err != nil {
		return written, err
	}

	reader := bufio.NewReaderSize(src, 32*1024)
	records := 0
	recordBytes := 0
	for {
		fragment, err := reader.ReadSlice('\n')
		if err == io.EOF && len(fragment) == 0 {
			if recordBytes != 0 || records == 0 {
				return written, io.ErrUnexpectedEOF
			}
			break
		}
		if err != nil && err != bufio.ErrBufferFull {
			if err == io.EOF {
				return written, io.ErrUnexpectedEOF
			}
			return written, jsonArrayReadError{err: err}
		}

		if recordBytes == 0 && records != 0 {
			if err := write(jsonArrayComma[:]); err != nil {
				return written, err
			}
		}

		complete := err == nil
		payload := fragment
		if complete {
			payload = fragment[:len(fragment)-1]
		}
		if len(payload) == 0 && recordBytes == 0 {
			return written, io.ErrUnexpectedEOF
		}
		if err := write(payload); err != nil {
			return written, err
		}
		recordBytes += len(payload)
		if complete {
			records++
			recordBytes = 0
		}
	}

	if err := write(jsonArrayClose[:]); err != nil {
		return written, err
	}
	return written, nil
}

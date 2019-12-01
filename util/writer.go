package util

import (
	"io"
	"bufio"
)

type writeFlusher interface {
	io.Writer
	Flush() error
}

type FlushWriter struct {
	Wf writeFlusher
	Bw *bufio.Writer
}

func (w *FlushWriter) Write(p []byte) (int, error) {
	n, err := w.Wf.Write(p)
	if err != nil {
		return 0, err
	}
	if err = w.Wf.Flush(); err != nil {
		return 0, err
	}
	if err = w.Bw.Flush(); err != nil {
		return 0, err
	}
	return n, nil
}


package util

import (
	"io"
	"sync"
)

var CopyBufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096)
	},
}

func CopyZeroAlloc(w io.Writer, r io.Reader) (int64, error) {
	vbuf := CopyBufPool.Get()
	buf := vbuf.([]byte)
	n, err := io.CopyBuffer(w, r, buf)
	CopyBufPool.Put(vbuf)
	return n, err
}

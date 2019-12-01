package http

import (
	"github.com/gottingen/simo/http/compress"
	"github.com/gottingen/gekko/buffer"
)

func gunzipData(p []byte) ([]byte, error) {
	var bb buffer.Buffer
	_, err := compress.WriteGunzip(&bb, p)
	if err != nil {
		return nil, err
	}
	return bb.B, nil
}


func inflateData(p []byte) ([]byte, error) {
	var bb buffer.Buffer
	_, err := compress.WriteInflate(&bb, p)
	if err != nil {
		return nil, err
	}
	return bb.B, nil
}

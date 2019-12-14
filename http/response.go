package http

import (
	"bufio"
	"fmt"
	"github.com/gottingen/gekko/buffer"
	"github.com/gottingen/simo/compress"
	"github.com/gottingen/simo/http/bytesconv"
	"github.com/gottingen/simo/http/constant"
	"github.com/gottingen/simo/util"
	"io"
	"net"
	"os"
)

var responseBodyPool buffer.Pool

// Response represents HTTP response.
//
// It is forbidden copying Response instances. Create new instances
// and use CopyTo instead.
//
// Response instance MUST NOT be used from concurrently running goroutines.
type Response struct {
	noCopy util.NoCopy //nolint:unused,structcheck

	// Response header
	//
	// Copying Header by value is forbidden. Use pointer to Header instead.
	Header ResponseHeader

	// Flush headers as soon as possible without waiting for first body bytes.
	// Relevant for bodyStream only.
	ImmediateHeaderFlush bool

	bodyStream io.Reader
	w          responseBodyWriter
	body       *buffer.Buffer
	bodyRaw    []byte

	// Response.Read() skips reading body if set to true.
	// Use it for reading HEAD responses.
	//
	// Response.Write() skips writing body if set to true.
	// Use it for writing HEAD responses.
	SkipBody bool

	keepBodyBuffer bool

	// Remote TCPAddr from concurrently net.Conn
	raddr net.Addr
	// Local TCPAddr from concurrently net.Conn
	laddr net.Addr
}

// StatusCode returns response status code.
func (resp *Response) StatusCode() int {
	return resp.Header.StatusCode()
}

// SetStatusCode sets response status code.
func (resp *Response) SetStatusCode(statusCode int) {
	resp.Header.SetStatusCode(statusCode)
}

// ConnectionClose returns true if 'Connection: close' header is set.
func (resp *Response) ConnectionClose() bool {
	return resp.Header.ConnectionClose()
}

// SetConnectionClose sets 'Connection: close' header.
func (resp *Response) SetConnectionClose() {
	resp.Header.SetConnectionClose()
}

// SendFile registers file on the given path to be used as response body
// when Write is called.
//
// Note that SendFile doesn't set Content-Type, so set it yourself
// with Header.SetContentType.
func (resp *Response) SendFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	fileInfo, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	size64 := fileInfo.Size()
	size := int(size64)
	if int64(size) != size64 {
		size = -1
	}

	resp.Header.SetLastModified(fileInfo.ModTime())
	resp.SetBodyStream(f, size)
	return nil
}

// SetBodyStream sets response body stream and, optionally body size.
//
// If bodySize is >= 0, then the bodyStream must provide exactly bodySize bytes
// before returning io.EOF.
//
// If bodySize < 0, then bodyStream is read until io.EOF.
//
// bodyStream.Close() is called after finishing reading all body data
// if it implements io.Closer.
//
// See also SetBodyStreamWriter.
func (resp *Response) SetBodyStream(bodyStream io.Reader, bodySize int) {
	resp.ResetBody()
	resp.bodyStream = bodyStream
	resp.Header.SetContentLength(bodySize)
}

// IsBodyStream returns true if body is set via SetBodyStream*
func (resp *Response) IsBodyStream() bool {
	return resp.bodyStream != nil
}

// SetBodyStreamWriter registers the given sw for populating response body.
//
// This function may be used in the following cases:
//
//     * if response body is too big (more than 10MB).
//     * if response body is streamed from slow external sources.
//     * if response body must be streamed to the client in chunks
//     (aka `http server push` or `chunked transfer-encoding`).
//
// See also SetBodyStream.
func (resp *Response) SetBodyStreamWriter(sw StreamWriter) {
	sr := NewStreamReader(sw)
	resp.SetBodyStream(sr, -1)
}

// BodyWriter returns writer for populating response body.
//
// If used inside RequestHandler, the returned writer must not be used
// after returning from RequestHandler. Use RequestCtx.Write
// or SetBodyStreamWriter in this case.
func (resp *Response) BodyWriter() io.Writer {
	resp.w.r = resp
	return &resp.w
}

type responseBodyWriter struct {
	r *Response
}

func (w *responseBodyWriter) Write(p []byte) (int, error) {
	w.r.AppendBody(p)
	return len(p), nil
}

func (resp *Response) parseNetConn(conn net.Conn) {
	resp.raddr = conn.RemoteAddr()
	resp.laddr = conn.LocalAddr()
}

// RemoteAddr returns the remote network address. The Addr returned is shared
// by all invocations of RemoteAddr, so do not modify it.
func (resp *Response) RemoteAddr() net.Addr {
	return resp.raddr
}

// LocalAddr returns the local network address. The Addr returned is shared
// by all invocations of LocalAddr, so do not modify it.
func (resp *Response) LocalAddr() net.Addr {
	return resp.laddr
}

// Body returns response body.
//
// The returned body is valid until the response modification.
func (resp *Response) Body() []byte {
	if resp.bodyStream != nil {
		bodyBuf := resp.bodyBuffer()
		bodyBuf.Reset()
		_, err := util.CopyZeroAlloc(bodyBuf, resp.bodyStream)
		resp.closeBodyStream() //nolint:errcheck
		if err != nil {
			bodyBuf.SetString(err.Error())
		}
	}
	return resp.bodyBytes()
}

func (resp *Response) bodyBytes() []byte {
	if resp.bodyRaw != nil {
		return resp.bodyRaw
	}
	if resp.body == nil {
		return nil
	}
	return resp.body.B
}

func (resp *Response) bodyBuffer() *buffer.Buffer {
	if resp.body == nil {
		resp.body = responseBodyPool.Get()
	}
	resp.bodyRaw = nil
	return resp.body
}

// BodyGunzip returns un-gzipped body data.
//
// This method may be used if the response header contains
// 'Content-Encoding: gzip' for reading un-gzipped body.
// Use Body for reading gzipped response body.
func (resp *Response) BodyGunzip() ([]byte, error) {
	return compress.GunzipData(resp.Body())
}

// BodyInflate returns inflated body data.
//
// This method may be used if the response header contains
// 'Content-Encoding: deflate' for reading inflated response body.
// Use Body for reading deflated response body.
func (resp *Response) BodyInflate() ([]byte, error) {
	return compress.InflateData(resp.Body())
}

// BodyWriteTo writes response body to w.
func (resp *Response) BodyWriteTo(w io.Writer) error {
	if resp.bodyStream != nil {
		_, err := util.CopyZeroAlloc(w, resp.bodyStream)
		resp.closeBodyStream() //nolint:errcheck
		return err
	}
	_, err := w.Write(resp.bodyBytes())
	return err
}

// AppendBody appends p to response body.
//
// It is safe re-using p after the function returns.
func (resp *Response) AppendBody(p []byte) {
	resp.AppendBodyString(bytesconv.ToString(p))
}

// AppendBodyString appends s to response body.
func (resp *Response) AppendBodyString(s string) {
	resp.closeBodyStream()           //nolint:errcheck
	resp.bodyBuffer().WriteString(s) //nolint:errcheck
}

// SetBody sets response body.
//
// It is safe re-using body argument after the function returns.
func (resp *Response) SetBody(body []byte) {
	resp.SetBodyString(bytesconv.ToString(body))
}

// SetBodyString sets response body.
func (resp *Response) SetBodyString(body string) {
	resp.closeBodyStream() //nolint:errcheck
	bodyBuf := resp.bodyBuffer()
	bodyBuf.Reset()
	bodyBuf.WriteString(body) //nolint:errcheck
}

// ResetBody resets response body.
func (resp *Response) ResetBody() {
	resp.bodyRaw = nil
	resp.closeBodyStream() //nolint:errcheck
	if resp.body != nil {
		if resp.keepBodyBuffer {
			resp.body.Reset()
		} else {
			responseBodyPool.Put(resp.body)
			resp.body = nil
		}
	}
}

// SetBodyRaw sets response body, but without copying it.
//
// From this point onward the body argument must not be changed.
func (resp *Response) SetBodyRaw(body []byte) {
	resp.ResetBody()
	resp.bodyRaw = body
}

// ReleaseBody retires the response body if it is greater than "size" bytes.
//
// This permits GC to reclaim the large buffer.  If used, must be before
// ReleaseResponse.
//
// Use this method only if you really understand how it works.
// The majority of workloads don't need this method.
func (resp *Response) ReleaseBody(size int) {
	resp.bodyRaw = nil
	if cap(resp.body.B) > size {
		resp.closeBodyStream() //nolint:errcheck
		resp.body = nil
	}
}

// SwapBody swaps response body with the given body and returns
// the previous response body.
//
// It is forbidden to use the body passed to SwapBody after
// the function returns.
func (resp *Response) SwapBody(body []byte) []byte {
	bb := resp.bodyBuffer()

	if resp.bodyStream != nil {
		bb.Reset()
		_, err := util.CopyZeroAlloc(bb, resp.bodyStream)
		resp.closeBodyStream() //nolint:errcheck
		if err != nil {
			bb.Reset()
			bb.SetString(err.Error())
		}
	}

	resp.bodyRaw = nil

	oldBody := bb.B
	bb.B = body
	return oldBody
}

// CopyTo copies resp contents to dst except of body stream.
func (resp *Response) CopyTo(dst *Response) {
	resp.copyToSkipBody(dst)
	if resp.bodyRaw != nil {
		dst.bodyRaw = resp.bodyRaw
		if dst.body != nil {
			dst.body.Reset()
		}
	} else if resp.body != nil {
		dst.bodyBuffer().Set(resp.body.B)
	} else if dst.body != nil {
		dst.body.Reset()
	}
}

func (resp *Response) copyToSkipBody(dst *Response) {
	dst.Reset()
	resp.Header.CopyTo(&dst.Header)
	dst.SkipBody = resp.SkipBody
	dst.raddr = resp.raddr
	dst.laddr = resp.laddr
}

func swapResponseBody(a, b *Response) {
	a.body, b.body = b.body, a.body
	a.bodyRaw, b.bodyRaw = b.bodyRaw, a.bodyRaw
	a.bodyStream, b.bodyStream = b.bodyStream, a.bodyStream
}

// Reset clears response contents.
func (resp *Response) Reset() {
	resp.Header.Reset()
	resp.resetSkipHeader()
	resp.SkipBody = false
	resp.raddr = nil
	resp.laddr = nil
	resp.ImmediateHeaderFlush = false
}

func (resp *Response) resetSkipHeader() {
	resp.ResetBody()
}

// Read reads response (including body) from the given r.
//
// io.EOF is returned if r is closed before reading the first header byte.
func (resp *Response) Read(r *bufio.Reader) error {
	return resp.ReadLimitBody(r, 0)
}

// ReadLimitBody reads response from the given r, limiting the body size.
//
// If maxBodySize > 0 and the body size exceeds maxBodySize,
// then ErrBodyTooLarge is returned.
//
// io.EOF is returned if r is closed before reading the first header byte.
func (resp *Response) ReadLimitBody(r *bufio.Reader, maxBodySize int) error {
	resp.resetSkipHeader()
	err := resp.Header.Read(r)
	if err != nil {
		return err
	}
	if resp.Header.StatusCode() == constant.StatusContinue {
		// Read the next response according to http://www.w3.org/Protocols/rfc2616/rfc2616-sec8.html .
		if err = resp.Header.Read(r); err != nil {
			return err
		}
	}

	if !resp.mustSkipBody() {
		bodyBuf := resp.bodyBuffer()
		bodyBuf.Reset()
		bodyBuf.B, err = readBody(r, resp.Header.ContentLength(), maxBodySize, bodyBuf.B)
		if err != nil {
			return err
		}
		resp.Header.SetContentLength(len(bodyBuf.B))
	}
	return nil
}

func (resp *Response) mustSkipBody() bool {
	return resp.SkipBody || resp.Header.mustSkipContentLength()
}

// WriteTo writes response to w. It implements io.WriterTo.
func (resp *Response) WriteTo(w io.Writer) (int64, error) {
	return writeBufio(resp, w)
}

// WriteGzip writes response with gzipped body to w.
//
// The method gzips response body and sets 'Content-Encoding: gzip'
// header before writing response to w.
//
// WriteGzip doesn't flush response to w for performance reasons.
func (resp *Response) WriteGzip(w *bufio.Writer) error {
	return resp.WriteGzipLevel(w, compress.CompressDefaultCompression)
}

// WriteGzipLevel writes response with gzipped body to w.
//
// Level is the desired compression level:
//
//     * CompressNoCompression
//     * CompressBestSpeed
//     * CompressBestCompression
//     * CompressDefaultCompression
//     * CompressHuffmanOnly
//
// The method gzips response body and sets 'Content-Encoding: gzip'
// header before writing response to w.
//
// WriteGzipLevel doesn't flush response to w for performance reasons.
func (resp *Response) WriteGzipLevel(w *bufio.Writer, level int) error {
	if err := resp.gzipBody(level); err != nil {
		return err
	}
	return resp.Write(w)
}

// WriteDeflate writes response with deflated body to w.
//
// The method deflates response body and sets 'Content-Encoding: deflate'
// header before writing response to w.
//
// WriteDeflate doesn't flush response to w for performance reasons.
func (resp *Response) WriteDeflate(w *bufio.Writer) error {
	return resp.WriteDeflateLevel(w, compress.CompressDefaultCompression)
}

// WriteDeflateLevel writes response with deflated body to w.
//
// Level is the desired compression level:
//
//     * CompressNoCompression
//     * CompressBestSpeed
//     * CompressBestCompression
//     * CompressDefaultCompression
//     * CompressHuffmanOnly
//
// The method deflates response body and sets 'Content-Encoding: deflate'
// header before writing response to w.
//
// WriteDeflateLevel doesn't flush response to w for performance reasons.
func (resp *Response) WriteDeflateLevel(w *bufio.Writer, level int) error {
	if err := resp.deflateBody(level); err != nil {
		return err
	}
	return resp.Write(w)
}

func (resp *Response) gzipBody(level int) error {
	if len(resp.Header.peek(constant.StrContentEncoding)) > 0 {
		// It looks like the body is already compressed.
		// Do not compress it again.
		return nil
	}

	if !resp.Header.isCompressibleContentType() {
		// The content-type cannot be compressed.
		return nil
	}

	if resp.bodyStream != nil {
		// Reset Content-Length to -1, since it is impossible
		// to determine body size beforehand of streamed compression.
		resp.Header.SetContentLength(-1)

		// Do not care about memory allocations here, since gzip is slow
		// and allocates a lot of memory by itself.
		bs := resp.bodyStream
		resp.bodyStream = NewStreamReader(func(sw *bufio.Writer) {
			zw := compress.AcquireStacklessGzipWriter(sw, level)
			fw := &util.FlushWriter{
				Wf: zw,
				Bw: sw,
			}
			util.CopyZeroAlloc(fw, bs) //nolint:errcheck
			compress.ReleaseStacklessGzipWriter(zw, level)
			if bsc, ok := bs.(io.Closer); ok {
				bsc.Close()
			}
		})
	} else {
		bodyBytes := resp.bodyBytes()
		if len(bodyBytes) < minCompressLen {
			// There is no sense in spending CPU time on small body compression,
			// since there is a very high probability that the compressed
			// body size will be bigger than the original body size.
			return nil
		}
		w := responseBodyPool.Get()
		w.B = compress.AppendGzipBytesLevel(w.B, bodyBytes, level)

		// Hack: swap resp.body with w.
		if resp.body != nil {
			responseBodyPool.Put(resp.body)
		}
		resp.body = w
		resp.bodyRaw = nil
	}
	resp.Header.SetCanonical(constant.StrContentEncoding, constant.StrGzip)
	return nil
}

func (resp *Response) deflateBody(level int) error {
	if len(resp.Header.peek(constant.StrContentEncoding)) > 0 {
		// It looks like the body is already compressed.
		// Do not compress it again.
		return nil
	}

	if !resp.Header.isCompressibleContentType() {
		// The content-type cannot be compressed.
		return nil
	}

	if resp.bodyStream != nil {
		// Reset Content-Length to -1, since it is impossible
		// to determine body size beforehand of streamed compression.
		resp.Header.SetContentLength(-1)

		// Do not care about memory allocations here, since flate is slow
		// and allocates a lot of memory by itself.
		bs := resp.bodyStream
		resp.bodyStream = NewStreamReader(func(sw *bufio.Writer) {
			zw := compress.AcquireStacklessDeflateWriter(sw, level)
			fw := &util.FlushWriter{
				Wf: zw,
				Bw: sw,
			}
			util.CopyZeroAlloc(fw, bs) //nolint:errcheck
			compress.ReleaseStacklessDeflateWriter(zw, level)
			if bsc, ok := bs.(io.Closer); ok {
				bsc.Close()
			}
		})
	} else {
		bodyBytes := resp.bodyBytes()
		if len(bodyBytes) < minCompressLen {
			// There is no sense in spending CPU time on small body compression,
			// since there is a very high probability that the compressed
			// body size will be bigger than the original body size.
			return nil
		}
		w := responseBodyPool.Get()
		w.B = compress.AppendDeflateBytesLevel(w.B, bodyBytes, level)

		// Hack: swap resp.body with w.
		if resp.body != nil {
			responseBodyPool.Put(resp.body)
		}
		resp.body = w
		resp.bodyRaw = nil
	}
	resp.Header.SetCanonical(constant.StrContentEncoding, constant.StrDeflate)
	return nil
}

// Write writes response to w.
//
// Write doesn't flush response to w for performance reasons.
//
// See also WriteTo.
func (resp *Response) Write(w *bufio.Writer) error {
	sendBody := !resp.mustSkipBody()

	if resp.bodyStream != nil {
		return resp.writeBodyStream(w, sendBody)
	}

	body := resp.bodyBytes()
	bodyLen := len(body)
	if sendBody || bodyLen > 0 {
		resp.Header.SetContentLength(bodyLen)
	}
	if err := resp.Header.Write(w); err != nil {
		return err
	}
	if sendBody {
		if _, err := w.Write(body); err != nil {
			return err
		}
	}
	return nil
}

func (resp *Response) writeBodyStream(w *bufio.Writer, sendBody bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &ErrBodyStreamWritePanic{
				error: fmt.Errorf("panic while writing body stream: %+v", r),
			}
		}
	}()

	contentLength := resp.Header.ContentLength()
	if contentLength < 0 {
		lrSize := limitedReaderSize(resp.bodyStream)
		if lrSize >= 0 {
			contentLength = int(lrSize)
			if int64(contentLength) != lrSize {
				contentLength = -1
			}
			if contentLength >= 0 {
				resp.Header.SetContentLength(contentLength)
			}
		}
	}
	if contentLength >= 0 {
		if err = resp.Header.Write(w); err == nil && sendBody {
			if resp.ImmediateHeaderFlush {
				err = w.Flush()
			}
			if err == nil {
				err = writeBodyFixedSize(w, resp.bodyStream, int64(contentLength))
			}
		}
	} else {
		resp.Header.SetContentLength(-1)
		if err = resp.Header.Write(w); err == nil && sendBody {
			if resp.ImmediateHeaderFlush {
				err = w.Flush()
			}
			if err == nil {
				err = writeBodyChunked(w, resp.bodyStream)
			}
		}
	}
	err1 := resp.closeBodyStream()
	if err == nil {
		err = err1
	}
	return err
}

func (resp *Response) closeBodyStream() error {
	if resp.bodyStream == nil {
		return nil
	}
	var err error
	if bsc, ok := resp.bodyStream.(io.Closer); ok {
		err = bsc.Close()
	}
	resp.bodyStream = nil
	return err
}

// String returns response representation.
//
// Returns error message instead of response representation on error.
//
// Use Write instead of String for performance-critical code.
func (resp *Response) String() string {
	return getHTTPString(resp)
}

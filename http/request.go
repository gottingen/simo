package http

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/gottingen/gekko/buffer"
	"github.com/gottingen/simo/http/bytesconv"
	"github.com/gottingen/simo/http/compress"
	"github.com/gottingen/simo/http/constant"
	"github.com/gottingen/simo/util"
	"io"
	"mime/multipart"
)

var requestBodyPool buffer.Pool

// ErrGetOnly is returned when server expects only GET requests,
// but some other type of request came (Server.GetOnly option is true).
var ErrGetOnly = errors.New("non-GET request received")

// Request represents HTTP request.
//
// It is forbidden copying Request instances. Create new instances
// and use CopyTo instead.
//
// Request instance MUST NOT be used from concurrently running goroutines.
type Request struct {
	noCopy util.NoCopy //nolint:unused,structcheck

	// Request header
	//
	// Copying Header by value is forbidden. Use pointer to Header instead.
	Header RequestHeader

	uri      URI
	postArgs Args

	bodyStream io.Reader
	w          requestBodyWriter
	body       *buffer.Buffer

	multipartForm         *multipart.Form
	multipartFormBoundary string

	// Group bool members in order to reduce Request object size.
	parsedURI      bool
	parsedPostArgs bool

	keepBodyBuffer bool

	isTLS bool

	// To detect scheme changes in redirects
	schemaUpdate bool
}

// ErrNoMultipartForm means that the request's Content-Type
// isn't 'multipart/form-data'.
var ErrNoMultipartForm = errors.New("request has no multipart/form-data Content-Type")

// SetHost sets host for the request.
func (req *Request) SetHost(host string) {
	req.URI().SetHost(host)
}

// SetHostBytes sets host for the request.
func (req *Request) SetHostBytes(host []byte) {
	req.URI().SetHostBytes(host)
}

// Host returns the host for the given request.
func (req *Request) Host() []byte {
	return req.URI().Host()
}

// SetRequestURI sets RequestURI.
func (req *Request) SetRequestURI(requestURI string) {
	req.Header.SetRequestURI(requestURI)
	req.parsedURI = false
}

// SetRequestURIBytes sets RequestURI.
func (req *Request) SetRequestURIBytes(requestURI []byte) {
	req.Header.SetRequestURIBytes(requestURI)
	req.parsedURI = false
}

// RequestURI returns request's URI.
func (req *Request) RequestURI() []byte {
	if req.parsedURI {
		requestURI := req.uri.RequestURI()
		req.SetRequestURIBytes(requestURI)
	}
	return req.Header.RequestURI()
}

// ConnectionClose returns true if 'Connection: close' header is set.
func (req *Request) ConnectionClose() bool {
	return req.Header.ConnectionClose()
}

// SetConnectionClose sets 'Connection: close' header.
func (req *Request) SetConnectionClose() {
	req.Header.SetConnectionClose()
}

func swapRequestBody(a, b *Request) {
	a.body, b.body = b.body, a.body
	a.bodyStream, b.bodyStream = b.bodyStream, a.bodyStream
}

// SetBodyStream sets request body stream and, optionally body size.
//
// If bodySize is >= 0, then the bodyStream must provide exactly bodySize bytes
// before returning io.EOF.
//
// If bodySize < 0, then bodyStream is read until io.EOF.
//
// bodyStream.Close() is called after finishing reading all body data
// if it implements io.Closer.
//
// Note that GET and HEAD requests cannot have body.
//
// See also SetBodyStreamWriter.
func (req *Request) SetBodyStream(bodyStream io.Reader, bodySize int) {
	req.ResetBody()
	req.bodyStream = bodyStream
	req.Header.SetContentLength(bodySize)
}

// IsBodyStream returns true if body is set via SetBodyStream*
func (req *Request) IsBodyStream() bool {
	return req.bodyStream != nil
}

// SetBodyStreamWriter registers the given sw for populating request body.
//
// This function may be used in the following cases:
//
//     * if request body is too big (more than 10MB).
//     * if request body is streamed from slow external sources.
//     * if request body must be streamed to the server in chunks
//     (aka `http client push` or `chunked transfer-encoding`).
//
// Note that GET and HEAD requests cannot have body.
//
/// See also SetBodyStream.
func (req *Request) SetBodyStreamWriter(sw StreamWriter) {
	sr := NewStreamReader(sw)
	req.SetBodyStream(sr, -1)
}

// BodyWriter returns writer for populating request body.
func (req *Request) BodyWriter() io.Writer {
	req.w.r = req
	return &req.w
}

type requestBodyWriter struct {
	r *Request
}

func (w *requestBodyWriter) Write(p []byte) (int, error) {
	w.r.AppendBody(p)
	return len(p), nil
}

func (req *Request) bodyBytes() []byte {
	if req.body == nil {
		return nil
	}
	return req.body.B
}

func (req *Request) bodyBuffer() *buffer.Buffer {
	if req.body == nil {
		req.body = requestBodyPool.Get()
	}
	return req.body
}

// BodyGunzip returns un-gzipped body data.
//
// This method may be used if the request header contains
// 'Content-Encoding: gzip' for reading un-gzipped body.
// Use Body for reading gzipped request body.
func (req *Request) BodyGunzip() ([]byte, error) {
	return gunzipData(req.Body())
}

// BodyInflate returns inflated body data.
//
// This method may be used if the response header contains
// 'Content-Encoding: deflate' for reading inflated request body.
// Use Body for reading deflated request body.
func (req *Request) BodyInflate() ([]byte, error) {
	return inflateData(req.Body())
}

// BodyWriteTo writes request body to w.
func (req *Request) BodyWriteTo(w io.Writer) error {
	if req.bodyStream != nil {
		_, err := util.CopyZeroAlloc(w, req.bodyStream)
		req.closeBodyStream() //nolint:errcheck
		return err
	}
	if req.onlyMultipartForm() {
		return WriteMultipartForm(w, req.multipartForm, req.multipartFormBoundary)
	}
	_, err := w.Write(req.bodyBytes())
	return err
}

// ReleaseBody retires the request body if it is greater than "size" bytes.
//
// This permits GC to reclaim the large buffer.  If used, must be before
// ReleaseRequest.
//
// Use this method only if you really understand how it works.
// The majority of workloads don't need this method.
func (req *Request) ReleaseBody(size int) {
	if cap(req.body.B) > size {
		req.closeBodyStream() //nolint:errcheck
		req.body = nil
	}
}

// SwapBody swaps request body with the given body and returns
// the previous request body.
//
// It is forbidden to use the body passed to SwapBody after
// the function returns.
func (req *Request) SwapBody(body []byte) []byte {
	bb := req.bodyBuffer()

	if req.bodyStream != nil {
		bb.Reset()
		_, err := util.CopyZeroAlloc(bb, req.bodyStream)
		req.closeBodyStream() //nolint:errcheck
		if err != nil {
			bb.Reset()
			bb.SetString(err.Error())
		}
	}

	oldBody := bb.B
	bb.B = body
	return oldBody
}

// Body returns request body.
//
// The returned body is valid until the request modification.
func (req *Request) Body() []byte {
	if req.bodyStream != nil {
		bodyBuf := req.bodyBuffer()
		bodyBuf.Reset()
		_, err := util.CopyZeroAlloc(bodyBuf, req.bodyStream)
		req.closeBodyStream() //nolint:errcheck
		if err != nil {
			bodyBuf.SetString(err.Error())
		}
	} else if req.onlyMultipartForm() {
		body, err := marshalMultipartForm(req.multipartForm, req.multipartFormBoundary)
		if err != nil {
			return []byte(err.Error())
		}
		return body
	}
	return req.bodyBytes()
}

// AppendBody appends p to request body.
//
// It is safe re-using p after the function returns.
func (req *Request) AppendBody(p []byte) {
	req.AppendBodyString(bytesconv.ToString(p))
}

// AppendBodyString appends s to request body.
func (req *Request) AppendBodyString(s string) {
	req.RemoveMultipartFormFiles()
	req.closeBodyStream()           //nolint:errcheck
	req.bodyBuffer().WriteString(s) //nolint:errcheck
}

// SetBody sets request body.
//
// It is safe re-using body argument after the function returns.
func (req *Request) SetBody(body []byte) {
	req.SetBodyString(bytesconv.ToString(body))
}

// SetBodyString sets request body.
func (req *Request) SetBodyString(body string) {
	req.RemoveMultipartFormFiles()
	req.closeBodyStream() //nolint:errcheck
	req.bodyBuffer().SetString(body)
}

// ResetBody resets request body.
func (req *Request) ResetBody() {
	req.RemoveMultipartFormFiles()
	req.closeBodyStream() //nolint:errcheck
	if req.body != nil {
		if req.keepBodyBuffer {
			req.body.Reset()
		} else {
			requestBodyPool.Put(req.body)
			req.body = nil
		}
	}
}

// CopyTo copies req contents to dst except of body stream.
func (req *Request) CopyTo(dst *Request) {
	req.copyToSkipBody(dst)
	if req.body != nil {
		dst.bodyBuffer().Set(req.body.B)
	} else if dst.body != nil {
		dst.body.Reset()
	}
}

func (req *Request) copyToSkipBody(dst *Request) {
	dst.Reset()
	req.Header.CopyTo(&dst.Header)

	req.uri.CopyTo(&dst.uri)
	dst.parsedURI = req.parsedURI

	req.postArgs.CopyTo(&dst.postArgs)
	dst.parsedPostArgs = req.parsedPostArgs
	dst.isTLS = req.isTLS

	// do not copy multipartForm - it will be automatically
	// re-created on the first call to MultipartForm.
}

// URI returns request URI
func (req *Request) URI() *URI {
	req.parseURI()
	return &req.uri
}

func (req *Request) parseURI() {
	if req.parsedURI {
		return
	}
	req.parsedURI = true

	req.uri.parse(req.Header.Host(), req.Header.RequestURI(), req.isTLS)
}

// PostArgs returns POST arguments.
func (req *Request) PostArgs() *Args {
	req.parsePostArgs()
	return &req.postArgs
}

func (req *Request) parsePostArgs() {
	if req.parsedPostArgs {
		return
	}
	req.parsedPostArgs = true

	if !bytes.HasPrefix(req.Header.ContentType(), constant.StrPostArgsContentType) {
		return
	}
	req.postArgs.ParseBytes(req.bodyBytes())
}

// MultipartForm returns requests's multipart form.
//
// Returns ErrNoMultipartForm if request's Content-Type
// isn't 'multipart/form-data'.
//
// RemoveMultipartFormFiles must be called after returned multipart form
// is processed.
func (req *Request) MultipartForm() (*multipart.Form, error) {
	if req.multipartForm != nil {
		return req.multipartForm, nil
	}

	req.multipartFormBoundary = string(req.Header.MultipartFormBoundary())
	if len(req.multipartFormBoundary) == 0 {
		return nil, ErrNoMultipartForm
	}

	ce := req.Header.peek(constant.StrContentEncoding)
	body := req.bodyBytes()
	if bytes.Equal(ce, constant.StrGzip) {
		// Do not care about memory usage here.
		var err error
		if body, err = compress.AppendGunzipBytes(nil, body); err != nil {
			return nil, fmt.Errorf("cannot gunzip request body: %s", err)
		}
	} else if len(ce) > 0 {
		return nil, fmt.Errorf("unsupported Content-Encoding: %q", ce)
	}

	f, err := readMultipartForm(bytes.NewReader(body), req.multipartFormBoundary, len(body), len(body))
	if err != nil {
		return nil, err
	}
	req.multipartForm = f
	return f, nil
}

// Reset clears request contents.
func (req *Request) Reset() {
	req.Header.Reset()
	req.resetSkipHeader()
}

func (req *Request) resetSkipHeader() {
	req.ResetBody()
	req.uri.Reset()
	req.parsedURI = false
	req.postArgs.Reset()
	req.parsedPostArgs = false
	req.isTLS = false
}

// RemoveMultipartFormFiles removes multipart/form-data temporary files
// associated with the request.
func (req *Request) RemoveMultipartFormFiles() {
	if req.multipartForm != nil {
		// Do not check for error, since these files may be deleted or moved
		// to new places by user code.
		req.multipartForm.RemoveAll() //nolint:errcheck
		req.multipartForm = nil
	}
	req.multipartFormBoundary = ""
}

// Read reads request (including body) from the given r.
//
// RemoveMultipartFormFiles or Reset must be called after
// reading multipart/form-data request in order to delete temporarily
// uploaded files.
//
// If MayContinue returns true, the caller must:
//
//     - Either send StatusExpectationFailed response if request headers don't
//       satisfy the caller.
//     - Or send StatusContinue response before reading request body
//       with ContinueReadBody.
//     - Or close the connection.
//
// io.EOF is returned if r is closed before reading the first header byte.
func (req *Request) Read(r *bufio.Reader) error {
	return req.ReadLimitBody(r, 0)
}

// ReadLimitBody reads request from the given r, limiting the body size.
//
// If maxBodySize > 0 and the body size exceeds maxBodySize,
// then ErrBodyTooLarge is returned.
//
// RemoveMultipartFormFiles or Reset must be called after
// reading multipart/form-data request in order to delete temporarily
// uploaded files.
//
// If MayContinue returns true, the caller must:
//
//     - Either send StatusExpectationFailed response if request headers don't
//       satisfy the caller.
//     - Or send StatusContinue response before reading request body
//       with ContinueReadBody.
//     - Or close the connection.
//
// io.EOF is returned if r is closed before reading the first header byte.
func (req *Request) ReadLimitBody(r *bufio.Reader, maxBodySize int) error {
	req.resetSkipHeader()
	if err := req.Header.Read(r); err != nil {
		return err
	}

	return req.readLimitBody(r, maxBodySize, false)
}

func (req *Request) readLimitBody(r *bufio.Reader, maxBodySize int, getOnly bool) error {
	// Do not reset the request here - the caller must reset it before
	// calling this method.

	if getOnly && !req.Header.IsGet() {
		return ErrGetOnly
	}

	if req.MayContinue() {
		// 'Expect: 100-continue' header found. Let the caller deciding
		// whether to read request body or
		// to return StatusExpectationFailed.
		return nil
	}

	return req.ContinueReadBody(r, maxBodySize)
}

// MayContinue returns true if the request contains
// 'Expect: 100-continue' header.
//
// The caller must do one of the following actions if MayContinue returns true:
//
//     - Either send StatusExpectationFailed response if request headers don't
//       satisfy the caller.
//     - Or send StatusContinue response before reading request body
//       with ContinueReadBody.
//     - Or close the connection.
func (req *Request) MayContinue() bool {
	return bytes.Equal(req.Header.peek(constant.StrExpect), constant.Str100Continue)
}

// ContinueReadBody reads request body if request header contains
// 'Expect: 100-continue'.
//
// The caller must send StatusContinue response before calling this method.
//
// If maxBodySize > 0 and the body size exceeds maxBodySize,
// then ErrBodyTooLarge is returned.
func (req *Request) ContinueReadBody(r *bufio.Reader, maxBodySize int) error {
	var err error
	contentLength := req.Header.realContentLength()
	if contentLength > 0 {
		if maxBodySize > 0 && contentLength > maxBodySize {
			return ErrBodyTooLarge
		}

		// Pre-read multipart form data of known length.
		// This way we limit memory usage for large file uploads, since their contents
		// is streamed into temporary files if file size exceeds defaultMaxInMemoryFileSize.
		req.multipartFormBoundary = string(req.Header.MultipartFormBoundary())
		if len(req.multipartFormBoundary) > 0 && len(req.Header.peek(constant.StrContentEncoding)) == 0 {
			req.multipartForm, err = readMultipartForm(r, req.multipartFormBoundary, contentLength, defaultMaxInMemoryFileSize)
			if err != nil {
				req.Reset()
			}
			return err
		}
	}

	if contentLength == -2 {
		// identity body has no sense for http requests, since
		// the end of body is determined by connection close.
		// So just ignore request body for requests without
		// 'Content-Length' and 'Transfer-Encoding' headers.
		req.Header.SetContentLength(0)
		return nil
	}

	bodyBuf := req.bodyBuffer()
	bodyBuf.Reset()
	bodyBuf.B, err = readBody(r, contentLength, maxBodySize, bodyBuf.B)
	if err != nil {
		req.Reset()
		return err
	}
	req.Header.SetContentLength(len(bodyBuf.B))
	return nil
}

var errRequestHostRequired = errors.New("missing required Host header in request")

// WriteTo writes request to w. It implements io.WriterTo.
func (req *Request) WriteTo(w io.Writer) (int64, error) {
	return writeBufio(req, w)
}

func (req *Request) onlyMultipartForm() bool {
	return req.multipartForm != nil && (req.body == nil || len(req.body.B) == 0)
}

// Write writes request to w.
//
// Write doesn't flush request to w for performance reasons.
//
// See also WriteTo.
func (req *Request) Write(w *bufio.Writer) error {
	if len(req.Header.Host()) == 0 || req.parsedURI {
		uri := req.URI()
		host := uri.Host()
		if len(host) == 0 {
			return errRequestHostRequired
		}
		req.Header.SetHostBytes(host)
		req.Header.SetRequestURIBytes(uri.RequestURI())

		if len(uri.username) > 0 {
			// RequestHeader.SetBytesKV only uses RequestHeader.bufKV.key
			// So we are free to use RequestHeader.bufKV.value as a scratch pad for
			// the base64 encoding.
			nl := len(uri.username) + len(uri.password) + 1
			nb := nl + len(constant.StrBasicSpace)
			tl := nb + base64.StdEncoding.EncodedLen(nl)
			if tl > cap(req.Header.bufKV.value) {
				req.Header.bufKV.value = make([]byte, 0, tl)
			}
			buf := req.Header.bufKV.value[:0]
			buf = append(buf, uri.username...)
			buf = append(buf, constant.StrColon...)
			buf = append(buf, uri.password...)
			buf = append(buf, constant.StrBasicSpace...)
			base64.StdEncoding.Encode(buf[nb:tl], buf[:nl])
			req.Header.SetBytesKV(constant.StrAuthorization, buf[nl:tl])
		}
	}

	if req.bodyStream != nil {
		return req.writeBodyStream(w)
	}

	body := req.bodyBytes()
	var err error
	if req.onlyMultipartForm() {
		body, err = marshalMultipartForm(req.multipartForm, req.multipartFormBoundary)
		if err != nil {
			return fmt.Errorf("error when marshaling multipart form: %s", err)
		}
		req.Header.SetMultipartFormBoundary(req.multipartFormBoundary)
	}

	hasBody := !req.Header.ignoreBody()
	if hasBody {
		if len(body) == 0 {
			body = req.postArgs.QueryString()
		}
		req.Header.SetContentLength(len(body))
	}
	if err = req.Header.Write(w); err != nil {
		return err
	}
	if hasBody {
		_, err = w.Write(body)
	} else if len(body) > 0 {
		return fmt.Errorf("non-zero body for non-POST request. body=%q", body)
	}
	return err
}

func (req *Request) writeBodyStream(w *bufio.Writer) error {
	var err error

	contentLength := req.Header.ContentLength()
	if contentLength < 0 {
		lrSize := limitedReaderSize(req.bodyStream)
		if lrSize >= 0 {
			contentLength = int(lrSize)
			if int64(contentLength) != lrSize {
				contentLength = -1
			}
			if contentLength >= 0 {
				req.Header.SetContentLength(contentLength)
			}
		}
	}
	if contentLength >= 0 {
		if err = req.Header.Write(w); err == nil {
			err = writeBodyFixedSize(w, req.bodyStream, int64(contentLength))
		}
	} else {
		req.Header.SetContentLength(-1)
		if err = req.Header.Write(w); err == nil {
			err = writeBodyChunked(w, req.bodyStream)
		}
	}
	err1 := req.closeBodyStream()
	if err == nil {
		err = err1
	}
	return err
}

func (req *Request) closeBodyStream() error {
	if req.bodyStream == nil {
		return nil
	}
	var err error
	if bsc, ok := req.bodyStream.(io.Closer); ok {
		err = bsc.Close()
	}
	req.bodyStream = nil
	return err
}

// String returns request representation.
//
// Returns error message instead of request representation on error.
//
// Use Write instead of String for performance-critical code.
func (req *Request) String() string {
	return getHTTPString(req)
}

func marshalMultipartForm(f *multipart.Form, boundary string) ([]byte, error) {
	var buf buffer.Buffer
	if err := WriteMultipartForm(&buf, f, boundary); err != nil {
		return nil, err
	}
	return buf.B, nil
}

// WriteMultipartForm writes the given multipart form f with the given
// boundary to w.
func WriteMultipartForm(w io.Writer, f *multipart.Form, boundary string) error {
	// Do not care about memory allocations here, since multipart
	// form processing is slow.
	if len(boundary) == 0 {
		panic("BUG: form boundary cannot be empty")
	}

	mw := multipart.NewWriter(w)
	if err := mw.SetBoundary(boundary); err != nil {
		return fmt.Errorf("cannot use form boundary %q: %s", boundary, err)
	}

	// marshal values
	for k, vv := range f.Value {
		for _, v := range vv {
			if err := mw.WriteField(k, v); err != nil {
				return fmt.Errorf("cannot write form field %q value %q: %s", k, v, err)
			}
		}
	}

	// marshal files
	for k, fvv := range f.File {
		for _, fv := range fvv {
			vw, err := mw.CreatePart(fv.Header)
			if err != nil {
				return fmt.Errorf("cannot create form file %q (%q): %s", k, fv.Filename, err)
			}
			fh, err := fv.Open()
			if err != nil {
				return fmt.Errorf("cannot open form file %q (%q): %s", k, fv.Filename, err)
			}
			if _, err = util.CopyZeroAlloc(vw, fh); err != nil {
				return fmt.Errorf("error when copying form file %q (%q): %s", k, fv.Filename, err)
			}
			if err = fh.Close(); err != nil {
				return fmt.Errorf("cannot close form file %q (%q): %s", k, fv.Filename, err)
			}
		}
	}

	if err := mw.Close(); err != nil {
		return fmt.Errorf("error when closing multipart form writer: %s", err)
	}

	return nil
}

func readMultipartForm(r io.Reader, boundary string, size, maxInMemoryFileSize int) (*multipart.Form, error) {
	// Do not care about memory allocations here, since they are tiny
	// compared to multipart data (aka multi-MB files) usually sent
	// in multipart/form-data requests.

	if size <= 0 {
		return nil, fmt.Errorf("form size must be greater than 0. Given %d", size)
	}
	lr := io.LimitReader(r, int64(size))
	mr := multipart.NewReader(lr, boundary)
	f, err := mr.ReadForm(int64(maxInMemoryFileSize))
	if err != nil {
		return nil, fmt.Errorf("cannot read multipart/form-data body: %s", err)
	}
	return f, nil
}

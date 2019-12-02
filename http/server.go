package http

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/gottingen/simo/http/constant"
	"github.com/gottingen/simo/util"
	"io"
	"mime/multipart"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var errNoCertOrKeyProvided = errors.New("cert or key has not provided")

var (
	// ErrAlreadyServing is returned when calling Serve on a Server
	// that is already serving connections.
	ErrAlreadyServing = errors.New("Server is already serving connections")
)

// ServeConn serves HTTP requests from the given connection
// using the given handler.
//
// ServeConn returns nil if all requests from the c are successfully served.
// It returns non-nil error otherwise.
//
// Connection c must immediately propagate all the data passed to Write()
// to the client. Otherwise requests' processing may hang.
//
// ServeConn closes c before returning.
func ServeConn(c net.Conn, handler RequestHandler) error {
	v := serverPool.Get()
	if v == nil {
		v = &Server{}
	}
	s := v.(*Server)
	s.Handler = handler
	err := s.ServeConn(c)
	s.Handler = nil
	serverPool.Put(v)
	return err
}

var serverPool sync.Pool

// Serve serves incoming connections from the given listener
// using the given handler.
//
// Serve blocks until the given listener returns permanent error.
func Serve(ln net.Listener, handler RequestHandler) error {
	s := &Server{
		Handler: handler,
	}
	return s.Serve(ln)
}

// ServeTLS serves HTTPS requests from the given net.Listener
// using the given handler.
//
// certFile and keyFile are paths to TLS certificate and key files.
func ServeTLS(ln net.Listener, certFile, keyFile string, handler RequestHandler) error {
	s := &Server{
		Handler: handler,
	}
	return s.ServeTLS(ln, certFile, keyFile)
}

// ServeTLSEmbed serves HTTPS requests from the given net.Listener
// using the given handler.
//
// certData and keyData must contain valid TLS certificate and key data.
func ServeTLSEmbed(ln net.Listener, certData, keyData []byte, handler RequestHandler) error {
	s := &Server{
		Handler: handler,
	}
	return s.ServeTLSEmbed(ln, certData, keyData)
}

// ListenAndServe serves HTTP requests from the given TCP addr
// using the given handler.
func ListenAndServe(addr string, handler RequestHandler) error {
	s := &Server{
		Handler: handler,
	}
	return s.ListenAndServe(addr)
}

// ListenAndServeUNIX serves HTTP requests from the given UNIX addr
// using the given handler.
//
// The function deletes existing file at addr before starting serving.
//
// The server sets the given file mode for the UNIX addr.
func ListenAndServeUNIX(addr string, mode os.FileMode, handler RequestHandler) error {
	s := &Server{
		Handler: handler,
	}
	return s.ListenAndServeUNIX(addr, mode)
}

// ListenAndServeTLS serves HTTPS requests from the given TCP addr
// using the given handler.
//
// certFile and keyFile are paths to TLS certificate and key files.
func ListenAndServeTLS(addr, certFile, keyFile string, handler RequestHandler) error {
	s := &Server{
		Handler: handler,
	}
	return s.ListenAndServeTLS(addr, certFile, keyFile)
}

// ListenAndServeTLSEmbed serves HTTPS requests from the given TCP addr
// using the given handler.
//
// certData and keyData must contain valid TLS certificate and key data.
func ListenAndServeTLSEmbed(addr string, certData, keyData []byte, handler RequestHandler) error {
	s := &Server{
		Handler: handler,
	}
	return s.ListenAndServeTLSEmbed(addr, certData, keyData)
}

// RequestHandler must process incoming requests.
//
// RequestHandler must call ctx.TimeoutError() before returning
// if it keeps references to ctx and/or its' members after the return.
// Consider wrapping RequestHandler into TimeoutHandler if response time
// must be limited.
type RequestHandler func(ctx *RequestCtx)

// ServeHandler must process tls.Config.NextProto negotiated requests.
type ServeHandler func(c net.Conn) error

// Server implements HTTP server.
//
// Default Server settings should satisfy the majority of Server users.
// Adjust Server settings only if you really understand the consequences.
//
// It is forbidden copying Server instances. Create new Server instances
// instead.
//
// It is safe to call Server methods from concurrently running goroutines.
type Server struct {
	noCopy util.NoCopy //nolint:unused,structcheck

	// Handler for processing incoming requests.
	//
	// Take into account that no `panic` recovery is done by `http` (thus any `panic` will take down the entire server).
	// Instead the user should use `recover` to handle these situations.
	Handler RequestHandler

	// ErrorHandler for returning a response in case of an error while receiving or parsing the request.
	//
	// The following is a non-exhaustive list of errors that can be expected as argument:
	//   * io.EOF
	//   * io.ErrUnexpectedEOF
	//   * ErrGetOnly
	//   * ErrSmallBuffer
	//   * ErrBodyTooLarge
	//   * ErrBrokenChunks
	ErrorHandler func(ctx *RequestCtx, err error)

	// HeaderReceived is called after receiving the header
	//
	// non zero RequestConfig field values will overwrite the default configs
	HeaderReceived func(header *RequestHeader) RequestConfig

	// Server name for sending in response headers.
	//
	// Default server name is used if left blank.
	Name string

	// The maximum number of concurrent connections the server may serve.
	//
	// DefaultConcurrency is used if not set.
	Concurrency int

	// Whether to disable keep-alive connections.
	//
	// The server will close all the incoming connections after sending
	// the first response to client if this option is set to true.
	//
	// By default keep-alive connections are enabled.
	DisableKeepalive bool

	// Per-connection buffer size for requests' reading.
	// This also limits the maximum header size.
	//
	// Increase this buffer if your clients send multi-KB RequestURIs
	// and/or multi-KB headers (for example, BIG cookies).
	//
	// Default buffer size is used if not set.
	ReadBufferSize int

	// Per-connection buffer size for responses' writing.
	//
	// Default buffer size is used if not set.
	WriteBufferSize int

	// ReadTimeout is the amount of time allowed to read
	// the full request including body. The connection's read
	// deadline is reset when the connection opens, or for
	// keep-alive connections after the first byte has been read.
	//
	// By default request read timeout is unlimited.
	ReadTimeout time.Duration

	// WriteTimeout is the maximum duration before timing out
	// writes of the response. It is reset after the request handler
	// has returned.
	//
	// By default response write timeout is unlimited.
	WriteTimeout time.Duration

	// IdleTimeout is the maximum amount of time to wait for the
	// next request when keep-alive is enabled. If IdleTimeout
	// is zero, the value of ReadTimeout is used.
	IdleTimeout time.Duration

	// Maximum number of concurrent client connections allowed per IP.
	//
	// By default unlimited number of concurrent connections
	// may be established to the server from a single IP address.
	MaxConnsPerIP int

	// Maximum number of requests served per connection.
	//
	// The server closes connection after the last request.
	// 'Connection: close' header is added to the last response.
	//
	// By default unlimited number of requests may be served per connection.
	MaxRequestsPerConn int

	// MaxKeepaliveDuration is a no-op and only left here for backwards compatibility.
	// Deprecated: Use IdleTimeout instead.
	MaxKeepaliveDuration time.Duration

	// Whether to enable tcp keep-alive connections.
	//
	// Whether the operating system should send tcp keep-alive messages on the tcp connection.
	//
	// By default tcp keep-alive connections are disabled.
	TCPKeepalive bool

	// Period between tcp keep-alive messages.
	//
	// TCP keep-alive period is determined by operation system by default.
	TCPKeepalivePeriod time.Duration

	// Maximum request body size.
	//
	// The server rejects requests with bodies exceeding this limit.
	//
	// Request body size is limited by DefaultMaxRequestBodySize by default.
	MaxRequestBodySize int

	// Aggressively reduces memory usage at the cost of higher CPU usage
	// if set to true.
	//
	// Try enabling this option only if the server consumes too much memory
	// serving mostly idle keep-alive connections. This may reduce memory
	// usage by more than 50%.
	//
	// Aggressive memory usage reduction is disabled by default.
	ReduceMemoryUsage bool

	// Rejects all non-GET requests if set to true.
	//
	// This option is useful as anti-DoS protection for servers
	// accepting only GET requests. The request size is limited
	// by ReadBufferSize if GetOnly is set.
	//
	// Server accepts all the requests by default.
	GetOnly bool

	// Logs all errors, including the most frequent
	// 'connection reset by peer', 'broken pipe' and 'connection timeout'
	// errors. Such errors are common in production serving real-world
	// clients.
	//
	// By default the most frequent errors such as
	// 'connection reset by peer', 'broken pipe' and 'connection timeout'
	// are suppressed in order to limit output log traffic.
	LogAllErrors bool

	// Header names are passed as-is without normalization
	// if this option is set.
	//
	// Disabled header names' normalization may be useful only for proxying
	// incoming requests to other servers expecting case-sensitive
	// for details.
	//
	// By default request and response header names are normalized, i.e.
	// The first letter and the first letters following dashes
	// are uppercased, while all the other letters are lowercased.
	// Examples:
	//
	//     * HOST -> Host
	//     * content-type -> Content-Type
	//     * cONTENT-lenGTH -> Content-Length
	DisableHeaderNamesNormalizing bool

	// SleepWhenConcurrencyLimitsExceeded is a duration to be slept of if
	// the concurrency limit in exceeded (default [when is 0]: don't sleep
	// and accept new connections immidiatelly).
	SleepWhenConcurrencyLimitsExceeded time.Duration

	// NoDefaultServerHeader, when set to true, causes the default Server header
	// to be excluded from the Response.
	//
	// The default Server header value is the value of the Name field or an
	// internal default value in its absence. With this option set to true,
	// the only time a Server header will be sent is if a non-zero length
	// value is explicitly provided during a request.
	NoDefaultServerHeader bool

	// NoDefaultContentType, when set to true, causes the default Content-Type
	// header to be excluded from the Response.
	//
	// The default Content-Type header value is the internal default value. When
	// set to true, the Content-Type will not be present.
	NoDefaultContentType bool

	// ConnState specifies an optional callback function that is
	// called when a client connection changes state. See the
	// ConnState type and associated constants for details.
	ConnState func(net.Conn, ConnState)

	// Logger, which is used by RequestCtx.Logger().
	//
	// By default standard logger from log package is used.
	Logger Logger

	// KeepHijackedConns is an opt-in disable of connection
	// close by http after connections' HijackHandler returns.
	// This allows to save goroutines, e.g. when http used to upgrade
	// http connections to WS and connection goes to another handler,
	// which will close it when needed.
	KeepHijackedConns bool

	tlsConfig  *tls.Config
	nextProtos map[string]ServeHandler

	concurrency      uint32
	concurrencyCh    chan struct{}
	perIPConnCounter perIPConnCounter
	serverName       atomic.Value

	ctxPool        sync.Pool
	readerPool     sync.Pool
	writerPool     sync.Pool
	hijackConnPool sync.Pool

	// We need to know our listener so we can close it in Shutdown().
	ln net.Listener

	mu   sync.Mutex
	open int32
	stop int32
	done chan struct{}
}

// TimeoutHandler creates RequestHandler, which returns StatusRequestTimeout
// error with the given msg to the client if h didn't return during
// the given duration.
//
// The returned handler may return StatusTooManyRequests error with the given
// msg to the client if there are more than Server.Concurrency concurrent
// handlers h are running at the moment.
func TimeoutHandler(h RequestHandler, timeout time.Duration, msg string) RequestHandler {
	return TimeoutWithCodeHandler(h, timeout, msg, constant.StatusRequestTimeout)
}

//RequestConfig configure the per request deadline and body limits
type RequestConfig struct {
	// ReadTimeout is the maximum duration for reading the entire
	// request body.
	// a zero value means that default values will be honored
	ReadTimeout time.Duration
	// WriteTimeout is the maximum duration before timing out
	// writes of the response.
	// a zero value means that default values will be honored
	WriteTimeout time.Duration
	// Maximum request body size.
	// a zero value means that default values will be honored
	MaxRequestBodySize int
}

type connTLSer interface {
	Handshake() error
	ConnectionState() tls.ConnectionState
}


type firstByteReader struct {
	c        net.Conn
	ch       byte
	byteRead bool
}

func (r *firstByteReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	nn := 0
	if !r.byteRead {
		b[0] = r.ch
		b = b[1:]
		r.byteRead = true
		nn = 1
	}
	n, err := r.c.Read(b)
	return n + nn, err
}

var ctxLoggerLock sync.Mutex

var zeroTCPAddr = &net.TCPAddr{
	IP: net.IPv4zero,
}


// ErrMissingFile may be returned from FormFile when the is no uploaded file
// associated with the given multipart form key.
var ErrMissingFile = errors.New("there is no uploaded file associated with the given key")

// SaveMultipartFile saves multipart file fh under the given filename path.
func SaveMultipartFile(fh *multipart.FileHeader, path string) error {
	f, err := fh.Open()
	if err != nil {
		return err
	}

	if ff, ok := f.(*os.File); ok {
		// Windows can't rename files that are opened.
		if err := f.Close(); err != nil {
			return err
		}

		// If renaming fails we try the normal copying method.
		// Renaming could fail if the files are on different devices.
		if os.Rename(ff.Name(), path) == nil {
			return nil
		}

		// Reopen f for the code below.
		f, err = fh.Open()
		if err != nil {
			return err
		}
	}

	defer f.Close()

	ff, err := os.Create(path)
	if err != nil {
		return err
	}
	defer ff.Close()
	_, err = util.CopyZeroAlloc(ff, f)
	return err
}


func addrToIP(addr net.Addr) net.IP {
	x, ok := addr.(*net.TCPAddr)
	if !ok {
		return net.IPv4zero
	}
	return x.IP
}

func getRedirectStatusCode(statusCode int) int {
	if statusCode == constant.StatusMovedPermanently || statusCode == constant.StatusFound ||
		statusCode == constant.StatusSeeOther || statusCode == constant.StatusTemporaryRedirect ||
		statusCode == constant.StatusPermanentRedirect {
		return statusCode
	}
	return constant.StatusFound
}

// NextProto adds nph to be processed when key is negotiated when TLS
// connection is established.
//
// This function can only be called before the server is started.
func (s *Server) NextProto(key string, nph ServeHandler) {
	if s.nextProtos == nil {
		s.nextProtos = make(map[string]ServeHandler)
	}
	s.configTLS()
	s.tlsConfig.NextProtos = append(s.tlsConfig.NextProtos, key)
	s.nextProtos[key] = nph
}

func (s *Server) getNextProto(c net.Conn) (proto string, err error) {
	if tlsConn, ok := c.(connTLSer); ok {
		err = tlsConn.Handshake()
		if err == nil {
			proto = tlsConn.ConnectionState().NegotiatedProtocol
		}
	}
	return
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe, ListenAndServeTLS and
// ListenAndServeTLSEmbed so dead TCP connections (e.g. closing laptop mid-download)
// eventually go away.
type tcpKeepaliveListener struct {
	*net.TCPListener
	keepalivePeriod time.Duration
}

func (ln tcpKeepaliveListener) Accept() (net.Conn, error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return nil, err
	}
	if err := tc.SetKeepAlive(true); err != nil {
		tc.Close() //nolint:errcheck
		return nil, err
	}
	if ln.keepalivePeriod > 0 {
		if err := tc.SetKeepAlivePeriod(ln.keepalivePeriod); err != nil {
			tc.Close() //nolint:errcheck
			return nil, err
		}
	}
	return tc, nil
}

// ListenAndServe serves HTTP requests from the given TCP4 addr.
//
// Pass custom listener to Serve if you need listening on non-TCP4 media
// such as IPv6.
//
// Accepted connections are configured to enable TCP keep-alives.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		return err
	}
	if s.TCPKeepalive {
		if tcpln, ok := ln.(*net.TCPListener); ok {
			return s.Serve(tcpKeepaliveListener{
				TCPListener:     tcpln,
				keepalivePeriod: s.TCPKeepalivePeriod,
			})
		}
	}
	return s.Serve(ln)
}

// ListenAndServeUNIX serves HTTP requests from the given UNIX addr.
//
// The function deletes existing file at addr before starting serving.
//
// The server sets the given file mode for the UNIX addr.
func (s *Server) ListenAndServeUNIX(addr string, mode os.FileMode) error {
	if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("unexpected error when trying to remove unix socket file %q: %s", addr, err)
	}
	ln, err := net.Listen("unix", addr)
	if err != nil {
		return err
	}
	if err = os.Chmod(addr, mode); err != nil {
		return fmt.Errorf("cannot chmod %#o for %q: %s", mode, addr, err)
	}
	return s.Serve(ln)
}

// ListenAndServeTLS serves HTTPS requests from the given TCP4 addr.
//
// certFile and keyFile are paths to TLS certificate and key files.
//
// Pass custom listener to Serve if you need listening on non-TCP4 media
// such as IPv6.
//
// If the certFile or keyFile has not been provided to the server structure,
// the function will use the previously added TLS configuration.
//
// Accepted connections are configured to enable TCP keep-alives.
func (s *Server) ListenAndServeTLS(addr, certFile, keyFile string) error {
	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		return err
	}
	if s.TCPKeepalive {
		if tcpln, ok := ln.(*net.TCPListener); ok {
			return s.ServeTLS(tcpKeepaliveListener{
				TCPListener:     tcpln,
				keepalivePeriod: s.TCPKeepalivePeriod,
			}, certFile, keyFile)
		}
	}
	return s.ServeTLS(ln, certFile, keyFile)
}

// ListenAndServeTLSEmbed serves HTTPS requests from the given TCP4 addr.
//
// certData and keyData must contain valid TLS certificate and key data.
//
// Pass custom listener to Serve if you need listening on arbitrary media
// such as IPv6.
//
// If the certFile or keyFile has not been provided the server structure,
// the function will use previously added TLS configuration.
//
// Accepted connections are configured to enable TCP keep-alives.
func (s *Server) ListenAndServeTLSEmbed(addr string, certData, keyData []byte) error {
	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		return err
	}
	if s.TCPKeepalive {
		if tcpln, ok := ln.(*net.TCPListener); ok {
			return s.ServeTLSEmbed(tcpKeepaliveListener{
				TCPListener:     tcpln,
				keepalivePeriod: s.TCPKeepalivePeriod,
			}, certData, keyData)
		}
	}
	return s.ServeTLSEmbed(ln, certData, keyData)
}

// ServeTLS serves HTTPS requests from the given listener.
//
// certFile and keyFile are paths to TLS certificate and key files.
//
// If the certFile or keyFile has not been provided the server structure,
// the function will use previously added TLS configuration.
func (s *Server) ServeTLS(ln net.Listener, certFile, keyFile string) error {
	err := s.AppendCert(certFile, keyFile)
	if err != nil && err != errNoCertOrKeyProvided {
		return err
	}
	if s.tlsConfig == nil {
		return errNoCertOrKeyProvided
	}
	s.tlsConfig.BuildNameToCertificate()

	return s.Serve(
		tls.NewListener(ln, s.tlsConfig),
	)
}

// ServeTLSEmbed serves HTTPS requests from the given listener.
//
// certData and keyData must contain valid TLS certificate and key data.
//
// If the certFile or keyFile has not been provided the server structure,
// the function will use previously added TLS configuration.
func (s *Server) ServeTLSEmbed(ln net.Listener, certData, keyData []byte) error {
	err := s.AppendCertEmbed(certData, keyData)
	if err != nil && err != errNoCertOrKeyProvided {
		return err
	}
	if s.tlsConfig == nil {
		return errNoCertOrKeyProvided
	}
	s.tlsConfig.BuildNameToCertificate()

	return s.Serve(
		tls.NewListener(ln, s.tlsConfig),
	)
}

// AppendCert appends certificate and keyfile to TLS Configuration.
//
// This function allows programmer to handle multiple domains
// in one server structure. See examples/multidomain
func (s *Server) AppendCert(certFile, keyFile string) error {
	if len(certFile) == 0 && len(keyFile) == 0 {
		return errNoCertOrKeyProvided
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("cannot load TLS key pair from certFile=%q and keyFile=%q: %s", certFile, keyFile, err)
	}

	s.configTLS()

	s.tlsConfig.Certificates = append(s.tlsConfig.Certificates, cert)
	return nil
}

// AppendCertEmbed does the same as AppendCert but using in-memory data.
func (s *Server) AppendCertEmbed(certData, keyData []byte) error {
	if len(certData) == 0 && len(keyData) == 0 {
		return errNoCertOrKeyProvided
	}

	cert, err := tls.X509KeyPair(certData, keyData)
	if err != nil {
		return fmt.Errorf("cannot load TLS key pair from the provided certData(%d) and keyData(%d): %s",
			len(certData), len(keyData), err)
	}

	s.configTLS()

	s.tlsConfig.Certificates = append(s.tlsConfig.Certificates, cert)
	return nil
}

func (s *Server) configTLS() {
	if s.tlsConfig == nil {
		s.tlsConfig = &tls.Config{
			PreferServerCipherSuites: true,
		}
	}
}

// DefaultConcurrency is the maximum number of concurrent connections
// the Server may serve by default (i.e. if Server.Concurrency isn't set).
const DefaultConcurrency = 256 * 1024

// Serve serves incoming connections from the given listener.
//
// Serve blocks until the given listener returns permanent error.
func (s *Server) Serve(ln net.Listener) error {
	var lastOverflowErrorTime time.Time
	var lastPerIPErrorTime time.Time
	var c net.Conn
	var err error

	s.mu.Lock()
	{
		if s.ln != nil {
			s.mu.Unlock()
			return ErrAlreadyServing
		}

		s.ln = ln
		s.done = make(chan struct{})
	}
	s.mu.Unlock()

	maxWorkersCount := s.getConcurrency()
	s.concurrencyCh = make(chan struct{}, maxWorkersCount)
	wp := &workerPool{
		WorkerFunc:      s.serveConn,
		MaxWorkersCount: maxWorkersCount,
		LogAllErrors:    s.LogAllErrors,
		Logger:          s.logger(),
		connState:       s.setState,
	}
	wp.Start()

	// Count our waiting to accept a connection as an open connection.
	// This way we can't get into any weird state where just after accepting
	// a connection Shutdown is called which reads open as 0 because it isn't
	// incremented yet.
	atomic.AddInt32(&s.open, 1)
	defer atomic.AddInt32(&s.open, -1)

	for {
		if c, err = acceptConn(s, ln, &lastPerIPErrorTime); err != nil {
			wp.Stop()
			if err == io.EOF {
				return nil
			}
			return err
		}
		s.setState(c, StateNew)
		atomic.AddInt32(&s.open, 1)
		if !wp.Serve(c) {
			atomic.AddInt32(&s.open, -1)
			s.writeFastError(c, constant.StatusServiceUnavailable,
				"The connection cannot be served because Server.Concurrency limit exceeded")
			c.Close()
			s.setState(c, StateClosed)
			if time.Since(lastOverflowErrorTime) > time.Minute {
				s.logger().Printf("The incoming connection cannot be served, because %d concurrent connections are served. "+
					"Try increasing Server.Concurrency", maxWorkersCount)
				lastOverflowErrorTime = time.Now()
			}

			// The current server reached concurrency limit,
			// so give other concurrently running servers a chance
			// accepting incoming connections on the same address.
			//
			// There is a hope other servers didn't reach their
			// concurrency limits yet :)
			//
			if s.SleepWhenConcurrencyLimitsExceeded > 0 {
				time.Sleep(s.SleepWhenConcurrencyLimitsExceeded)
			}
		}
		c = nil
	}
}

// Shutdown gracefully shuts down the server without interrupting any active connections.
// Shutdown works by first closing all open listeners and then waiting indefinitely for all connections to return to idle and then shut down.
//
// When Shutdown is called, Serve, ListenAndServe, and ListenAndServeTLS immediately return nil.
// Make sure the program doesn't exit and waits instead for Shutdown to return.
//
// Shutdown does not close keepalive connections so its recommended to set ReadTimeout to something else than 0.
func (s *Server) Shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	atomic.StoreInt32(&s.stop, 1)
	defer atomic.StoreInt32(&s.stop, 0)

	if s.ln == nil {
		return nil
	}

	if err := s.ln.Close(); err != nil {
		return err
	}

	if s.done != nil {
		close(s.done)
	}

	// Closing the listener will make Serve() call Stop on the worker pool.
	// Setting .stop to 1 will make serveConn() break out of its loop.
	// Now we just have to wait until all workers are done.
	for {
		if open := atomic.LoadInt32(&s.open); open == 0 {
			break
		}
		// This is not an optimal solution but using a sync.WaitGroup
		// here causes data races as it's hard to prevent Add() to be called
		// while Wait() is waiting.
		time.Sleep(time.Millisecond * 100)
	}

	s.ln = nil
	return nil
}

func acceptConn(s *Server, ln net.Listener, lastPerIPErrorTime *time.Time) (net.Conn, error) {
	for {
		c, err := ln.Accept()
		if err != nil {
			if c != nil {
				panic("BUG: net.Listener returned non-nil conn and non-nil error")
			}
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				s.logger().Printf("Temporary error when accepting new connections: %s", netErr)
				time.Sleep(time.Second)
				continue
			}
			if err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
				s.logger().Printf("Permanent error when accepting new connections: %s", err)
				return nil, err
			}
			return nil, io.EOF
		}
		if c == nil {
			panic("BUG: net.Listener returned (nil, nil)")
		}
		if s.MaxConnsPerIP > 0 {
			pic := wrapPerIPConn(s, c)
			if pic == nil {
				if time.Since(*lastPerIPErrorTime) > time.Minute {
					s.logger().Printf("The number of connections from %s exceeds MaxConnsPerIP=%d",
						getConnIP4(c), s.MaxConnsPerIP)
					*lastPerIPErrorTime = time.Now()
				}
				continue
			}
			c = pic
		}
		return c, nil
	}
}

func wrapPerIPConn(s *Server, c net.Conn) net.Conn {
	ip := getUint32IP(c)
	if ip == 0 {
		return c
	}
	n := s.perIPConnCounter.Register(ip)
	if n > s.MaxConnsPerIP {
		s.perIPConnCounter.Unregister(ip)
		s.writeFastError(c, constant.StatusTooManyRequests, "The number of connections from your ip exceeds MaxConnsPerIP")
		c.Close()
		return nil
	}
	return acquirePerIPConn(c, ip, &s.perIPConnCounter)
}

func (s *Server) logger() Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return defaultLogger
}

var (
	// ErrPerIPConnLimit may be returned from ServeConn if the number of connections
	// per ip exceeds Server.MaxConnsPerIP.
	ErrPerIPConnLimit = errors.New("too many connections per ip")

	// ErrConcurrencyLimit may be returned from ServeConn if the number
	// of concurrently served connections exceeds Server.Concurrency.
	ErrConcurrencyLimit = errors.New("cannot serve the connection because Server.Concurrency concurrent connections are served")
)

// ServeConn serves HTTP requests from the given connection.
//
// ServeConn returns nil if all requests from the c are successfully served.
// It returns non-nil error otherwise.
//
// Connection c must immediately propagate all the data passed to Write()
// to the client. Otherwise requests' processing may hang.
//
// ServeConn closes c before returning.
func (s *Server) ServeConn(c net.Conn) error {
	if s.MaxConnsPerIP > 0 {
		pic := wrapPerIPConn(s, c)
		if pic == nil {
			return ErrPerIPConnLimit
		}
		c = pic
	}

	n := atomic.AddUint32(&s.concurrency, 1)
	if n > uint32(s.getConcurrency()) {
		atomic.AddUint32(&s.concurrency, ^uint32(0))
		s.writeFastError(c, constant.StatusServiceUnavailable, "The connection cannot be served because Server.Concurrency limit exceeded")
		c.Close()
		return ErrConcurrencyLimit
	}

	atomic.AddInt32(&s.open, 1)

	err := s.serveConn(c)

	atomic.AddUint32(&s.concurrency, ^uint32(0))

	if err != errHijacked {
		err1 := c.Close()
		s.setState(c, StateClosed)
		if err == nil {
			err = err1
		}
	} else {
		err = nil
		s.setState(c, StateHijacked)
	}
	return err
}

var errHijacked = errors.New("connection has been hijacked")

// GetCurrentConcurrency returns a number of currently served
// connections.
//
// This function is intended be used by monitoring systems
func (s *Server) GetCurrentConcurrency() uint32 {
	return atomic.LoadUint32(&s.concurrency)
}

// GetOpenConnectionsCount returns a number of opened connections.
//
// This function is intended be used by monitoring systems
func (s *Server) GetOpenConnectionsCount() int32 {
	return atomic.LoadInt32(&s.open) - 1
}

func (s *Server) getConcurrency() int {
	n := s.Concurrency
	if n <= 0 {
		n = DefaultConcurrency
	}
	return n
}

var globalConnID uint64

func nextConnID() uint64 {
	return atomic.AddUint64(&globalConnID, 1)
}

// DefaultMaxRequestBodySize is the maximum request body size the server
// reads by default.
//
// See Server.MaxRequestBodySize for details.
const DefaultMaxRequestBodySize = 4 * 1024 * 1024

func (s *Server) idleTimeout() time.Duration {
	if s.IdleTimeout != 0 {
		return s.IdleTimeout
	}
	return s.ReadTimeout
}

func (s *Server) serveConn(c net.Conn) error {
	defer atomic.AddInt32(&s.open, -1)

	if proto, err := s.getNextProto(c); err != nil {
		return err
	} else {
		handler, ok := s.nextProtos[proto]
		if ok {
			return handler(c)
		}
	}

	var serverName []byte
	if !s.NoDefaultServerHeader {
		serverName = s.getServerName()
	}
	connRequestNum := uint64(0)
	connID := nextConnID()
	connTime := time.Now()
	maxRequestBodySize := s.MaxRequestBodySize
	if maxRequestBodySize <= 0 {
		maxRequestBodySize = DefaultMaxRequestBodySize
	}
	writeTimeout := s.WriteTimeout

	ctx := s.acquireCtx(c)
	ctx.connTime = connTime
	isTLS := ctx.IsTLS()
	var (
		br *bufio.Reader
		bw *bufio.Writer

		err             error
		timeoutResponse *Response
		hijackHandler   HijackHandler

		connectionClose bool
		isHTTP11        bool

		reqReset bool
	)
	for {
		connRequestNum++

		// If this is a keep-alive connection set the idle timeout.
		if connRequestNum > 1 {
			if d := s.idleTimeout(); d > 0 {
				if err := c.SetReadDeadline(time.Now().Add(d)); err != nil {
					panic(fmt.Sprintf("BUG: error in SetReadDeadline(%s): %s", d, err))
				}
			}
		}

		if !s.ReduceMemoryUsage || br != nil {
			if br == nil {
				br = acquireReader(ctx)
			}

			// If this is a keep-alive connection we want to try and read the first bytes
			// within the idle time.
			if connRequestNum > 1 {
				var b []byte
				b, err = br.Peek(4)
				if len(b) == 0 {
					// If reading from a keep-alive connection returns nothing it means
					// the connection was closed (either timeout or from the other side).
					if err != io.EOF {
						err = errNothingRead{err}
					}
				}
			}
		} else {
			// If this is a keep-alive connection acquireByteReader will try to peek
			// a couple of bytes already so the idle timeout will already be used.
			br, err = acquireByteReader(&ctx)
		}

		ctx.Request.isTLS = isTLS
		ctx.Response.Header.noDefaultContentType = s.NoDefaultContentType

		if err == nil {
			if s.ReadTimeout > 0 {
				if err := c.SetReadDeadline(time.Now().Add(s.ReadTimeout)); err != nil {
					panic(fmt.Sprintf("BUG: error in SetReadDeadline(%s): %s", s.ReadTimeout, err))
				}
			}
			if s.DisableHeaderNamesNormalizing {
				ctx.Request.Header.DisableNormalizing()
				ctx.Response.Header.DisableNormalizing()
			}
			// reading Headers
			if err = ctx.Request.Header.Read(br); err == nil {
				if onHdrRecv := s.HeaderReceived; onHdrRecv != nil {
					reqConf := onHdrRecv(&ctx.Request.Header)
					if reqConf.ReadTimeout > 0 {
						deadline := time.Now().Add(reqConf.ReadTimeout)
						if err := c.SetReadDeadline(deadline); err != nil {
							panic(fmt.Sprintf("BUG: error in SetReadDeadline(%s): %s", deadline, err))
						}
					}
					if reqConf.MaxRequestBodySize > 0 {
						maxRequestBodySize = reqConf.MaxRequestBodySize
					}
					if reqConf.WriteTimeout > 0 {
						writeTimeout = reqConf.WriteTimeout
					}
				}
				//read body
				err = ctx.Request.readLimitBody(br, maxRequestBodySize, s.GetOnly)
			}
			if err == nil {
				// If we read any bytes off the wire, we're active.
				s.setState(c, StateActive)
			}

			if (s.ReduceMemoryUsage && br.Buffered() == 0) || err != nil {
				releaseReader(s, br)
				br = nil
			}
		}

		if err != nil {
			if err == io.EOF {
				err = nil
			} else if nr, ok := err.(errNothingRead); ok {
				if connRequestNum > 1 {
					// This is not the first request and we haven't read a single byte
					// of a new request yet. This means it's just a keep-alive connection
					// closing down either because the remote closed it or because
					// or a read timeout on our side. Either way just close the connection
					// and don't return any error response.
					err = nil
				} else {
					err = nr.error
				}
			}

			if err != nil {
				bw = s.writeErrorResponse(bw, ctx, serverName, err)
			}
			break
		}

		// 'Expect: 100-continue' request handling.
		// See http://www.w3.org/Protocols/rfc2616/rfc2616-sec8.html for details.
		if !ctx.Request.Header.ignoreBody() && ctx.Request.MayContinue() {
			// Send 'HTTP/1.1 100 Continue' response.
			if bw == nil {
				bw = acquireWriter(ctx)
			}
			_, err = bw.Write(constant.StrResponseContinue)
			if err != nil {
				break
			}
			err = bw.Flush()
			if err != nil {
				break
			}
			if s.ReduceMemoryUsage {
				releaseWriter(s, bw)
				bw = nil
			}

			// Read request body.
			if br == nil {
				br = acquireReader(ctx)
			}
			err = ctx.Request.ContinueReadBody(br, maxRequestBodySize)
			if (s.ReduceMemoryUsage && br.Buffered() == 0) || err != nil {
				releaseReader(s, br)
				br = nil
			}
			if err != nil {
				bw = s.writeErrorResponse(bw, ctx, serverName, err)
				break
			}
		}

		connectionClose = s.DisableKeepalive || ctx.Request.Header.ConnectionClose()
		isHTTP11 = ctx.Request.Header.IsHTTP11()

		if serverName != nil {
			ctx.Response.Header.SetServerBytes(serverName)
		}
		ctx.connID = connID
		ctx.connRequestNum = connRequestNum
		ctx.time = time.Now()
		s.Handler(ctx)

		timeoutResponse = ctx.timeoutResponse
		if timeoutResponse != nil {
			ctx = s.acquireCtx(c)
			timeoutResponse.CopyTo(&ctx.Response)
			if br != nil {
				// Close connection, since br may be attached to the old ctx via ctx.fbr.
				ctx.SetConnectionClose()
			}
		}

		if !ctx.IsGet() && ctx.IsHead() {
			ctx.Response.SkipBody = true
		}
		reqReset = true
		ctx.Request.Reset()

		hijackHandler = ctx.hijackHandler
		ctx.hijackHandler = nil

		ctx.userValues.Reset()

		if s.MaxRequestsPerConn > 0 && connRequestNum >= uint64(s.MaxRequestsPerConn) {
			ctx.SetConnectionClose()
		}

		if writeTimeout > 0 {
			if err := c.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
				panic(fmt.Sprintf("BUG: error in SetWriteDeadline(%s): %s", s.WriteTimeout, err))
			}
		}

		connectionClose = connectionClose || ctx.Response.ConnectionClose()
		if connectionClose {
			ctx.Response.Header.SetCanonical(constant.StrConnection, constant.StrClose)
		} else if !isHTTP11 {
			// Set 'Connection: keep-alive' response header for non-HTTP/1.1 request.
			// There is no need in setting this header for http/1.1, since in http/1.1
			// connections are keep-alive by default.
			ctx.Response.Header.SetCanonical(constant.StrConnection, constant.StrKeepAlive)
		}

		if serverName != nil && len(ctx.Response.Header.Server()) == 0 {
			ctx.Response.Header.SetServerBytes(serverName)
		}

		if bw == nil {
			bw = acquireWriter(ctx)
		}
		if err = writeResponse(ctx, bw); err != nil {
			break
		}

		// Only flush the writer if we don't have another request in the pipeline.
		// This is a big of an ugly optimization for https://www.techempower.com/benchmarks/
		// This benchmark will send 16 pipelined requests. It is faster to pack as many responses
		// in a TCP packet and send it back at once than waiting for a flush every request.
		// In real world circumstances this behaviour could be argued as being wrong.
		if br == nil || br.Buffered() == 0 || connectionClose {
			err = bw.Flush()
			if err != nil {
				break
			}
		}
		if connectionClose {
			break
		}
		if s.ReduceMemoryUsage {
			releaseWriter(s, bw)
			bw = nil
		}

		if hijackHandler != nil {
			var hjr io.Reader = c
			if br != nil {
				hjr = br
				br = nil

				// br may point to ctx.fbr, so do not return ctx into pool below.
				ctx = nil
			}
			if bw != nil {
				err = bw.Flush()
				if err != nil {
					break
				}
				releaseWriter(s, bw)
				bw = nil
			}
			err = c.SetReadDeadline(zeroTime)
			if err != nil {
				break
			}
			err = c.SetWriteDeadline(zeroTime)
			if err != nil {
				break
			}
			go hijackConnHandler(hjr, c, s, hijackHandler)
			err = errHijacked
			break
		}

		s.setState(c, StateIdle)

		if atomic.LoadInt32(&s.stop) == 1 {
			err = nil
			break
		}
	}

	if br != nil {
		releaseReader(s, br)
	}
	if bw != nil {
		releaseWriter(s, bw)
	}
	if ctx != nil {
		// in unexpected cases the for loop will break
		// before request reset call. in such cases, call it before
		// release to fix #548
		if !reqReset {
			ctx.Request.Reset()
		}
		s.releaseCtx(ctx)
	}
	return err
}

func (s *Server) setState(nc net.Conn, state ConnState) {
	if hook := s.ConnState; hook != nil {
		hook(nc, state)
	}
}

func hijackConnHandler(r io.Reader, c net.Conn, s *Server, h HijackHandler) {
	hjc := s.acquireHijackConn(r, c)
	h(hjc)

	if br, ok := r.(*bufio.Reader); ok {
		releaseReader(s, br)
	}
	if !s.KeepHijackedConns {
		c.Close()
		s.releaseHijackConn(hjc)
	}
}

func (s *Server) acquireHijackConn(r io.Reader, c net.Conn) *hijackConn {
	v := s.hijackConnPool.Get()
	if v == nil {
		hjc := &hijackConn{
			Conn: c,
			r:    r,
			s:    s,
		}
		return hjc
	}
	hjc := v.(*hijackConn)
	hjc.Conn = c
	hjc.r = r
	return hjc
}

func (s *Server) releaseHijackConn(hjc *hijackConn) {
	hjc.Conn = nil
	hjc.r = nil
	s.hijackConnPool.Put(hjc)
}

type hijackConn struct {
	net.Conn
	r io.Reader
	s *Server
}

func (c *hijackConn) UnsafeConn() net.Conn {
	return c.Conn
}

func (c *hijackConn) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func (c *hijackConn) Close() error {
	if !c.s.KeepHijackedConns {
		// when we do not keep hijacked connections,
		// it is closed in hijackConnHandler.
		return nil
	}

	conn := c.Conn
	c.s.releaseHijackConn(c)
	return conn.Close()
}

func releaseReader(s *Server, r *bufio.Reader) {
	s.readerPool.Put(r)
}

func releaseWriter(s *Server, w *bufio.Writer) {
	s.writerPool.Put(w)
}

var fakeServer = &Server{
	// Initialize concurrencyCh for TimeoutHandler
	concurrencyCh: make(chan struct{}, DefaultConcurrency),
}

type fakeAddrer struct {
	net.Conn
	laddr net.Addr
	raddr net.Addr
}

func (fa *fakeAddrer) RemoteAddr() net.Addr {
	return fa.raddr
}

func (fa *fakeAddrer) LocalAddr() net.Addr {
	return fa.laddr
}

func (fa *fakeAddrer) Read(p []byte) (int, error) {
	panic("BUG: unexpected Read call")
}

func (fa *fakeAddrer) Write(p []byte) (int, error) {
	panic("BUG: unexpected Write call")
}

func (fa *fakeAddrer) Close() error {
	panic("BUG: unexpected Close call")
}


func (s *Server) getServerName() []byte {
	v := s.serverName.Load()
	var serverName []byte
	if v == nil {
		serverName = []byte(s.Name)
		if len(serverName) == 0 {
			serverName = constant.DefaultServerName
		}
		s.serverName.Store(serverName)
	} else {
		serverName = v.([]byte)
	}
	return serverName
}

func (s *Server) writeFastError(w io.Writer, statusCode int, msg string) {
	w.Write(constant.StatusLine(statusCode)) //nolint:errcheck

	server := ""
	if !s.NoDefaultServerHeader {
		server = fmt.Sprintf("Server: %s\r\n", s.getServerName())
	}

	serverDateOnce.Do(updateServerDate)

	fmt.Fprintf(w, "Connection: close\r\n"+
		server+
		"Date: %s\r\n"+
		"Content-Type: text/plain\r\n"+
		"Content-Length: %d\r\n"+
		"\r\n"+
		"%s",
		serverDate.Load(), len(msg), msg)
}


func (s *Server) writeErrorResponse(bw *bufio.Writer, ctx *RequestCtx, serverName []byte, err error) *bufio.Writer {
	errorHandler := defaultErrorHandler
	if s.ErrorHandler != nil {
		errorHandler = s.ErrorHandler
	}

	errorHandler(ctx, err)

	if serverName != nil {
		ctx.Response.Header.SetServerBytes(serverName)
	}
	ctx.SetConnectionClose()
	if bw == nil {
		bw = acquireWriter(ctx)
	}
	writeResponse(ctx, bw) //nolint:errcheck
	bw.Flush()
	return bw
}

// A ConnState represents the state of a client connection to a server.
// It's used by the optional Server.ConnState hook.
type ConnState int

const (
	// StateNew represents a new connection that is expected to
	// send a request immediately. Connections begin at this
	// state and then transition to either StateActive or
	// StateClosed.
	StateNew ConnState = iota

	// StateActive represents a connection that has read 1 or more
	// bytes of a request. The Server.ConnState hook for
	// StateActive fires before the request has entered a handler
	// and doesn't fire again until the request has been
	// handled. After the request is handled, the state
	// transitions to StateClosed, StateHijacked, or StateIdle.
	// For HTTP/2, StateActive fires on the transition from zero
	// to one active request, and only transitions away once all
	// active requests are complete. That means that ConnState
	// cannot be used to do per-request work; ConnState only notes
	// the overall state of the connection.
	StateActive

	// StateIdle represents a connection that has finished
	// handling a request and is in the keep-alive state, waiting
	// for a new request. Connections transition from StateIdle
	// to either StateActive or StateClosed.
	StateIdle

	// StateHijacked represents a hijacked connection.
	// This is a terminal state. It does not transition to StateClosed.
	StateHijacked

	// StateClosed represents a closed connection.
	// This is a terminal state. Hijacked connections do not
	// transition to StateClosed.
	StateClosed
)

var stateName = map[ConnState]string{
	StateNew:      "new",
	StateActive:   "active",
	StateIdle:     "idle",
	StateHijacked: "hijacked",
	StateClosed:   "closed",
}

func (c ConnState) String() string {
	return stateName[c]
}


func (s *Server) releaseCtx(ctx *RequestCtx) {
	if ctx.timeoutResponse != nil {
		panic("BUG: cannot release timed out RequestCtx")
	}
	ctx.c = nil
	ctx.fbr.c = nil
	s.ctxPool.Put(ctx)
}


package listener

import (
	"errors"
	"net"
	"sync"
)

var ErrMemListenerClosed = errors.New("MemListener is already closed: use of closed network connection")

// MemListener provides in-memory dialer<->net.Listener implementation.
//
// It may be used either for fast in-process client<->server communications
// without network stack overhead or for client<->server tests.
type MemListener struct {
	lock   sync.Mutex
	closed bool
	conns  chan acceptConn
}

type acceptConn struct {
	conn     net.Conn
	accepted chan struct{}
}

// NewMemListener returns new in-memory dialer<->net.Listener.
func NewMemListener() *MemListener {
	return &MemListener{
		conns: make(chan acceptConn, 1024),
	}
}

// Accept implements net.Listener's Accept.
//
// It is safe calling Accept from concurrently running goroutines.
//
// Accept returns new connection per each Dial call.
func (ln *MemListener) Accept() (net.Conn, error) {
	c, ok := <-ln.conns
	if !ok {
		return nil, ErrMemListenerClosed
	}
	close(c.accepted)
	return c.conn, nil
}

// Close implements net.Listener's Close.
func (ln *MemListener) Close() error {
	var err error

	ln.lock.Lock()
	if !ln.closed {
		close(ln.conns)
		ln.closed = true
	} else {
		err = ErrMemListenerClosed
	}
	ln.lock.Unlock()
	return err
}

// Addr implements net.Listener's Addr.
func (ln *MemListener) Addr() net.Addr {
	return &net.UnixAddr{
		Name: "MemListener",
		Net:  "memory",
	}
}

// Dial creates new client<->server connection.
// Just like a real Dial it only returns once the server
// has accepted the connection.
//
// It is safe calling Dial from concurrently running goroutines.
func (ln *MemListener) Dial() (net.Conn, error) {
	pc := NewPipeConns()
	cConn := pc.Conn1()
	sConn := pc.Conn2()
	ln.lock.Lock()
	accepted := make(chan struct{})
	if !ln.closed {
		ln.conns <- acceptConn{sConn, accepted}
		// Wait until the connection has been accepted.
		<-accepted
	} else {
		sConn.Close()
		cConn.Close()
		cConn = nil
	}
	ln.lock.Unlock()

	if cConn == nil {
		return nil, ErrMemListenerClosed
	}
	return cConn, nil
}

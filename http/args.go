package http

import (
	"bytes"
	"errors"
	"io"
	"sort"
	"sync"

	"github.com/gottingen/gekko/buffer"
	"github.com/gottingen/simo/http/bytesconv"
	"github.com/gottingen/simo/util"
)

const (
	argsNoValue  = true
	argsHasValue = false
)

// AcquireArgs returns an empty Args object from the pool.
//
// The returned Args may be returned to the pool with ReleaseArgs
// when no longer needed. This allows reducing GC load.
func AcquireArgs() *Args {
	return argsPool.Get().(*Args)
}

// ReleaseArgs returns the object acquired via AcquireArgs to the pool.
//
// Do not access the released Args object, otherwise data races may occur.
func ReleaseArgs(a *Args) {
	a.Reset()
	argsPool.Put(a)
}

var argsPool = &sync.Pool{
	New: func() interface{} {
		return &Args{}
	},
}

// Args represents query arguments.
//
// It is forbidden copying Args instances. Create new instances instead
// and use CopyTo().
//
// Args instance MUST NOT be used from concurrently running goroutines.
type Args struct {
	noCopy util.NoCopy //nolint:unused,structcheck

	args []argsKV
	buf  []byte
}

type argsKV struct {
	key     []byte
	value   []byte
	noValue bool
}

// Reset clears query args.
func (a *Args) Reset() {
	a.args = a.args[:0]
}

// CopyTo copies all args to dst.
func (a *Args) CopyTo(dst *Args) {
	dst.Reset()
	dst.args = copyArgs(dst.args, a.args)
}

// VisitAll calls f for each existing arg.
//
// f must not retain references to key and value after returning.
// Make key and/or value copies if you need storing them after returning.
func (a *Args) VisitAll(f func(key, value []byte)) {
	visitArgs(a.args, f)
}

// Len returns the number of query args.
func (a *Args) Len() int {
	return len(a.args)
}

// Parse parses the given string containing query args.
func (a *Args) Parse(s string) {
	a.buf = append(a.buf[:0], s...)
	a.ParseBytes(a.buf)
}

// ParseBytes parses the given b containing query args.
func (a *Args) ParseBytes(b []byte) {
	a.Reset()

	var s argsScanner
	s.b = b

	var kv *argsKV
	a.args, kv = allocArg(a.args)
	for s.next(kv) {
		if len(kv.key) > 0 || len(kv.value) > 0 {
			a.args, kv = allocArg(a.args)
		}
	}
	a.args = releaseArg(a.args)
}

// String returns string representation of query args.
func (a *Args) String() string {
	return string(a.QueryString())
}

// QueryString returns query string for the args.
//
// The returned value is valid until the next call to Args methods.
func (a *Args) QueryString() []byte {
	a.buf = a.AppendBytes(a.buf[:0])
	return a.buf
}

// Sort sorts Args by key and then value using 'f' as comparison function.
//
// For example args.Sort(bytes.Compare)
func (a *Args) Sort(f func(x, y []byte) int) {
	sort.SliceStable(a.args, func(i, j int) bool {
		n := f(a.args[i].key, a.args[j].key)
		if n == 0 {
			return f(a.args[i].value, a.args[j].value) == -1
		}
		return n == -1
	})
}

// AppendBytes appends query string to dst and returns the extended dst.
func (a *Args) AppendBytes(dst []byte) []byte {
	for i, n := 0, len(a.args); i < n; i++ {
		kv := &a.args[i]
		dst = bytesconv.AppendQuotedArg(dst, kv.key)
		if !kv.noValue {
			dst = append(dst, '=')
			if len(kv.value) > 0 {
				dst = bytesconv.AppendQuotedArg(dst, kv.value)
			}
		}
		if i+1 < n {
			dst = append(dst, '&')
		}
	}
	return dst
}

// WriteTo writes query string to w.
//
// WriteTo implements io.WriterTo interface.
func (a *Args) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(a.QueryString())
	return int64(n), err
}

// Del deletes argument with the given key from query args.
func (a *Args) Del(key string) {
	a.args = delAllArgs(a.args, key)
}

// DelBytes deletes argument with the given key from query args.
func (a *Args) DelBytes(key []byte) {
	a.args = delAllArgs(a.args, bytesconv.ToString(key))
}

// Add adds 'key=value' argument.
//
// Multiple values for the same key may be added.
func (a *Args) Add(key, value string) {
	a.args = appendArg(a.args, key, value, argsHasValue)
}

// AddBytesK adds 'key=value' argument.
//
// Multiple values for the same key may be added.
func (a *Args) AddBytesK(key []byte, value string) {
	a.args = appendArg(a.args, bytesconv.ToString(key), value, argsHasValue)
}

// AddBytesV adds 'key=value' argument.
//
// Multiple values for the same key may be added.
func (a *Args) AddBytesV(key string, value []byte) {
	a.args = appendArg(a.args, key, bytesconv.ToString(value), argsHasValue)
}

// AddBytesKV adds 'key=value' argument.
//
// Multiple values for the same key may be added.
func (a *Args) AddBytesKV(key, value []byte) {
	a.args = appendArg(a.args, bytesconv.ToString(key), bytesconv.ToString(value), argsHasValue)
}

// AddNoValue adds only 'key' as argument without the '='.
//
// Multiple values for the same key may be added.
func (a *Args) AddNoValue(key string) {
	a.args = appendArg(a.args, key, "", argsNoValue)
}

// AddBytesKNoValue adds only 'key' as argument without the '='.
//
// Multiple values for the same key may be added.
func (a *Args) AddBytesKNoValue(key []byte) {
	a.args = appendArg(a.args, bytesconv.ToString(key), "", argsNoValue)
}

// Set sets 'key=value' argument.
func (a *Args) Set(key, value string) {
	a.args = setArg(a.args, key, value, argsHasValue)
}

// SetBytesK sets 'key=value' argument.
func (a *Args) SetBytesK(key []byte, value string) {
	a.args = setArg(a.args, bytesconv.ToString(key), value, argsHasValue)
}

// SetBytesV sets 'key=value' argument.
func (a *Args) SetBytesV(key string, value []byte) {
	a.args = setArg(a.args, key, bytesconv.ToString(value), argsHasValue)
}

// SetBytesKV sets 'key=value' argument.
func (a *Args) SetBytesKV(key, value []byte) {
	a.args = setArgBytes(a.args, key, value, argsHasValue)
}

// SetNoValue sets only 'key' as argument without the '='.
//
// Only key in argumemt, like key1&key2
func (a *Args) SetNoValue(key string) {
	a.args = setArg(a.args, key, "", argsNoValue)
}

// SetBytesKNoValue sets 'key' argument.
func (a *Args) SetBytesKNoValue(key []byte) {
	a.args = setArg(a.args, bytesconv.ToString(key), "", argsNoValue)
}

// Peek returns query arg value for the given key.
//
// Returned value is valid until the next Args call.
func (a *Args) Peek(key string) []byte {
	return peekArgStr(a.args, key)
}

// PeekBytes returns query arg value for the given key.
//
// Returned value is valid until the next Args call.
func (a *Args) PeekBytes(key []byte) []byte {
	return peekArgBytes(a.args, key)
}

// PeekMulti returns all the arg values for the given key.
func (a *Args) PeekMulti(key string) [][]byte {
	var values [][]byte
	a.VisitAll(func(k, v []byte) {
		if string(k) == key {
			values = append(values, v)
		}
	})
	return values
}

// PeekMultiBytes returns all the arg values for the given key.
func (a *Args) PeekMultiBytes(key []byte) [][]byte {
	return a.PeekMulti(bytesconv.ToString(key))
}

// Has returns true if the given key exists in Args.
func (a *Args) Has(key string) bool {
	return hasArg(a.args, key)
}

// HasBytes returns true if the given key exists in Args.
func (a *Args) HasBytes(key []byte) bool {
	return hasArg(a.args, bytesconv.ToString(key))
}

// ErrNoArgValue is returned when Args value with the given key is missing.
var ErrNoArgValue = errors.New("no Args value for the given key")

// GetUint returns uint value for the given key.
func (a *Args) GetUint(key string) (int, error) {
	value := a.Peek(key)
	if len(value) == 0 {
		return -1, ErrNoArgValue
	}
	return bytesconv.ParseUint(value)
}

// SetUint sets uint value for the given key.
func (a *Args) SetUint(key string, value int) {
	bb := buffer.Get()
	bb.B = bytesconv.AppendUint(bb.B[:0], value)
	a.SetBytesV(key, bb.B)
	buffer.Put(bb)
}

// SetUintBytes sets uint value for the given key.
func (a *Args) SetUintBytes(key []byte, value int) {
	a.SetUint(bytesconv.ToString(key), value)
}

// GetUintOrZero returns uint value for the given key.
//
// Zero (0) is returned on error.
func (a *Args) GetUintOrZero(key string) int {
	n, err := a.GetUint(key)
	if err != nil {
		n = 0
	}
	return n
}

// GetUfloat returns ufloat value for the given key.
func (a *Args) GetUfloat(key string) (float64, error) {
	value := a.Peek(key)
	if len(value) == 0 {
		return -1, ErrNoArgValue
	}
	return bytesconv.ParseUfloat(value)
}

// GetUfloatOrZero returns ufloat value for the given key.
//
// Zero (0) is returned on error.
func (a *Args) GetUfloatOrZero(key string) float64 {
	f, err := a.GetUfloat(key)
	if err != nil {
		f = 0
	}
	return f
}

// GetBool returns boolean value for the given key.
//
// true is returned for "1", "t", "T", "true", "TRUE", "True", "y", "yes", "Y", "YES", "Yes",
// otherwise false is returned.
func (a *Args) GetBool(key string) bool {
	switch bytesconv.ToString(a.Peek(key)) {
	// Support the same true cases as strconv.ParseBool
	// See: https://github.com/golang/go/blob/4e1b11e2c9bdb0ddea1141eed487be1a626ff5be/src/strconv/atob.go#L12
	// and Y and Yes versions.
	case "1", "t", "T", "true", "TRUE", "True", "y", "yes", "Y", "YES", "Yes":
		return true
	default:
		return false
	}
}

func visitArgs(args []argsKV, f func(k, v []byte)) {
	for i, n := 0, len(args); i < n; i++ {
		kv := &args[i]
		f(kv.key, kv.value)
	}
}

func copyArgs(dst, src []argsKV) []argsKV {
	if cap(dst) < len(src) {
		tmp := make([]argsKV, len(src))
		copy(tmp, dst)
		dst = tmp
	}
	n := len(src)
	dst = dst[:n]
	for i := 0; i < n; i++ {
		dstKV := &dst[i]
		srcKV := &src[i]
		dstKV.key = append(dstKV.key[:0], srcKV.key...)
		if srcKV.noValue {
			dstKV.value = dstKV.value[:0]
		} else {
			dstKV.value = append(dstKV.value[:0], srcKV.value...)
		}
		dstKV.noValue = srcKV.noValue
	}
	return dst
}

func delAllArgsBytes(args []argsKV, key []byte) []argsKV {
	return delAllArgs(args, bytesconv.ToString(key))
}

func delAllArgs(args []argsKV, key string) []argsKV {
	for i, n := 0, len(args); i < n; i++ {
		kv := &args[i]
		if key == string(kv.key) {
			tmp := *kv
			copy(args[i:], args[i+1:])
			n--
			args[n] = tmp
			args = args[:n]
		}
	}
	return args
}

func setArgBytes(h []argsKV, key, value []byte, noValue bool) []argsKV {
	return setArg(h, bytesconv.ToString(key), bytesconv.ToString(value), noValue)
}

func setArg(h []argsKV, key, value string, noValue bool) []argsKV {
	n := len(h)
	for i := 0; i < n; i++ {
		kv := &h[i]
		if key == string(kv.key) {
			if noValue {
				kv.value = kv.value[:0]
			} else {
				kv.value = append(kv.value[:0], value...)
			}
			kv.noValue = noValue
			return h
		}
	}
	return appendArg(h, key, value, noValue)
}

func appendArgBytes(h []argsKV, key, value []byte, noValue bool) []argsKV {
	return appendArg(h, bytesconv.ToString(key), bytesconv.ToString(value), noValue)
}

func appendArg(args []argsKV, key, value string, noValue bool) []argsKV {
	var kv *argsKV
	args, kv = allocArg(args)
	kv.key = append(kv.key[:0], key...)
	if noValue {
		kv.value = kv.value[:0]
	} else {
		kv.value = append(kv.value[:0], value...)
	}
	kv.noValue = noValue
	return args
}

func allocArg(h []argsKV) ([]argsKV, *argsKV) {
	n := len(h)
	if cap(h) > n {
		h = h[:n+1]
	} else {
		h = append(h, argsKV{})
	}
	return h, &h[n]
}

func releaseArg(h []argsKV) []argsKV {
	return h[:len(h)-1]
}

func hasArg(h []argsKV, key string) bool {
	for i, n := 0, len(h); i < n; i++ {
		kv := &h[i]
		if key == string(kv.key) {
			return true
		}
	}
	return false
}

func peekArgBytes(h []argsKV, k []byte) []byte {
	for i, n := 0, len(h); i < n; i++ {
		kv := &h[i]
		if bytes.Equal(kv.key, k) {
			return kv.value
		}
	}
	return nil
}

func peekArgStr(h []argsKV, k string) []byte {
	for i, n := 0, len(h); i < n; i++ {
		kv := &h[i]
		if string(kv.key) == k {
			return kv.value
		}
	}
	return nil
}

type argsScanner struct {
	b []byte
}

func (s *argsScanner) next(kv *argsKV) bool {
	if len(s.b) == 0 {
		return false
	}
	kv.noValue = argsHasValue

	isKey := true
	k := 0
	for i, c := range s.b {
		switch c {
		case '=':
			if isKey {
				isKey = false
				kv.key = bytesconv.DecodeArgAppend(kv.key[:0], s.b[:i])
				k = i + 1
			}
		case '&':
			if isKey {
				kv.key = bytesconv.DecodeArgAppend(kv.key[:0], s.b[:i])
				kv.value = kv.value[:0]
				kv.noValue = argsNoValue
			} else {
				kv.value = bytesconv.DecodeArgAppend(kv.value[:0], s.b[k:i])
			}
			s.b = s.b[i+1:]
			return true
		}
	}

	if isKey {
		kv.key = bytesconv.DecodeArgAppend(kv.key[:0], s.b)
		kv.value = kv.value[:0]
		kv.noValue = argsNoValue
	} else {
		kv.value = bytesconv.DecodeArgAppend(kv.value[:0], s.b[k:])
	}
	s.b = s.b[len(s.b):]
	return true
}
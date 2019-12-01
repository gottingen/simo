package http

import (
	"log"
	"os"
	"time"
)

const (
	defaultReadBufferSize  = 4096
	defaultWriteBufferSize = 4096
	DefaultDialTimeout = 3 * time.Second
)

var defaultLogger = Logger(log.New(os.Stderr, "", log.LstdFlags))
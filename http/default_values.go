package http

import "github.com/gottingen/viper"

const (
	defaultReadBufferSize  = 4096
	defaultWriteBufferSize = 4096
)

//var defaultLogger = Logger(log.New(os.Stderr, "", log.LstdFlags))

var defaultLogger = viper.NewNop()
package gcsmpu

import (
	"fmt"
)

type LogLevel int

const (
	// Error error log level
	Error LogLevel = iota + 1
	// Warn warn log level
	Warn
	// Info info log level
	Info
	// Debug debug log level
	Debug
	Trace
)

// Logger is a struct that implements the Interface interface
type Logger struct {
	debug bool
	log   LoggerInterface
}

// Interface logger interface
type LoggerInterface interface {
	Info(...interface{})
	Warn(...interface{})
	Error(...interface{})
	Debug(...interface{})
	Infof(string, ...interface{})
	Warnf(string, ...interface{})
	Errorf(string, ...interface{})
	Debugf(string, ...interface{})
	Trace(...interface{})
	Tracef(string, ...interface{})
}

// Log outputs the log message based on the log level
func (l *Logger) Log(logLevel LogLevel, args ...interface{}) {
	if l.log == nil {
		return
	}
	if logLevel == Debug && !l.debug {
		return
	}
	switch logLevel {
	case Error:
		l.log.Error(args)
	case Warn:
		l.log.Warn(args)
	case Info:
		l.log.Info(args)
	case Debug:
		l.log.Debug(args)
	}
}

// Logf outputs the log message based on the log level
func (l *Logger) Logf(logLevel LogLevel, format string, args ...interface{}) {
	if l.log == nil {
		return
	}
	if logLevel == Debug && !l.debug {
		return
	}

	message := fmt.Sprintf(format, args...)
	switch logLevel {
	case Error:
		l.log.Errorf(message)
	case Warn:
		l.log.Warnf(message)
	case Info:
		l.log.Infof(message)
	case Debug:
		l.log.Debugf(message)
	}
}

// Info outputs an info log message
func (l *Logger) Info(args ...interface{}) {
	l.Log(Info, args...)
}

// Warn outputs a warn log message
func (l *Logger) Warn(args ...interface{}) {
	l.Log(Warn, args...)
}

// Error outputs an error log message
func (l *Logger) Error(args ...interface{}) {
	l.Log(Error, args...)
}

// Debug outputs a debug log message
func (l *Logger) Debug(args ...interface{}) {
	l.Log(Debug, args...)
}

// NewLogger creates a new Logger object with the specified log level and logger implementation
func NewLogger(log LoggerInterface, debug bool) *Logger {
	return &Logger{
		debug: debug,
		log:   log,
	}
}

// Trace outputs a trace log message
func (l *Logger) Trace(args ...interface{}) {
	l.Log(Trace, args...)
}

// Tracef outputs a formatted trace log message
func (l *Logger) Tracef(format string, args ...interface{}) {
	l.Logf(Trace, format, args...)
}

// Infof outputs a formatted info log message
func (l *Logger) Infof(format string, args ...interface{}) {
	l.Logf(Info, format, args...)
}

// Warnf outputs a formatted warn log message
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.Logf(Warn, format, args...)
}

// Errorf outputs a formatted error log message
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.Logf(Error, format, args...)
}

// Debugf outputs a formatted debug log message
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.Logf(Debug, format, args...)
}

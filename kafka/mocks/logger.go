package mocks

import (
	"context"
	"testing"
	"time"

	"github.com/tryfix/log"
)

// TestLogger implements log.Logger interface using testing.T
// This ensures all log output appears in test output and is properly
// associated with the test that generated it.
type TestLogger struct {
	t      *testing.T
	prefix string
	level  log.Level
}

// NewTestLogger creates a new logger that outputs to testing.T
func NewTestLogger(t *testing.T) *TestLogger {
	return &TestLogger{
		t:     t,
		level: log.TRACE,
	}
}

// NewTestLoggerWithPrefix creates a new logger with a prefix
func NewTestLoggerWithPrefix(t *testing.T, prefix string) *TestLogger {
	return &TestLogger{
		t:      t,
		prefix: prefix,
		level:  log.DEBUG,
	}
}

// NewLog creates a new child logger
func (l *TestLogger) NewLog(opts ...log.Option) log.Logger {
	return &TestLogger{
		t:      l.t,
		prefix: l.prefix,
		level:  l.level,
	}
}

// NewPrefixedLog creates a new prefixed logger
func (l *TestLogger) NewPrefixedLog(opts ...log.Option) log.PrefixedLogger {
	return &TestPrefixedLogger{
		t:      l.t,
		prefix: l.prefix,
		level:  l.level,
	}
}

// Print logs a message
func (l *TestLogger) Print(v ...interface{}) {
	l.t.Helper()
	l.t.Log(v...)
}

// Printf logs a formatted message
func (l *TestLogger) Printf(format string, v ...interface{}) {
	l.t.Helper()
	timestamp := time.Now().Format("15:04:05.000000")
	l.t.Logf("[%s] "+format, append([]interface{}{timestamp}, v...)...)
}

// Println logs a message with newline
func (l *TestLogger) Println(v ...interface{}) {
	l.t.Helper()
	l.t.Log(v...)
}

func (l *TestLogger) log(level, message interface{}, params ...interface{}) {
	l.t.Helper()
	timestamp := time.Now().Format("15:04:05.000000")
	prefix := l.prefix
	if prefix != "" {
		prefix = "[" + prefix + "] "
	}
	if len(params) > 0 {
		l.t.Logf("[%s] %s%s %v %v", timestamp, prefix, level, message, params)
	} else {
		l.t.Logf("[%s] %s%s %v", timestamp, prefix, level, message)
	}
}

// Fatal logs a fatal message
func (l *TestLogger) Fatal(message interface{}, params ...interface{}) {
	l.t.Helper()
	l.log("FATAL", message, params...)
	l.t.FailNow()
}

// Error logs an error message
func (l *TestLogger) Error(message interface{}, params ...interface{}) {
	l.t.Helper()
	l.log("ERROR", message, params...)
}

// Warn logs a warning message
func (l *TestLogger) Warn(message interface{}, params ...interface{}) {
	l.t.Helper()
	l.log("WARN", message, params...)
}

// Debug logs a debug message
func (l *TestLogger) Debug(message interface{}, params ...interface{}) {
	l.t.Helper()
	l.log("DEBUG", message, params...)
}

// Info logs an info message
func (l *TestLogger) Info(message interface{}, params ...interface{}) {
	l.t.Helper()
	l.log("INFO", message, params...)
}

// Trace logs a trace message
func (l *TestLogger) Trace(message interface{}, params ...interface{}) {
	l.t.Helper()
	l.log("TRACE", message, params...)
}

// FatalContext logs a fatal message with context
func (l *TestLogger) FatalContext(ctx context.Context, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.Fatal(message, params...)
}

// ErrorContext logs an error message with context
func (l *TestLogger) ErrorContext(ctx context.Context, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.Error(message, params...)
}

// WarnContext logs a warning message with context
func (l *TestLogger) WarnContext(ctx context.Context, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.Warn(message, params...)
}

// DebugContext logs a debug message with context
func (l *TestLogger) DebugContext(ctx context.Context, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.Debug(message, params...)
}

// InfoContext logs an info message with context
func (l *TestLogger) InfoContext(ctx context.Context, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.Info(message, params...)
}

// TraceContext logs a trace message with context
func (l *TestLogger) TraceContext(ctx context.Context, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.Trace(message, params...)
}

// TestPrefixedLogger implements log.PrefixedLogger interface using testing.T
type TestPrefixedLogger struct {
	t      *testing.T
	prefix string
	level  log.Level
}

// NewLog creates a new child logger
func (l *TestPrefixedLogger) NewLog(opts ...log.Option) log.Logger {
	return &TestLogger{
		t:      l.t,
		prefix: l.prefix,
		level:  l.level,
	}
}

// NewPrefixedLog creates a new prefixed logger
func (l *TestPrefixedLogger) NewPrefixedLog(opts ...log.Option) log.PrefixedLogger {
	return &TestPrefixedLogger{
		t:      l.t,
		prefix: l.prefix,
		level:  l.level,
	}
}

// Print logs a message
func (l *TestPrefixedLogger) Print(v ...interface{}) {
	l.t.Helper()
	l.t.Log(v...)
}

// Printf logs a formatted message
func (l *TestPrefixedLogger) Printf(format string, v ...interface{}) {
	l.t.Helper()
	timestamp := time.Now().Format("15:04:05.000000")
	l.t.Logf("[%s] "+format, append([]interface{}{timestamp}, v...)...)
}

// Println logs a message with newline
func (l *TestPrefixedLogger) Println(v ...interface{}) {
	l.t.Helper()
	l.t.Log(v...)
}

func (l *TestPrefixedLogger) log(level, prefix string, message interface{}, params ...interface{}) {
	l.t.Helper()
	timestamp := time.Now().Format("15:04:05.000000")
	basePrefix := l.prefix
	if basePrefix != "" {
		basePrefix = "[" + basePrefix + "] "
	}
	if prefix != "" {
		prefix = "[" + prefix + "] "
	}
	if len(params) > 0 {
		l.t.Logf("[%s] %s%s%s %v %v", timestamp, basePrefix, prefix, level, message, params)
	} else {
		l.t.Logf("[%s] %s%s%s %v", timestamp, basePrefix, prefix, level, message)
	}
}

// Fatal logs a fatal message with prefix
func (l *TestPrefixedLogger) Fatal(prefix string, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.log("FATAL", prefix, message, params...)
	l.t.FailNow()
}

// Error logs an error message with prefix
func (l *TestPrefixedLogger) Error(prefix string, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.log("ERROR", prefix, message, params...)
}

// Warn logs a warning message with prefix
func (l *TestPrefixedLogger) Warn(prefix string, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.log("WARN", prefix, message, params...)
}

// Debug logs a debug message with prefix
func (l *TestPrefixedLogger) Debug(prefix string, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.log("DEBUG", prefix, message, params...)
}

// Info logs an info message with prefix
func (l *TestPrefixedLogger) Info(prefix string, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.log("INFO", prefix, message, params...)
}

// Trace logs a trace message with prefix
func (l *TestPrefixedLogger) Trace(prefix string, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.log("TRACE", prefix, message, params...)
}

// FatalContext logs a fatal message with context and prefix
func (l *TestPrefixedLogger) FatalContext(ctx context.Context, prefix string, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.Fatal(prefix, message, params...)
}

// ErrorContext logs an error message with context and prefix
func (l *TestPrefixedLogger) ErrorContext(ctx context.Context, prefix string, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.Error(prefix, message, params...)
}

// WarnContext logs a warning message with context and prefix
func (l *TestPrefixedLogger) WarnContext(ctx context.Context, prefix string, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.Warn(prefix, message, params...)
}

// DebugContext logs a debug message with context and prefix
func (l *TestPrefixedLogger) DebugContext(ctx context.Context, prefix string, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.Debug(prefix, message, params...)
}

// InfoContext logs an info message with context and prefix
func (l *TestPrefixedLogger) InfoContext(ctx context.Context, prefix string, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.Info(prefix, message, params...)
}

// TraceContext logs a trace message with context and prefix
func (l *TestPrefixedLogger) TraceContext(ctx context.Context, prefix string, message interface{}, params ...interface{}) {
	l.t.Helper()
	l.Trace(prefix, message, params...)
}

// Compile-time interface checks
var (
	_ log.Logger         = (*TestLogger)(nil)
	_ log.PrefixedLogger = (*TestPrefixedLogger)(nil)
)

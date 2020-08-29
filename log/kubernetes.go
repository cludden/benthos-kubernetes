package log

import (
	"fmt"

	benthoslog "github.com/Jeffail/benthos/v3/lib/log"
	"github.com/go-logr/logr"
)

var _ logr.Logger = &Logger{}

// Logger wraps the benthos logger for use by controller-runtime
type Logger struct {
	log benthoslog.Modular
}

// New returns a new context logger
func New(l benthoslog.Modular) *Logger {
	return &Logger{log: l}
}

// Enabled implementation
func (l *Logger) Enabled() bool {
	return true
}

// Error err error, implementation
func (l *Logger) Error(err error, msg string, keysAndValues ...interface{}) {
	fields := map[string]string{}
	for i := 0; i < len(keysAndValues); i += 2 {
		fields[fmt.Sprintf("%s", keysAndValues[i])] = fmt.Sprintf("%v", keysAndValues[i+1])
	}
	fields["error"] = err.Error()
	l.log.WithFields(fields).Errorln(msg)
}

// Info implementation
func (l *Logger) Info(msg string, keysAndValues ...interface{}) {
	fields := map[string]string{}
	for i := 0; i < len(keysAndValues); i += 2 {
		fields[fmt.Sprintf("%s", keysAndValues[i])] = fmt.Sprintf("%v", keysAndValues[i+1])
	}
	l.log.WithFields(fields).Infoln(msg)
}

// V returns a level scoped logger
func (l *Logger) V(level int) logr.InfoLogger {
	return &Logger{
		log: l.log,
	}
}

// WithName returns a level scoped logger
func (l *Logger) WithName(name string) logr.Logger {
	return &Logger{
		log: l.log.WithFields(map[string]string{"name": name}),
	}
}

// WithValues returns a level scoped logger
func (l *Logger) WithValues(keysAndValues ...interface{}) logr.Logger {
	fields := map[string]string{}
	for i := 0; i < len(keysAndValues); i += 2 {
		fields[fmt.Sprintf("%s", keysAndValues[i])] = fmt.Sprintf("%s", keysAndValues[i+1])
	}
	return &Logger{
		log: l.log.WithFields(fields),
	}
}

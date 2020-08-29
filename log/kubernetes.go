package log

import (
	"fmt"

	benthoslog "github.com/Jeffail/benthos/v3/lib/log"
)

// Logger wraps the benthos logger for use by controller-runtime
type Logger struct {
	log benthoslog.Logger
}

// New returns a new context logger
func New(l benthoslog.Logger) *Logger {
	return &Logger{log: l}
}

// Enabled implementation
func (l *Logger) Enabled() bool {
	return true
}

// Info implementation
func (l *Logger) Info(msg string, keysAndValues ...interface{}) {
	fields := map[string]string{}
	for i := 0; i < len(keysAndValues); i += 2 {
		fields[fmt.Sprintf("%s", keysAndValues[i])] = fmt.Sprintf("%v", keysAndValues[i+1])
	}
	l.log.WithFields(fields).Infoln(msg)
}

package log

//------------------------------------------------------------------------------

// PrintFormatter is an interface implemented by standard loggers.
type PrintFormatter interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

//------------------------------------------------------------------------------

// wrapped is an object with support for levelled logging and modular components.
type wrapped struct {
	pf    PrintFormatter
	level int
}

// Wrap a PrintFormatter with a log.Modular implementation. Log level is set to
// INFO, use WrapAtLevel to set this explicitly.
func Wrap(l PrintFormatter) Modular {
	return &wrapped{
		pf:    l,
		level: LogInfo,
	}
}

// WrapAtLevel wraps a PrintFormatter with a log.Modular implementation with an
// explicit log level.
func WrapAtLevel(l PrintFormatter, level int) Modular {
	return &wrapped{
		pf:    l,
		level: level,
	}
}

func (l *wrapped) NewModule(prefix string) Modular {
	return l
}

//------------------------------------------------------------------------------

// WithFields is a no-op.
func (l *wrapped) WithFields(fields map[string]string) Modular {
	return l
}

// Fatalf prints a fatal message to the console. Does NOT cause panic.
func (l *wrapped) Fatalf(format string, v ...interface{}) {
	if LogFatal <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Errorf prints an error message to the console.
func (l *wrapped) Errorf(format string, v ...interface{}) {
	if LogError <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Warnf prints a warning message to the console.
func (l *wrapped) Warnf(format string, v ...interface{}) {
	if LogWarn <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Infof prints an information message to the console.
func (l *wrapped) Infof(format string, v ...interface{}) {
	if LogInfo <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Debugf prints a debug message to the console.
func (l *wrapped) Debugf(format string, v ...interface{}) {
	if LogDebug <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Tracef prints a trace message to the console.
func (l *wrapped) Tracef(format string, v ...interface{}) {
	if LogTrace <= l.level {
		l.pf.Printf(format, v...)
	}
}

//------------------------------------------------------------------------------

// Fatalln prints a fatal message to the console. Does NOT cause panic.
func (l *wrapped) Fatalln(message string) {
	if LogFatal <= l.level {
		l.pf.Println(message)
	}
}

// Errorln prints an error message to the console.
func (l *wrapped) Errorln(message string) {
	if LogError <= l.level {
		l.pf.Println(message)
	}
}

// Warnln prints a warning message to the console.
func (l *wrapped) Warnln(message string) {
	if LogWarn <= l.level {
		l.pf.Println(message)
	}
}

// Infoln prints an information message to the console.
func (l *wrapped) Infoln(message string) {
	if LogInfo <= l.level {
		l.pf.Println(message)
	}
}

// Debugln prints a debug message to the console.
func (l *wrapped) Debugln(message string) {
	if LogDebug <= l.level {
		l.pf.Println(message)
	}
}

// Traceln prints a trace message to the console.
func (l *wrapped) Traceln(message string) {
	if LogTrace <= l.level {
		l.pf.Println(message)
	}
}

//------------------------------------------------------------------------------

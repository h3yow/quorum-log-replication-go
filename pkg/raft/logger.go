package raft

import (
	"log"
	"os"
)

// Logger prints state transitions and the reason for each decision.
type Logger struct{ inner *log.Logger }

// NewLogger creates a logger prefixed with the node id.
func NewLogger(id string) *Logger {
	return &Logger{inner: log.New(os.Stderr, "[raft:"+id+"] ", log.LstdFlags|log.Lmicroseconds)}
}

func (l *Logger) infof(f string, a ...any)  { l.inner.Printf("INFO  "+f, a...) }
func (l *Logger) statef(f string, a ...any) { l.inner.Printf("STATE "+f, a...) }
func (l *Logger) warnf(f string, a ...any)  { l.inner.Printf("WARN  "+f, a...) }

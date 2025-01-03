package mqtt

import (
	"fmt"
	"github.com/go-estar/logger"
)

type InfoLogger struct {
	logger logger.Logger
}

func (l *InfoLogger) Println(v ...interface{}) {
	l.logger.Info(fmt.Sprint(v...))
}
func (l *InfoLogger) Printf(format string, v ...interface{}) {
	l.logger.Info(fmt.Sprintf(format, v...))
}

type ErrorLogger struct {
	logger logger.Logger
}

func (l *ErrorLogger) Println(v ...interface{}) {
	l.logger.Error(fmt.Sprint(v...))
}
func (l *ErrorLogger) Printf(format string, v ...interface{}) {
	l.logger.Error(fmt.Sprintf(format, v...))
}

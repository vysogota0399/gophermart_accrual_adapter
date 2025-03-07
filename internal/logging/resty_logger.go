package logging

import (
	"context"
	"fmt"

	"github.com/vysogota0399/gophermart_accural_adapter/internal/config"
	"go.uber.org/zap"
)

type RestyLogger struct {
	ZapLogger
}

func NewRestyLogger(cfg *config.Config) (*RestyLogger, error) {
	baseLogger, err := NewZapLogger(cfg)
	if err != nil {
		return nil, err
	}

	return &RestyLogger{
		ZapLogger: *baseLogger,
	}, nil
}

func (l *RestyLogger) Errorf(format string, v ...interface{}) {
	l.ErrorCtx(
		l.WithContextFields(context.Background(), zap.String("name", "resty")),
		fmt.Sprintf(format, v...),
	)
}
func (l *RestyLogger) Warnf(format string, v ...interface{}) {
	l.WarnCtx(
		l.WithContextFields(context.Background(), zap.String("name", "resty")),
		fmt.Sprintf(format, v...),
	)
}
func (l *RestyLogger) Debugf(format string, v ...interface{}) {
	l.DebugCtx(
		l.WithContextFields(context.Background(), zap.String("name", "resty")),
		fmt.Sprintf(format, v...),
	)
}

package transaction_outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/vysogota0399/gophermart_accural_adapter/internal/logging"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/models"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/repositories"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Daemon struct {
	lg           *logging.ZapLogger
	pollInterval time.Duration
	workersCount int64

	cfg *Config

	cancaller context.CancelFunc
	globalCtx context.Context
	events    OutboxEventsRepository

	failedPublisher   Publisher
	finishedPublisher Publisher
	startedPublisher  Publisher
}

type Publisher interface {
	Publish(ctx context.Context, e *models.Event) error
}

type OutboxEventsRepository interface {
	FirstOutboxEvent(ctx context.Context) (*models.Event, error)
	SetState(ctx context.Context, in *models.Event) error
}

func NewDaemon(
	failed Publisher,
	finished Publisher,
	started Publisher,
	lc fx.Lifecycle,
	events OutboxEventsRepository,
	lg *logging.ZapLogger,
	cfg *Config,
) *Daemon {
	dmn := &Daemon{
		lg:                lg,
		pollInterval:      time.Duration(cfg.AccrualsOutboxPollInterval) * time.Millisecond,
		workersCount:      cfg.WorkersCount,
		events:            events,
		cfg:               cfg,
		failedPublisher:   failed,
		finishedPublisher: finished,
		startedPublisher:  started,
	}

	lc.Append(
		fx.Hook{
			OnStart: func(ctx context.Context) error {
				dmn.Start()
				return nil
			},
			OnStop: func(ctx context.Context) error {
				dmn.cancaller()
				return nil
			},
		},
	)

	return dmn
}

func (dmn *Daemon) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	dmn.cancaller = cancel
	dmn.globalCtx = ctx

	dmn.lg.InfoCtx(dmn.globalCtx, "start transaction outbox daemon", zap.Any("config", dmn.cfg))

	for i := 0; i < int(dmn.workersCount); i++ {
		wctx := dmn.lg.WithContextFields(ctx, zap.Int("worker_id", i))

		go func() {
			ticker := time.NewTicker(dmn.pollInterval)

			for {
				select {
				case <-wctx.Done():
					dmn.lg.DebugCtx(wctx, "daemon worker graceful shutdown")
					return
				case <-ticker.C:
					if err := dmn.processEvent(wctx); err != nil {
						dmn.lg.ErrorCtx(wctx, "send events finished with error", zap.Error(err))
					}
				}
			}
		}()
	}
}

func (dmn *Daemon) processEvent(ctx context.Context) error {
	e, err := dmn.events.FirstOutboxEvent(ctx)
	if err != nil {
		return fmt.Errorf("transaction_outbox/daemon send event failed error %w", err)
	}

	if e == nil {
		return nil
	}

	if err := dmn.publisher(ctx, e).Publish(ctx, e); err != nil {
		e.State = models.FailedState
		if err := dmn.events.SetState(ctx, e); err != nil {
			return fmt.Errorf("transaction_outbox/daemon update event state publish event failed error %w", err)
		}

		return fmt.Errorf("transaction_outbox/daemon publish event failed error %w", err)
	}

	e.State = models.FinishedState
	if err := dmn.events.SetState(ctx, e); err != nil {
		return fmt.Errorf("transaction_outbox/daemon update event finished state error %w", err)
	}

	return nil
}

func (dmn *Daemon) publisher(ctx context.Context, e *models.Event) Publisher {
	switch e.Name {
	case repositories.AccrualFailedEventName:
		return dmn.failedPublisher
	case repositories.AccrualStartedEventName:
		return dmn.startedPublisher
	case repositories.AccrualFinishedEventName:
		return dmn.finishedPublisher
	}

	dmn.lg.ErrorCtx(ctx, "transaction outbox publisher for event - underfined!", zap.Any("event", e))

	return nil
}

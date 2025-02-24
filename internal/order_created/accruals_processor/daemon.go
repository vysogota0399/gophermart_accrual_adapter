package accruals_processor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vysogota0399/gophermart_accural_adapter/internal/logging"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/models"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/order_created/clients"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/order_created/config"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/repositories"
	"github.com/vysogota0399/gophermart_protos/gen/events"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Daemon struct {
	lg            *logging.ZapLogger
	pollInterval  time.Duration
	semaphore     *semaphore
	cancaller     context.CancelFunc
	globalCtx     context.Context
	cfg           *config.Config
	accrualsQueue AccrualsQueue
	accrual       AccrualClient
	accrualEvents AccrualEvents
}

type semaphore struct {
	jobs chan struct{}
}

func NewSemaphore(cfg *config.Config) *semaphore {
	return &semaphore{
		jobs: make(chan struct{}, cfg.WorkersCount),
	}
}

func (s *semaphore) push() {
	s.jobs <- struct{}{}
}

func (s *semaphore) pop() {
	<-s.jobs
}

type AccrualsQueue interface {
	Push(ctx context.Context, e *events.OrderCreated) error
	Subscribe(ctx context.Context) <-chan *events.OrderCreated
}

type AccrualClient interface {
	Result(ctx context.Context, number string) (*clients.Accrual, error)
}

type AccrualEvents interface {
	CreateNewEvent(ctx context.Context, eventName string, meta *models.Meta) error
}

func NewDaemon(
	lc fx.Lifecycle,
	lg *logging.ZapLogger,
	cfg *config.Config,
	accrualsQueue AccrualsQueue,
	accrual AccrualClient,
	accrualEvents AccrualEvents,
) *Daemon {
	dmn := &Daemon{
		lg:            lg,
		pollInterval:  time.Duration(cfg.PollInterval) * time.Millisecond,
		cfg:           cfg,
		accrualsQueue: accrualsQueue,
		accrual:       accrual,
		accrualEvents: accrualEvents,
		semaphore:     NewSemaphore(cfg),
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
	dmn.globalCtx = dmn.lg.WithContextFields(ctx, zap.String("name", "order_created_daemon"))
	dmn.lg.InfoCtx(dmn.globalCtx, "accruals processor daemon started", zap.Any("config", dmn.cfg))

	go func() {
		for {
			select {
			case <-ctx.Done():
				dmn.lg.DebugCtx(ctx, "daemon worker graceful shutdown")
				return
			case event := <-dmn.accrualsQueue.Subscribe(ctx):
				dmn.doWork(ctx, event)
			}
		}
	}()
}

func (dmn *Daemon) doWork(ctx context.Context, e *events.OrderCreated) {
	dmn.semaphore.push()

	go func() {
		if err := dmn.processEvent(ctx, e); err != nil {
			dmn.lg.ErrorCtx(ctx, "process accrual error", zap.Error(err))
		}
	}()
}

func (dmn *Daemon) processEvent(ctx context.Context, e *events.OrderCreated) error {
	defer dmn.semaphore.pop()

	accrual, err := dmn.accrual.Result(ctx, e.Number)
	if err != nil {
		var errSpam *clients.ErrSpam
		if errors.As(err, &errSpam) {
			dmn.lg.DebugCtx(ctx, "too many requeste, go to sleep and requeue", zap.Any("reason", errSpam))
			time.Sleep(errSpam.RetryAfter)

			dmn.accrualsQueue.Push(ctx, e)
			return nil
		}

		if err := dmn.accrualEvents.CreateNewEvent(
			ctx,
			repositories.AccrualFailedEventName,
			&models.Meta{
				OrderUUID:   e.Uuid,
				OrderNumber: e.Number,
				Error:       err.Error(),
			},
		); err != nil {
			return fmt.Errorf("accruals_processor/daemon: save new event error %w", err)
		}

		return fmt.Errorf("accruals_processor/daemon: get accrual result error %w", err)
	}

	dmn.lg.DebugCtx(ctx, "got accrual result", zap.Any("accrual", accrual))

	if accrual.IsRegistered() || accrual.IsProcessing() {
		dmn.lg.DebugCtx(ctx, "accrual operation in progress, push to queue", zap.String("status", accrual.Status))
		dmn.accrualsQueue.Push(ctx, e)
		return nil
	}

	if accrual.IsInvalid() {
		dmn.lg.DebugCtx(ctx, "accrual invalid", zap.String("status", accrual.Status))
		if err := dmn.accrualEvents.CreateNewEvent(
			ctx,
			repositories.AccrualFailedEventName,
			&models.Meta{
				OrderUUID:   e.Uuid,
				OrderNumber: e.Number,
				Error:       fmt.Sprintf("accruale failed error, go invalud status - %s", accrual.Status),
			},
		); err != nil {
			return fmt.Errorf("accruals_processor/daemon: save new event error %w", err)
		}

		return fmt.Errorf("accruals_processor/daemon: invalid accrual status %w", err)
	}

	if err := dmn.accrualEvents.CreateNewEvent(
		ctx,
		repositories.AccrualFinishedEventName,
		&models.Meta{
			OrderUUID:   e.Uuid,
			OrderNumber: e.Number,
			Amount:      int64(accrual.Amount * 100),
		},
	); err != nil {
		return fmt.Errorf("accruals_processor/daemon: save new event error %w", err)
	}

	return nil
}

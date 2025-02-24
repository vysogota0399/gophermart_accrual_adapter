package events_processor

import (
	"context"
	"fmt"
	"time"

	"github.com/vysogota0399/gophermart_accural_adapter/internal/logging"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/models"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/order_created/clients"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/order_created/config"
	"github.com/vysogota0399/gophermart_protos/gen/events"
	"github.com/vysogota0399/gophermart_protos/gen/services/denormalized_order"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Daemon struct {
	lg            *logging.ZapLogger
	pollInterval  time.Duration
	workersCount  int64
	cancaller     context.CancelFunc
	globalCtx     context.Context
	cfg           *config.Config
	events        EventsRepository
	orders        OrderClient
	accrual       AccrualClient
	accrualsQueue AccrualsQueue
}

type EventsRepository interface {
	ReserveEvent(ctx context.Context) (*models.Event, error)
	SetStateFailed(ctx context.Context, e *models.Event) error
	SetStateFinished(ctx context.Context, e *models.Event) error
}

type OrderClient interface {
	Find(ctx context.Context, number string) (*denormalized_order.DenormalizedOrder, error)
}

type AccrualClient interface {
	Calculate(ctx context.Context, body clients.CalculateParams) error
}

type AccrualsQueue interface {
	Push(ctx context.Context, e *events.OrderCreated) error
}

func NewDaemon(
	lc fx.Lifecycle,
	lg *logging.ZapLogger,
	cfg *config.Config,
	events EventsRepository,
	accrual AccrualClient,
	accrualsQueue AccrualsQueue,
	orders OrderClient,
) *Daemon {
	dmn := &Daemon{
		lg:            lg,
		pollInterval:  time.Duration(cfg.PollInterval) * time.Millisecond,
		events:        events,
		accrual:       accrual,
		cfg:           cfg,
		accrualsQueue: accrualsQueue,
		workersCount:  cfg.WorkersCount,
		orders:        orders,
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
	dmn.globalCtx = dmn.lg.WithContextFields(ctx, zap.String("name", "accural_processor_daemon"))
	dmn.lg.InfoCtx(dmn.globalCtx, "events processor daemon started", zap.Any("config", dmn.cfg))

	for i := 0; i < int(dmn.workersCount); i++ {
		wctx := dmn.lg.WithContextFields(ctx, zap.Int("worker_id", i))
		go func() {
			ticker := time.NewTicker(dmn.pollInterval)

			dmn.lg.DebugCtx(wctx, "worker started", zap.Any("config", dmn.cfg))
			for {
				select {
				case <-wctx.Done():
					dmn.lg.DebugCtx(wctx, "daemon worker graceful shutdown")
					return
				case <-ticker.C:
					if err := dmn.processEvent(wctx); err != nil {
						dmn.lg.ErrorCtx(wctx, "process order created event error", zap.Error(err))
					}
				}
			}
		}()
	}
}

func (dmn *Daemon) processEvent(ctx context.Context) error {
	e, err := dmn.events.ReserveEvent(ctx)
	if err != nil {
		return fmt.Errorf("order_created: reserve event failed error %w", err)
	}

	if e == nil {
		return nil
	}

	ctx = dmn.lg.WithContextFields(ctx, zap.String("event_uuid", e.UUID))

	order, err := dmn.orders.Find(ctx, e.Meta.OrderNumber)
	if err != nil {
		e.Meta.Error = err.Error()
		if err := dmn.events.SetStateFailed(ctx, e); err != nil {
			return fmt.Errorf("order_created: process event failed, save event state error %w", err)
		}

		return fmt.Errorf("order_created: process event failed %w", err)
	}

	goods := []clients.CalculateParamsProduct{}
	for _, p := range order.Goods {
		goods = append(
			goods,
			clients.CalculateParamsProduct{
				Price:       p.Price,
				Description: p.Name,
			},
		)
	}

	if err := dmn.accrual.Calculate(
		ctx,
		clients.CalculateParams{
			Order: order.Number,
			Goods: goods,
		},
	); err != nil {
		e.Meta.Error = err.Error()
		if err := dmn.events.SetStateFailed(ctx, e); err != nil {
			return fmt.Errorf("order_created: process event failed, save event state error %w", err)
		}

		return fmt.Errorf("order_created: init calculate accrural error %w", err)
	}

	if err := dmn.accrualsQueue.Push(
		ctx,
		&events.OrderCreated{
			Number:    order.Number,
			EventUuid: e.UUID,
			Uuid:      e.Meta.OrderUUID,
		},
	); err != nil {
		e.Meta.Error = err.Error()
		if err := dmn.events.SetStateFailed(ctx, e); err != nil {
			return fmt.Errorf("order_created: save acctrual to queue error, save event state error  %w", err)
		}

		return fmt.Errorf("order_created: save acctrual to queue error %w", err)
	}

	if err := dmn.events.SetStateFinished(ctx, e); err != nil {
		return fmt.Errorf("order_created: save finished state error %w", err)
	}

	return nil
}

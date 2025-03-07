package repositories

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/vysogota0399/gophermart_accural_adapter/internal/logging"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/models"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/storage"
	"go.uber.org/zap"
)

var OrderCreatedEventName = "order_created"

type OrderEventsRepository struct {
	strg   OrderEventsStorage
	lg     *logging.ZapLogger
	events AccrualEvents
}

type OrderEventsStorage interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type AccrualEvents interface {
	CreateNewEventTX(ctx context.Context, OrderCreatedEventName string, meta *models.Meta, tx *sql.Tx) error
}

func NewOrderEventsRepository(strg *storage.Storage, lg *logging.ZapLogger, events AccrualEvents) *OrderEventsRepository {
	return &OrderEventsRepository{strg: strg.DB, lg: lg, events: events}
}

func (rep *OrderEventsRepository) ReserveEvent(ctx context.Context) (*models.Event, error) {
	tx, err := rep.strg.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("processor/repositories/order_events_repository: create tx error %w", err)
	}
	defer tx.Rollback()

	e := &models.Event{}
	row := tx.QueryRow(`
											SELECT uuid, state, name, message
											FROM events
											WHERE name = $1 AND state = $2
											ORDER BY created_at ASC
											FOR UPDATE SKIP LOCKED
											LIMIT 1
										`,
		OrderCreatedEventName, models.NewState)

	if err := row.Scan(&e.UUID, &e.State, &e.Name, &e.Meta); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("processor/repositories/order_events_repository: select models.Event error %w", err)
	}

	e.State = models.ProcessingState
	if err := rep.setStateTX(ctx, e.UUID, e.State, tx); err != nil {
		return nil, fmt.Errorf("processor/repositories/order_events_repository:set new state error %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("processor/repositories/order_events_repository: commit tx error %w", err)
	}
	return e, nil
}

func (rep *OrderEventsRepository) SetStateFailed(ctx context.Context, in *models.Event) error {
	tx, err := rep.strg.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("order_events_repository: create tx failed error %w", err)
	}
	defer tx.Rollback()

	if err := rep.setStateTX(ctx, in.UUID, models.FailedState, tx); err != nil {
		return fmt.Errorf("order_events_repository: update event failed state error %w", err)
	}

	if err := rep.events.CreateNewEventTX(
		ctx,
		AccrualFailedEventName,
		in.Meta,
		tx,
	); err != nil {
		return fmt.Errorf("order_events_repository: create new accrual event error %w", err)
	}

	return tx.Commit()
}

func (rep *OrderEventsRepository) SetStateFinished(ctx context.Context, in *models.Event) error {
	tx, err := rep.strg.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("order_events_repository: create tx failed error %w", err)
	}

	defer tx.Rollback()

	if err := rep.setStateTX(ctx, in.UUID, models.FinishedState, tx); err != nil {
		return fmt.Errorf("order_events_repository: update event finished state error %w", err)
	}

	if err := rep.events.CreateNewEventTX(
		ctx,
		AccrualStartedEventName,
		in.Meta,
		tx,
	); err != nil {
		return fmt.Errorf("order_events_repository: create new accrual event error %w", err)
	}

	return tx.Commit()
}

func (rep *OrderEventsRepository) setStateTX(ctx context.Context, uuid string, newState string, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx,
		`
			UPDATE events
			SET state = $1
			WHERE uuid = $2
		`,
		newState, uuid); err != nil {
		rep.lg.ErrorCtx(ctx, "set state error", zap.Error(err))
		return fmt.Errorf("repositories/order_events_repository: update state error %w", err)
	}

	return nil
}

func (rep *OrderEventsRepository) SaveOrderCreated(ctx context.Context, in *models.Event) error {
	_, err := rep.strg.ExecContext(
		ctx,
		`
		INSERT INTO events(uuid, state, name, message)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING
		`,
		in.UUID, in.State, in.Name, in.Meta,
	)

	if err != nil {
		return fmt.Errorf("orders_repository: save order error %w", err)
	}

	return nil
}

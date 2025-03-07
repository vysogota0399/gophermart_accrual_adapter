package repositories

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	uuid "github.com/satori/go.uuid"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/logging"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/models"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/storage"
)

type AccrualEventsRepository struct {
	strg AccrualEventsStorage
	lg   *logging.ZapLogger
}

var AccrualFailedEventName = "accrual_failed"
var AccrualFinishedEventName = "accrual_finished"
var AccrualStartedEventName = "accrual_started"

var OutboxEvents = []string{AccrualFailedEventName, AccrualFinishedEventName, AccrualStartedEventName}

type AccrualEventsStorage interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func NewAccrualEventsRepository(strg *storage.Storage, lg *logging.ZapLogger) *AccrualEventsRepository {
	return &AccrualEventsRepository{strg: strg.DB, lg: lg}
}

func (rep *AccrualEventsRepository) CreateNewEvent(ctx context.Context, eventName string, meta *models.Meta) error {
	tx, err := rep.strg.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("processor/repositories/accrual_events_repository: create tx error %w", err)
	}
	defer tx.Rollback()

	if err := rep.CreateNewEventTX(ctx, eventName, meta, tx); err != nil {
		return fmt.Errorf("processor/repositories/accrual_events_repository: create event new error %w", err)
	}

	tx.Commit()
	return nil
}

func (rep *AccrualEventsRepository) CreateNewEventTX(ctx context.Context, eventName string, meta *models.Meta, tx *sql.Tx) error {
	if _, err := tx.ExecContext(
		ctx,
		`
			INSERT INTO events(uuid, state, name, message)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT DO NOTHING
		`,
		uuid.NewV4().String(),
		models.NewState,
		eventName,
		meta,
	); err != nil {
		return fmt.Errorf("accrual_events_repository: save event failed error %w", err)
	}
	return nil
}

func (rep *AccrualEventsRepository) SetState(ctx context.Context, in *models.Event) error {
	_, err := rep.strg.ExecContext(
		ctx,
		`
			UPDATE events
			SET state = $1
			WHERE uuid = $2
		`,
		in.State,
		in.UUID,
	)

	if err != nil {
		return fmt.Errorf("processor/repositories/accrual_events_repository: update state error %w", err)
	}

	return nil
}

func (rep *AccrualEventsRepository) FirstOutboxEvent(ctx context.Context) (*models.Event, error) {
	tx, err := rep.strg.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("processor/repositories/accrual_events_repository: create tx error %w", err)
	}
	defer tx.Rollback()

	e := &models.Event{}

	row := tx.QueryRowContext(
		ctx,
		`
			SELECT uuid, name, message
			FROM events
			WHERE name = ANY($1) AND state = $2
			FOR UPDATE NOWAIT
			LIMIT 1
		`,
		OutboxEvents,
		models.NewState,
	)

	if err := row.Scan(&e.UUID, &e.Name, &e.Meta); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgerrcode.IsObjectNotInPrerequisiteState(pgErr.Code) {
			return nil, nil
		}

		return nil, fmt.Errorf("processor/repositories/accrual_events_repository: select error %w", err)
	}

	_, err = tx.ExecContext(
		ctx,
		`
			UPDATE events
			SET state = $1
			WHERE uuid = $2
		`,
		models.ProcessingState,
		e.UUID,
	)

	if err != nil {
		return nil, fmt.Errorf("processor/repositories/accrual_events_repository: update state error %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("processor/repositories/accrual_events_repository: commit tx error %w", err)
	}
	return e, nil
}

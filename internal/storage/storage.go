package storage

import (
	"context"
	"database/sql"
	"embed"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/config"
	"go.uber.org/fx"
)

type Storage struct {
	DB *sql.DB
}

func NewStorage(lc fx.Lifecycle, cfg *config.Config) (*Storage, error) {
	db, err := sql.Open("pgx", cfg.DatabaseDSN)
	if err != nil {
		return nil, err
	}

	strg := &Storage{
		DB: db,
	}

	lc.Append(
		fx.Hook{
			OnStart: func(ctx context.Context) error {
				if err := db.Ping(); err != nil {
					db.Close()
					return err
				}

				return nil
			},
			OnStop: func(ctx context.Context) error {
				return strg.DB.Close()
			},
		},
	)

	return strg, nil
}

//go:embed migrations/*.sql
var embedMigrations embed.FS

func RunMigration(cfg *config.Config) error {
	goose.SetBaseFS(embedMigrations)

	db, err := sql.Open("pgx", cfg.DatabaseDSN)
	if err != nil {
		return err
	}

	if err := goose.SetDialect(string(goose.DialectPostgres)); err != nil {
		return err
	}

	return goose.Up(db, "migrations")
}

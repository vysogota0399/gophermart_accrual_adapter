package config

import "github.com/caarlos0/env"

type Config struct {
	LogLevel          int      `json:"log_level" env:"LOG_LEVEL" envDefault:"-1"`
	DatabaseDSN       string   `json:"database_dsn" env:"DATABASE_DSN" envDefault:"postgres://postgres:secret@127.0.0.1:5432/gophermart_accrual_adapter_development"`
	RedisDSN          string   `json:"redis_dsn" env:"REDIS_DSN" envDefault:"redis://127.0.0.1:6379/0"`
	AccrualsQueueName string   `json:"accruals_queue_name" env:"ACCRUALS_QUEUE_NAME" envDefault:"accruals_in_progress"`
	KafkaBrokers      []string `json:"kafka_brokers" env:"KAFKA_BROKERS" envDefault:"127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094" envSeparator:","`
	KafkaLogLevel               int      `json:"kafka_log_level" env:"KAFKA_LOG_LEVEL" envDefault:"0"`
}

func MustNewConfig() *Config {
	c := &Config{}
	env.Parse(c)

	return c
}

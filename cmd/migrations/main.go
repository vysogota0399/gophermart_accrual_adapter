package main

import (
	"github.com/vysogota0399/gophermart_accural_adapter/internal/config"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/storage"
)

func main() {
	storage.RunMigration(config.MustNewConfig())
}

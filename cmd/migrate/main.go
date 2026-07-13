package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/ayushsarode/distributed-job-scheduler/internal/config"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "migration failed: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	command := "up"
	if len(args) > 0 {
		command = args[0]
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	migrationsPath := os.Getenv("MIGRATIONS_PATH")
	if migrationsPath == "" {
		migrationsPath = "migrations"
	}

	pgxCfg, err := pgx.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}

	db := stdlib.OpenDB(*pgxCfg)
	defer db.Close()

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("create postgres migration driver: %w", err)
	}

	m, err := migrate.NewWithDatabaseInstance("file://"+migrationsPath, "postgres", driver)
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}
	defer m.Close()

	switch command {
	case "up":
		err = m.Up()
	case "down":
		err = m.Down()
	case "version":
		version, dirty, err := m.Version()
		if errors.Is(err, migrate.ErrNilVersion) {
			fmt.Println("no migrations applied")
			return nil
		}
		if err != nil {
			return err
		}
		fmt.Printf("version=%d dirty=%t\n", version, dirty)
		return nil
	case "force":
		if len(args) != 2 {
			return fmt.Errorf("usage: go run ./cmd/migrate force <version>")
		}
		version, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("invalid version: %w", err)
		}
		err = m.Force(version)
	default:
		return fmt.Errorf("unknown command %q; use up, down, version, or force", command)
	}

	if errors.Is(err, migrate.ErrNoChange) {
		fmt.Println("no migrations to apply")
		return nil
	}
	if err != nil {
		return err
	}

	fmt.Printf("migration %s complete\n", command)
	return nil
}

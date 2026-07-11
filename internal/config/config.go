package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)


type Config struct {
	PostgresDSN string
	HTTPPort int
}

func Load() (*Config, error) {
	_ = godotenv.Load() // Ignore error if .env doesn't exist

	dsn := os.Getenv("DATABASE_URL")

	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}

	port := 8080

	if p := os.Getenv("HTTP_PORT"); p != "" {
		v, err := strconv.Atoi(p)

		if err != nil {
			return nil, fmt.Errorf("invalid HTTP_PORT: %w", err)
		}
		port = v
	}

	return &Config{PostgresDSN: dsn, HTTPPort: port}, nil
}
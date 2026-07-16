package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	PostgresDSN  string
	HTTPPort     int
	KafkaBrokers []string
	RedisAddr    string
	APIKey       string // API_KEY env var; empty means auth is bypassed (dev mode)
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

	brokers := []string{"localhost:9092"}
	if b := os.Getenv("KAFKA_BROKERS"); b != "" {
		brokers = strings.Split(b, ",")
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	return &Config{
		PostgresDSN:  dsn,
		HTTPPort:     port,
		KafkaBrokers: brokers,
		RedisAddr:    redisAddr,
		APIKey:       os.Getenv("API_KEY"),
	}, nil
}

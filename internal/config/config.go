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
	GRPCPort     int
	ControlAddr  string
	KafkaBrokers []string
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

	grpcPort := 9090
	if p := os.Getenv("GRPC_PORT"); p != "" {
		v, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("invalid GRPC_PORT: %w", err)
		}
		grpcPort = v
	}

	brokers := []string{"localhost:9092"}
	if b := os.Getenv("KAFKA_BROKERS"); b != "" {
		brokers = strings.Split(b, ",")
	}

	controlAddr := os.Getenv("CONTROL_ADDR")
	if controlAddr == "" {
		controlAddr = fmt.Sprintf("localhost:%d", grpcPort)
	}

	return &Config{
		PostgresDSN:  dsn,
		HTTPPort:     port,
		GRPCPort:     grpcPort,
		ControlAddr:  controlAddr,
		KafkaBrokers: brokers,
	}, nil
}

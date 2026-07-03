package logger

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

func New(service string) zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339

	var output = os.Stdout

	var writer zerolog.ConsoleWriter

	if os.Getenv("LOG_FORMAT") == "console" {
		writer = zerolog.ConsoleWriter{Out: output, TimeFormat: time.Kitchen}
		return zerolog.New(writer).With().Timestamp().Str("service", service).Logger()
	}

	return zerolog.New(output).With().Timestamp().Str("service", service).Logger()
}

package logger

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"strings"
)

func Init(pretty bool, level string) {
	l := zerolog.InfoLevel
	switch strings.ToLower(level) {
	case "debug":
		l = zerolog.DebugLevel
	case "info":
		l = zerolog.InfoLevel
	case "warn":
		l = zerolog.WarnLevel
	case "error":
		l = zerolog.ErrorLevel
	}
	zerolog.SetGlobalLevel(l)
	if pretty {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}
}

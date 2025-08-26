package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	HTTPAddr     string
	PGURL        string
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
	LogPretty    bool
	LogLevel     string
}

func Load() Config {
	_ = godotenv.Load()

	return Config{
		HTTPAddr: getenv("HTTP_ADDR", ":8081"),
		PGURL: firstNonEmpty(
			os.Getenv("POSTGRES_DSN"),
		),
		KafkaBrokers: split(getenv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:   getenv("KAFKA_TOPIC", "orders"),
		KafkaGroupID: firstNonEmpty(os.Getenv("KAFKA_GROUP_ID"), os.Getenv("KAFKA_GROUP"), "orders-consumer"),
		LogPretty:    getbool("LOG_PRETTY", true),
		LogLevel:     getenv("LOG_LEVEL", "info"),
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getbool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return def
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

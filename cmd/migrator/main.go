package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	defaultMigrationsPath = "file:///migrations"
	defaultRetryAttempts  = 10
	defaultRetryTimeout   = 5 * time.Second
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	log.Println("Starting migrator...")

	host := getenv("POSTGRES_HOST", "db")
	port := getenv("POSTGRES_PORT", "5432")
	user := getenv("POSTGRES_USER", "postgres")
	pass := getenv("POSTGRES_PASSWORD", "postgres")
	// поддерживаем оба имени: POSTGRES_DB и POSTGRES_DATABASE
	name := getenv("POSTGRES_DB", getenv("POSTGRES_DATABASE", "orders"))

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, name)

	var m *migrate.Migrate
	var err error
	for i := 0; i < defaultRetryAttempts; i++ {
		m, err = migrate.New(defaultMigrationsPath, dsn)
		if err == nil {
			break
		}
		log.Printf("init migrate failed (%d/%d): %v; retry in %s",
			i+1, defaultRetryAttempts, err, defaultRetryTimeout)
		time.Sleep(defaultRetryTimeout)
	}
	if err != nil {
		log.Fatalf("create migrate instance: %v", err)
	}

	log.Println("Applying migrations...")
	if err = m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatalf("run migrations: %v", err)
	}
	srcErr, dbErr := m.Close()
	if srcErr != nil {
		log.Printf("migrate source close: %v", srcErr)
	}
	if dbErr != nil {
		log.Printf("migrate db close: %v", dbErr)
	}

	if err == migrate.ErrNoChange {
		log.Println("No new migrations.")
	} else {
		log.Println("Migrations applied successfully!")
	}
}

package app

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/ratmirtech/techwb-l0/internal/cache"
	"github.com/ratmirtech/techwb-l0/internal/config"
	"github.com/ratmirtech/techwb-l0/internal/httpapi"
	"github.com/ratmirtech/techwb-l0/internal/kafka"
	"github.com/ratmirtech/techwb-l0/internal/logger"
	"github.com/ratmirtech/techwb-l0/internal/repo"

	"github.com/rs/zerolog/log"
)

func Run(ctx context.Context) {
	cfg := config.Load()

	logger.Init(cfg.LogPretty, cfg.LogLevel)
	log.Info().Msg("starting service")

	pg, err := repo.New(ctx, cfg.PGURL)
	if err != nil {
		log.Fatal().Err(err).Msg("db connect")
	}
	defer pg.Close()

	c := cache.New()
	if err := c.WarmUp(ctx, pg); err != nil {
		log.Warn().Err(err).Msg("cache warmup")
	} else {
		log.Info().Int("orders", c.Len()).Msg("cache warmed")
	}

	srv := httpapi.New(c, pg)
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Router(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	consumer := kafka.NewConsumer(cfg, pg, c)

	errCh := make(chan error, 2)
	go func() { errCh <- consumer.Run(ctx) }()
	go func() {
		log.Info().Str("addr", cfg.HTTPAddr).Msg("http listen")
		errCh <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		log.Info().Msg("shutting down...")
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("fatal")
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	consumer.Close()
	log.Info().Msg("bye")
	os.Exit(0)
}

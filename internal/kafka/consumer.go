package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ratmirtech/techwb-l0/internal/cache"
	"github.com/ratmirtech/techwb-l0/internal/config"
	"github.com/ratmirtech/techwb-l0/internal/models"
	"github.com/ratmirtech/techwb-l0/internal/repo"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	repo   *repo.PG
	cache  *cache.Store
}

func NewConsumer(cfg config.Config, r *repo.PG, c *cache.Store) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		GroupID:        cfg.KafkaGroupID,
		Topic:          cfg.KafkaTopic,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0, // ручной коммит после успешной обработки
	})
	return &Consumer{reader: reader, repo: r, cache: c}
}

func (c *Consumer) Run(ctx context.Context) error {
	log.Info().Strs("brokers", c.reader.Config().Brokers).Str("topic", c.reader.Config().Topic).
		Str("group", c.reader.Config().GroupID).Msg("kafka: start consumer")

	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		var order models.Order
		if err := json.Unmarshal(m.Value, &order); err != nil {
			log.Error().Err(err).Msg("kafka: bad message (skip)")
			// подтверждать нельзя — иначе потеряем; отправим в DLQ? Для демо — просто не коммитим и спим.
			time.Sleep(300 * time.Millisecond)
			continue
		}
		// простая валидация
		if order.OrderUID == "" || order.Payment.Transaction == "" {
			log.Warn().Str("key", string(m.Key)).Msg("validation failed (skip)")
			_ = c.reader.CommitMessages(ctx, m) // подтверждаем как просмотренное
			continue
		}

		if err := c.repo.UpsertOrder(ctx, order); err != nil {
			log.Error().Err(err).Msg("db write failed; will retry")
			// не коммитим — сообщение вернётся позже
			time.Sleep(500 * time.Millisecond)
			continue
		}
		c.cache.Set(order)

		if err := c.reader.CommitMessages(ctx, m); err != nil {
			log.Error().Err(err).Msg("commit failed")
		}
	}
}

func (c *Consumer) Close() { _ = c.reader.Close() }

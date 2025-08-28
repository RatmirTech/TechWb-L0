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
	"github.com/segmentio/kafka-go"

	"github.com/rs/zerolog/log"
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
		CommitInterval: 0,
	})
	return &Consumer{reader: reader, repo: r, cache: c}
}

func (c *Consumer) Run(ctx context.Context) error {
	log.Info().Strs("brokers", c.reader.Config().Brokers).Str("topic", c.reader.Config().Topic).
		Str("group", c.reader.Config().GroupID).Msg("Starting Kafka consumer")

	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			log.Error().Err(err).Msg("Failed to read message")
			return err
		}

		log.Info().Str("key", string(m.Key)).Msg("Got a message")

		var order models.Order
		if err := json.Unmarshal(m.Value, &order); err != nil {
			log.Error().Err(err).Str("value", string(m.Value)).Msg("Bad JSON, skipping")
			time.Sleep(300 * time.Millisecond)
			continue
		}

		log.Info().Str("order_uid", order.OrderUID).Msg("Parsed order")

		if order.OrderUID == "" || order.Payment.Transaction == "" {
			log.Warn().Str("key", string(m.Key)).Msg("Invalid order, skipping")
			_ = c.reader.CommitMessages(ctx, m)
			continue
		}

		if err := c.repo.UpsertOrder(ctx, order); err != nil {
			log.Error().Err(err).Str("order_uid", order.OrderUID).Msg("Failed to save to DB, will retry")
			time.Sleep(500 * time.Millisecond)
			continue
		}

		log.Info().Str("order_uid", order.OrderUID).Msg("Saved to DB")

		c.cache.Set(order)
		log.Info().Str("order_uid", order.OrderUID).Msg("Cached order")

		if err := c.reader.CommitMessages(ctx, m); err != nil {
			log.Error().Err(err).Str("order_uid", order.OrderUID).Msg("Failed to commit")
		} else {
			log.Info().Str("order_uid", order.OrderUID).Msg("Message committed")
		}
	}
}

func (c *Consumer) Close() { _ = c.reader.Close() }

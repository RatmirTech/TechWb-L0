package cache

import (
	"context"
	"sync"

	"github.com/ratmirtech/techwb-l0/internal/models"
	"github.com/ratmirtech/techwb-l0/internal/repo"
	"github.com/rs/zerolog/log"
)

type Store struct {
	mu sync.RWMutex
	m  map[string]models.Order
}

func New() *Store {
	return &Store{m: make(map[string]models.Order)}
}

func (s *Store) Get(id string) (models.Order, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.m[id]
	if ok {
		log.Info().Str("order_uid", id).Msg("Found order in cache")
	} else {
		log.Warn().Str("order_uid", id).Msg("Order not found in cache")
	}
	return o, ok
}

func (s *Store) Set(o models.Order) {
	s.mu.Lock()
	s.m[o.OrderUID] = o
	s.mu.Unlock()
	log.Info().Str("order_uid", o.OrderUID).Msg("Saved order to cache")
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

func (s *Store) WarmUp(ctx context.Context, r *repo.PG) error {
	orders, err := r.GetAllOrders(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to load orders from DB")
		return err
	}
	s.mu.Lock()
	for _, o := range orders {
		s.m[o.OrderUID] = o
	}
	s.mu.Unlock()
	log.Info().Int("count", len(orders)).Msg("Loaded orders to cache")
	return nil
}

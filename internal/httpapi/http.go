package httpapi

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/ratmirtech/techwb-l0/internal/cache"
	"github.com/ratmirtech/techwb-l0/internal/repo"

	"github.com/rs/zerolog/log"
)

//go:embed static/index.html
var indexHTML string

type API struct {
	cache *cache.Store
	repo  *repo.PG
}

func New(c *cache.Store, r *repo.PG) *API { return &API{cache: c, repo: r} }

func (a *API) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", a.handleIndex)
	mux.HandleFunc("/order/", a.handleGetOrder)
	mux.HandleFunc("/api/order/", a.handleGetOrder)
	return logMiddleware(mux)
}

func (a *API) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(indexHTML))
}

func (a *API) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/order/")
	id = strings.TrimPrefix(id, "/api/order/")
	if id == "" || id == "/" {
		http.Error(w, "order id required", http.StatusBadRequest)
		return
	}
	if idx := strings.IndexByte(id, '/'); idx >= 0 {
		id = id[:idx]
	}

	if o, ok := a.cache.Get(id); ok {
		writeJSON(w, o, http.StatusOK)
		return
	}
	o, err := a.repo.GetOrder(r.Context(), id)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	a.cache.Set(o)
	writeJSON(w, o, http.StatusOK)
}

func writeJSON(w http.ResponseWriter, v any, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Info().Str("method", r.Method).Str("path", r.URL.Path).Msg("http")
		next.ServeHTTP(w, r)
	})
}

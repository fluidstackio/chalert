// Package web provides the HTTP server for health checks, metrics, and version info.
package web

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server is the chalert HTTP server for health, readiness, metrics, and version info.
type Server struct {
	srv      *http.Server
	ready    atomic.Bool
	reloadFn atomic.Pointer[func()]
}

// New creates a new HTTP server on the given address.
func New(addr, version string) *Server {
	s := &Server{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.HandleFunc("POST /-/reload", s.handleReload)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"version": version})
	})

	s.srv = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return s
}

// SetReady marks the server as ready to receive traffic.
func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

// SetReloadFunc registers the function invoked by POST /-/reload.
// Until it is set, the endpoint responds 503. The function must be
// non-blocking: it schedules a reload rather than performing it.
func (s *Server) SetReloadFunc(fn func()) {
	s.reloadFn.Store(&fn)
}

func (s *Server) handleReload(w http.ResponseWriter, _ *http.Request) {
	fn := s.reloadFn.Load()
	if fn == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("reload not available"))
		return
	}
	(*fn)()
	slog.Info("reload requested via /-/reload")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("reload scheduled"))
}

// ListenAndServe starts the HTTP server. Blocks until the server stops.
func (s *Server) ListenAndServe() error {
	slog.Info("http server listening", "addr", s.srv.Addr)
	return s.srv.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

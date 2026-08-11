package web

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func newTestServer() (*Server, http.Handler) {
	s := New(":0", "test")
	return s, s.srv.Handler
}

func TestHandleReload(t *testing.T) {
	t.Run("503 before a reload func is registered", func(t *testing.T) {
		_, h := newTestServer()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/-/reload", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("expected 503, got %d", rec.Code)
		}
	})

	t.Run("invokes the registered func", func(t *testing.T) {
		s, h := newTestServer()
		var calls atomic.Int32
		s.SetReloadFunc(func() { calls.Add(1) })

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/-/reload", nil))
		if rec.Code != http.StatusAccepted {
			t.Errorf("expected 202, got %d", rec.Code)
		}
		if got := calls.Load(); got != 1 {
			t.Errorf("expected 1 reload call, got %d", got)
		}

		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/-/reload", nil))
		if got := calls.Load(); got != 2 {
			t.Errorf("expected 2 reload calls, got %d", got)
		}
	})

	t.Run("GET is not allowed", func(t *testing.T) {
		s, h := newTestServer()
		var calls atomic.Int32
		s.SetReloadFunc(func() { calls.Add(1) })

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/-/reload", nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", rec.Code)
		}
		if got := calls.Load(); got != 0 {
			t.Errorf("expected no reload calls, got %d", got)
		}
	})
}

func TestHandleReady(t *testing.T) {
	s, h := newTestServer()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 before ready, got %d", rec.Code)
	}

	s.SetReady(true)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 when ready, got %d", rec.Code)
	}
}

func TestHandleHealth(t *testing.T) {
	_, h := newTestServer()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

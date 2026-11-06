package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestIDMiddleware(t *testing.T) {
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := RequestIDMiddleware(nextHandler)

	t.Run("Debe generar un Request-ID si no viene en el header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()

		middleware.ServeHTTP(rec, req)

		resID := rec.Header().Get("X-Request-ID")
		if resID == "" {
			t.Errorf("Se esperaba que el middleware inyectara un X-Request-ID en la respuesta")
		}
	})

	t.Run("Debe preservar el X-Request-ID si el cliente ya lo envía", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		customID := "custom-trace-123"
		req.Header.Set("X-Request-ID", customID)
		rec := httptest.NewRecorder()

		middleware.ServeHTTP(rec, req)

		resID := rec.Header().Get("X-Request-ID")
		if resID != customID {
			t.Errorf("Se esperaba %s, se obtuvo %s", customID, resID)
		}
	})
}
func TestGetRequestID_Function(t *testing.T) {
	ctx := context.Background()

	t.Run("Debe retornar unknown si no existe el Request-ID en el contexto", func(t *testing.T) {
		id := GetRequestID(ctx)
		if id != "unknown" {
			t.Errorf("Se esperaba 'unknown', se obtuvo %s", id)
		}
	})

	t.Run("Debe retornar el Request-ID cuando está presente en el contexto", func(t *testing.T) {
		expectedID := "req-abc-123"
		ctxWithID := context.WithValue(ctx, contextKey("request_id"), expectedID)

		id := GetRequestID(ctxWithID)
		if id != expectedID {
			t.Errorf("Se esperaba %s, se obtuvo %s", expectedID, id)
		}
	})
}

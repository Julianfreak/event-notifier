package middleware

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

type contextKey string

const RequestIDKey contextKey = "request_id"

// RequestIDMiddleware intercepta peticiones HTTP, inyecta/genera un Request ID y configura slog
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = uuid.New().String()
		}

		// Devolver el ID en la respuesta HTTP para debugging del cliente
		w.Header().Set("X-Request-ID", reqID)

		// Crear contexto con el ID
		ctx := context.WithValue(r.Context(), RequestIDKey, reqID)

		// Crear un logger hijo con el request_id predeterminado
		logger := slog.With("request_id", reqID)
		ctx = context.WithValue(ctx, "logger", logger)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetRequestID extrae el ID del contexto de forma segura
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(RequestIDKey).(string); ok {
		return id
	}
	return "unknown"
}

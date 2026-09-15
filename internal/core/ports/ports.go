package ports

import (
	"context"
	"time"

	"event-notifier/internal/core/domain"
)

// ==========================================
// PUERTOS DE SALIDA (Infraestructura / Conectores)
// ==========================================

// IdempotencyRepository define el contrato para verificar y bloquear duplicados (ej. Redis)
type IdempotencyRepository interface {
	EsDuplicado(ctx context.Context, key string) (bool, error)
	MarcarProcesado(ctx context.Context, key string, ttl time.Duration) error
}

// NotificationSender define el contrato para despachar notificaciones al mundo exterior (ej. SES, Twilio, Firebase)
type NotificationSender interface {
	Enviar(ctx context.Context, n *domain.Notificacion) error
	SoportaCanal(canal domain.CanalNotificacion) bool
}

// ==========================================
// PUERTOS DE ENTRADA (Casos de Uso / Procesamiento)
// ==========================================

// NotificationDispatcher define el caso de uso para orquestar el envío seguro
type NotificationDispatcher interface {
	Procesar(ctx context.Context, n *domain.Notificacion) error
}

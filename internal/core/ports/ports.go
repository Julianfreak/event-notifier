package ports

import (
	"context"
	"time"

	"event-notifier/internal/core/domain"
)

// ==========================================
// PUERTOS DE SALIDA (Infraestructura / Conectores)
// ==========================================

// IdempotencyRepository define el contrato para verificar y bloquear duplicados en Redis
type IdempotencyRepository interface {
	EsDuplicado(ctx context.Context, key string) (bool, error)
	MarcarProcesado(ctx context.Context, key string, ttl time.Duration) error
}

// EventPublisher define el contrato para publicar eventos hacia el broker (RabbitMQ)
type EventPublisher interface {
	Publicar(ctx context.Context, n *domain.Notificacion) error
}

// EventConsumer define el contrato para escuchar y procesar eventos entrantes del broker
type EventConsumer interface {
	IniciarConsumo(ctx context.Context, handler func(n *domain.Notificacion) error) error
	Cerrar() error
}

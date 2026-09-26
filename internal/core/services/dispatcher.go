package services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"event-notifier/internal/adapters/metrics"
	"event-notifier/internal/core/domain"
	"event-notifier/internal/core/ports"

	"github.com/prometheus/client_golang/prometheus"
)

type NotificationDispatcher struct {
	idempotencyRepo ports.IdempotencyRepository
	maxRetries      int
	initialBackoff  time.Duration
}

func NewNotificationDispatcher(
	idempotencyRepo ports.IdempotencyRepository,
	maxRetries int,
	initialBackoff time.Duration,
) *NotificationDispatcher {
	return &NotificationDispatcher{
		idempotencyRepo: idempotencyRepo,
		maxRetries:      maxRetries,
		initialBackoff:  initialBackoff,
	}
}

func (d *NotificationDispatcher) DespacharConResiliencia(ctx context.Context, n *domain.Notificacion) error {
	timer := prometheus.NewTimer(metrics.ProcessingDuration.WithLabelValues(string(n.Canal)))
	defer timer.ObserveDuration()

	esDuplicado, err := d.idempotencyRepo.EsDuplicado(ctx, n.IdempotencyKey)
	if err != nil {
		metrics.EventsProcessed.WithLabelValues(string(n.Canal), "error_redis").Inc()
		return fmt.Errorf("error al verificar idempotencia: %w", err)
	}

	if esDuplicado {
		slog.Info("Mensaje descartado por clave de idempotencia duplicada",
			slog.String("id", n.ID),
			slog.String("idempotency_key", n.IdempotencyKey),
		)
		metrics.EventsProcessed.WithLabelValues(string(n.Canal), "duplicate").Inc()
		return nil
	}

	backoff := d.initialBackoff

	for intento := 1; intento <= d.maxRetries; intento++ {
		if ctx.Err() != nil {
			metrics.EventsProcessed.WithLabelValues(string(n.Canal), "context_canceled").Inc()
			return ctx.Err()
		}

		slog.Info("Procesando envío de notificación",
			slog.String("id", n.ID),
			slog.Int("intento", intento),
			slog.Int("max_retries", d.maxRetries),
		)

		errEnvio := d.simularEnvio(n)
		if errEnvio == nil {
			n.MarcarEnviado()
			_ = d.idempotencyRepo.MarcarProcesado(ctx, n.IdempotencyKey, 24*time.Hour)

			slog.Info("Envío completado con éxito",
				slog.String("id", n.ID),
				slog.String("destinatario", n.Destinatario),
			)
			metrics.EventsProcessed.WithLabelValues(string(n.Canal), "success").Inc()
			return nil
		}

		_ = n.RegistrarFallo(d.maxRetries)
		slog.Warn("Fallo en intento de envío",
			slog.Int("intento", intento),
			slog.String("id", n.ID),
			slog.String("error", errEnvio.Error()),
			slog.Duration("espera", backoff),
		)

		select {
		case <-ctx.Done():
			metrics.EventsProcessed.WithLabelValues(string(n.Canal), "context_canceled_wait").Inc()
			return ctx.Err()
		case <-time.After(backoff):
		}

		backoff *= 2
	}

	metrics.EventsProcessed.WithLabelValues(string(n.Canal), "dlq_failed").Inc()
	return fmt.Errorf("se agotaron los %d reintentos para la notificación %s: %w", d.maxRetries, n.ID, domain.ErrMaxReintentosExcedido)
}

func (d *NotificationDispatcher) simularEnvio(n *domain.Notificacion) error {
	if n.Destinatario == "servidor.caido@error.com" {
		return errors.New("error 503: proveedor de mensajería no responde")
	}
	time.Sleep(200 * time.Millisecond)
	return nil
}

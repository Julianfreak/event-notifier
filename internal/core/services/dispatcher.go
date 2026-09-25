package services

import (
	"context"
	"errors"
	"fmt"
	"log"
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

// DespacharConResiliencia ejecuta la idempotencia y los reintentos con espera exponencial
func (d *NotificationDispatcher) DespacharConResiliencia(ctx context.Context, n *domain.Notificacion) error {
	// MÉTRICAS PROMETHEUS: Inicia el temporizador usando el canal (EMAIL/SMS/PUSH) como etiqueta
	timer := prometheus.NewTimer(metrics.ProcessingDuration.WithLabelValues(string(n.Canal)))
	defer timer.ObserveDuration()
	// 1. Verificación de Idempotencia en Redis
	esDuplicado, err := d.idempotencyRepo.EsDuplicado(ctx, n.IdempotencyKey)
	if err != nil {
		metrics.EventsProcessed.WithLabelValues(string(n.Canal), "error_redis").Inc()
		return fmt.Errorf("error al verificar idempotencia: %w", err)
	}

	if esDuplicado {
		log.Printf("[Dispatcher]: Mensaje %s descartado por clave de idempotencia duplicada (%s)", n.ID, n.IdempotencyKey)
		metrics.EventsProcessed.WithLabelValues(string(n.Canal), "duplicate").Inc()
		return nil // Se considera exitoso para dar Ack y no procesar de nuevo
	}

	// 2. Bucle de Reintentos con Exponential Backoff
	backoff := d.initialBackoff

	for intento := 1; intento <= d.maxRetries; intento++ {
		// Validamos si el contexto fue cancelado externamente antes de reintentar
		if ctx.Err() != nil {
			metrics.EventsProcessed.WithLabelValues(string(n.Canal), "context_canceled").Inc()
			return ctx.Err()
		}

		log.Printf("[Dispatcher]: Procesando envío para %s [Intento %d/%d]...", n.ID, intento, d.maxRetries)

		// Ejecutamos la simulación del envío
		errEnvio := d.simularEnvio(n)
		if errEnvio == nil {
			// Éxito: marcamos la notificación como enviada
			n.MarcarEnviado()

			// Registramos la clave en Redis para prevenir reprocesamiento futuro (TTL 24h)
			_ = d.idempotencyRepo.MarcarProcesado(ctx, n.IdempotencyKey, 24*time.Hour)

			log.Printf("[Dispatcher]: ¡Envío de %s completado con éxito a %s!", n.ID, n.Destinatario)
			metrics.EventsProcessed.WithLabelValues(string(n.Canal), "success").Inc()
			return nil
		}

		// Si falla, registramos el intento en la entidad de dominio
		_ = n.RegistrarFallo(d.maxRetries)
		log.Printf("[Dispatcher]: Fallo en intento %d de %s: %v. Esperando %v antes de reintentar...",
			intento, n.ID, errEnvio, backoff)

		// Pausamos la ejecución con select para responder si el contexto se cancela durante la espera
		select {
		case <-ctx.Done():
			metrics.EventsProcessed.WithLabelValues(string(n.Canal), "context_canceled_wait").Inc()
			return ctx.Err()
		case <-time.After(backoff):
		}

		// Duplicamos el tiempo para el próximo reintento (1s -> 2s -> 4s...)
		backoff *= 2
	}

	metrics.EventsProcessed.WithLabelValues(string(n.Canal), "dlq_failed").Inc()
	// Si se agotaron todos los intentos, retornamos el error para que RabbitMQ lo mande a la DLQ
	return fmt.Errorf("se agotaron los %d reintentos para la notificación %s: %w", d.maxRetries, n.ID, domain.ErrMaxReintentosExcedido)
}

// simularEnvio simula la llamada a un proveedor externo (e-mail, SMS, etc.)
func (d *NotificationDispatcher) simularEnvio(n *domain.Notificacion) error {
	// Regla de simulación didáctica:
	// Si el destinatario contiene la palabra 'error', simulamos que el proveedor externo está caído
	if n.Destinatario == "servidor.caido@error.com" {
		return errors.New("error 503: proveedor de mensajería no responde")
	}

	// Simulación de latencia normal de envío
	time.Sleep(200 * time.Millisecond)
	return nil
}

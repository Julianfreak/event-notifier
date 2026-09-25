package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Mide la cantidad total de notificaciones procesadas, separadas por estado (éxito/error) y canal (email/sms/push).
	EventsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "event_notifier_processed_total",
			Help: "Número total de eventos procesados",
		},
		[]string{"channel", "status"},
	)

	// Mide el tiempo que toma procesar un mensaje completo (ideal para detectar latencia en servicios externos).
	ProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "event_notifier_processing_duration_seconds",
			Help:    "Duración del procesamiento de eventos",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"channel"},
	)

	// Mide cuántas goroutines (workers) están activas en tiempo real.
	ActiveWorkers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "event_notifier_active_workers",
			Help: "Número actual de workers procesando mensajes",
		},
	)
)

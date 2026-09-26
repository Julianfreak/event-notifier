package services

import (
	"log/slog"
	"sync"
	"time"

	"event-notifier/internal/adapters/metrics"
	"event-notifier/internal/core/domain"
)

type Pool struct {
	numWorkers int
	tareas     chan *domain.Notificacion
	wg         sync.WaitGroup
}

func NewPool(numWorkers int, bufferSize int) *Pool {
	return &Pool{
		numWorkers: numWorkers,
		tareas:     make(chan *domain.Notificacion, bufferSize),
	}
}

func (p *Pool) Iniciar() {
	for w := 1; w <= p.numWorkers; w++ {
		p.wg.Add(1)

		go func(workerID int) {
			defer p.wg.Done()
			metrics.ActiveWorkers.Inc()
			defer metrics.ActiveWorkers.Dec()

			for notificacion := range p.tareas {
				slog.Info("Procesando notificación",
					slog.Int("worker_id", workerID),
					slog.String("id", notificacion.ID),
					slog.String("destinatario", notificacion.Destinatario),
					slog.String("canal", string(notificacion.Canal)),
				)

				time.Sleep(300 * time.Millisecond)

				slog.Info("Notificación enviada con éxito",
					slog.Int("worker_id", workerID),
					slog.String("id", notificacion.ID),
				)
			}
		}(w)
	}
}

func (p *Pool) Encolar(n *domain.Notificacion) {
	p.tareas <- n
}
func (p *Pool) Detener() {
	close(p.tareas)
	p.wg.Wait()
}

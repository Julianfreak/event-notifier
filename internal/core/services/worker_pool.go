package services

import (
	"fmt"
	"sync"
	"time"

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
		// 1. Incrementamos el contador del WaitGroup por cada trabajador
		p.wg.Add(1)

		// 2. Disparamos cada worker en su propia GOROUTINE en segundo plano
		go func(workerID int) {
			defer p.wg.Done() // Notifica al terminar de procesar todo el canal

			// Cada worker extrae concurrentemente del mismo canal compartido
			for notificacion := range p.tareas {
				fmt.Printf("[Worker %d]: Procesando notificación ID: %s para %s (Canal: %s)\n",
					workerID, notificacion.ID, notificacion.Destinatario, notificacion.Canal)

				time.Sleep(300 * time.Millisecond) // Simula la llamada de red/envío

				fmt.Printf("[Worker %d]: ¡Notificación ID: %s enviada con éxito!\n",
					workerID, notificacion.ID)
			}
		}(w) // Pasamos 'w' como parámetro a la función anónima
	}
}

func (p *Pool) Encolar(n *domain.Notificacion) {
	p.tareas <- n
}
func (p *Pool) Detener() {
	close(p.tareas)
	p.wg.Wait()
}

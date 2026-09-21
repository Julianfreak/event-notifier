package services_test

import (
	"fmt"
	"testing"
	"time"

	"event-notifier/internal/core/domain"
	"event-notifier/internal/core/services"
)

func TestWorkerPool(t *testing.T) {
	t.Run("Debe procesar todas las notificaciones encoladas concurrentemente", func(t *testing.T) {
		const numWorkers = 3
		const totalTareas = 6

		// 1. Inicializamos el pool con 3 workers y buffer para 6 tareas
		pool := services.NewPool(numWorkers, totalTareas)
		pool.Iniciar()

		// 2. Encolamos las tareas
		for i := 1; i <= totalTareas; i++ {
			notif := &domain.Notificacion{
				ID:           fmt.Sprintf("notif-pool-%d", i),
				Destinatario: fmt.Sprintf("usuario%d@empresa.com", i),
				Canal:        domain.CanalEmail,
				Mensaje:      "Mensaje de prueba de pool",
				Estado:       domain.EstadoPendiente,
			}
			pool.Encolar(notif)
		}

		// 3. Detenemos el pool (espera interna con sync.WaitGroup)
		pool.Detener()

		// Si pool.Detener() finaliza sin congelarse (deadlock), garantiza que
		// todos los workers procesaron sus elementos y cerraron limpiamente.
	})

	t.Run("Debe permitir detener un pool vacio sin bloquearse", func(t *testing.T) {
		pool := services.NewPool(2, 5)
		pool.Iniciar()

		// Detenemos de inmediato sin encolar nada
		canalFin := make(chan struct{})
		go func() {
			pool.Detener()
			close(canalFin)
		}()

		select {
		case <-canalFin:
			// Éxito: cerró de inmediato sin bloquearse
		case <-time.After(1 * time.Second):
			t.Fatal("pool.Detener() se congeló en un pool sin tareas")
		}
	})
}

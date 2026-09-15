package main

import (
	"fmt"
	"time"

	"event-notifier/internal/core/domain"
	"event-notifier/internal/core/services"
)

func main() {
	fmt.Println("Iniciando Worker Pool de Notificaciones...")

	// Creamos un pool de 3 trabajadores con un búfer de 10 tareas
	pool := services.NewPool(3, 10)
	pool.Iniciar()

	// Simulamos la llegada de 6 notificaciones masivas
	destinatarios := []string{"ana@email.com", "carlos@email.com", "pedro@email.com", "+573001234567", "sofia@email.com", "+573109876543"}

	for i, dest := range destinatarios {
		notif := &domain.Notificacion{
			ID:           fmt.Sprintf("notif-%d", i+1),
			Canal:        domain.CanalEmail,
			Destinatario: dest,
			Mensaje:      "Tu código de verificación es 123456",
			Estado:       domain.EstadoPendiente,
			CreatedAt:    time.Now(),
		}
		pool.Encolar(notif)
	}

	fmt.Println("--> Todas las notificaciones fueron encoladas. Esperando finalización...")

	// Detenemos el pool de forma limpia
	pool.Detener()

	fmt.Println("--> Todas las tareas fueron procesadas concurrentemente. Apagado limpio exitoso.")
}

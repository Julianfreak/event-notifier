package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"event-notifier/internal/adapters/broker"
	"event-notifier/internal/adapters/cache"
	"event-notifier/internal/core/domain"
	"event-notifier/internal/core/services"
)

func main() {
	fmt.Println("==========================================================")
	fmt.Println("  Event Notifier - Resiliencia, DLQ y Graceful Shutdown   ")
	fmt.Println("==========================================================")

	// 1. Creamos un contexto raíz con cancelación para el apagado limpio
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Conectamos con Redis (localhost:6379)
	redisRepo, err := cache.NewRedisIdempotencyStorage("localhost:6379", "", 0)
	if err != nil {
		log.Fatalf("Error conectando con Redis: %v", err)
	}
	fmt.Println("[Redis]: Conexión establecida exitosamente.")

	// 3. Conectamos con RabbitMQ e inicializamos Colas, DLX y DLQ
	amqpURL := "amqp://guest:guest@localhost:5672/"
	exchange := "notificaciones_exchange"
	queue := "cola_notificaciones"
	routingKey := "notificaciones.envio"

	rabbitAdapter, err := broker.NewRabbitMQAdapter(amqpURL, exchange, queue, routingKey)
	if err != nil {
		log.Fatalf("Error inicializando RabbitMQ: %v", err)
	}
	defer rabbitAdapter.Cerrar()
	fmt.Println("[RabbitMQ]: Conexión, Colas durables y Dead Letter Queue listas.")

	// 4. Inicializamos el Despachador de Negocio (3 reintentos, espera inicial 1s)
	dispatcher := services.NewNotificationDispatcher(redisRepo, 3, 1*time.Second)

	// 5. Canal del sistema operativo para capturar señales de apagado (SIGINT / SIGTERM)
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	// WaitGroup para coordinar que el consumidor termine antes de salir
	var wg sync.WaitGroup

	// 6. Arrancamos el Consumidor en una Goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := rabbitAdapter.IniciarConsumo(ctx, func(n *domain.Notificacion) error {
			// Toda la lógica de reintentos y desvío a DLQ ocurre dentro del dispatcher
			return dispatcher.DespacharConResiliencia(ctx, n)
		})
		if err != nil {
			log.Printf("[Consumidor]: Finalizado: %v", err)
		}
	}()

	// 7. Publicamos dos eventos de prueba:
	// Evento A: Mensaje normal que debe procesarse con éxito.
	// Evento B: Mensaje con falla simulada que agotará reintentos y viajará a la DLQ.
	time.Sleep(500 * time.Millisecond)
	fmt.Println("\n[Productor]: Publicando eventos de prueba...")

	eventoExitoso := &domain.Notificacion{
		ID:             "notif-201",
		IdempotencyKey: "orden-exitosa-001",
		Canal:          domain.CanalEmail,
		Destinatario:   "usuario.valido@empresa.com",
		Mensaje:        "Su pedido #201 está en camino.",
		Estado:         domain.EstadoPendiente,
	}

	eventoFallido := &domain.Notificacion{
		ID:             "notif-202",
		IdempotencyKey: "orden-fallida-002",
		Canal:          domain.CanalEmail,
		Destinatario:   "servidor.caido@error.com", // Dispara error 503
		Mensaje:        "Este mensaje probará el desvío a la DLQ.",
		Estado:         domain.EstadoPendiente,
	}

	_ = rabbitAdapter.Publicar(ctx, eventoExitoso)
	fmt.Println("    -> Evento normal (notif-201) publicado.")

	_ = rabbitAdapter.Publicar(ctx, eventoFallido)
	fmt.Println("    -> Evento con falla simulada (notif-202) publicado.")

	fmt.Println("\n[Sistema]: En ejecución. Presiona Ctrl+C en cualquier momento para probar el Graceful Shutdown.\n")

	// 8. Esperamos la señal de apagado del sistema operativo (Ctrl+C o SIGTERM)
	<-stopChan
	fmt.Println("\n[Apagado]: Señal recibida. Iniciando Graceful Shutdown...")

	// 9. Cancelamos el contexto para detener nuevos consumos y esperas de backoff
	cancel()

	// 10. Esperamos a que los workers en vuelo finalicen ordenadamente
	wg.Wait()
	fmt.Println("[Apagado]: Todos los procesos finalizaron de forma limpia. Saliendo sin pérdida de datos.")
}

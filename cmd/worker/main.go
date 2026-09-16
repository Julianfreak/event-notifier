package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"event-notifier/internal/adapters/broker"
	"event-notifier/internal/adapters/cache"
	"event-notifier/internal/core/domain"
)

func main() {
	fmt.Println("==================================================")
	fmt.Println("  Event Notifier - Prueba RabbitMQ + Redis")
	fmt.Println("==================================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Conectamos con Redis (localhost:6379)
	redisRepo, err := cache.NewRedisIdempotencyStorage("localhost:6379", "", 0)
	if err != nil {
		log.Fatalf("Error al conectar con Redis: %v", err)
	}
	fmt.Println("[Redis]: Conectado exitosamente.")

	// 2. Conectamos con RabbitMQ (localhost:5672)
	amqpURL := "amqp://guest:guest@localhost:5672/"
	exchange := "notificaciones_exchange"
	queue := "cola_notificaciones"
	routingKey := "notificaciones.envio"

	rabbitAdapter, err := broker.NewRabbitMQAdapter(amqpURL, exchange, queue, routingKey)
	if err != nil {
		log.Fatalf("Error al inicializar RabbitMQ: %v", err)
	}
	defer rabbitAdapter.Cerrar()
	fmt.Println("[RabbitMQ]: Conexión, Exchange y Cola inicializados exitosamente.")

	// 3. Iniciamos el Consumidor en segundo plano (Goroutine)
	go func() {
		err := rabbitAdapter.IniciarConsumo(ctx, func(n *domain.Notificacion) error {
			fmt.Printf("\n--> [Worker]: Mensaje recibido de RabbitMQ. ID: %s | Destinatario: %s\n", n.ID, n.Destinatario)

			// Verificamos idempotencia en Redis
			esDuplicado, err := redisRepo.EsDuplicado(ctx, n.IdempotencyKey)
			if err != nil {
				return fmt.Errorf("error consultando Redis: %w", err)
			}

			if esDuplicado {
				fmt.Printf("    [Idempotencia]: Notificación %s con clave '%s' ya fue procesada. DESCARTANDO.\n", n.ID, n.IdempotencyKey)
				return nil // Retornamos nil para dar Ack a RabbitMQ y quitar el duplicado de la cola
			}

			// Marcamos la clave como procesada en Redis con TTL de 1 hora
			if err := redisRepo.MarcarProcesado(ctx, n.IdempotencyKey, 1*time.Hour); err != nil {
				return fmt.Errorf("error registrando idempotencia: %w", err)
			}

			// Simulamos el envío del mensaje (Email / SMS)
			time.Sleep(500 * time.Millisecond)
			n.MarcarEnviado()
			fmt.Printf("    [Éxito]: Notificación %s enviada por canal %s a %s. Estado: %s\n",
				n.ID, n.Canal, n.Destinatario, n.Estado)

			return nil // Retorna nil para que RabbitMQ ejecute Ack(false)
		})
		if err != nil {
			log.Printf("Consumidor finalizado con error: %v", err)
		}
	}()

	// Damos 1 segundo para que el consumidor se registre en la cola
	time.Sleep(1 * time.Second)

	// 4. Publicamos 3 Notificaciones al Broker
	fmt.Println("\n[Productor]: Publicando 3 eventos hacia RabbitMQ...")

	eventos := []*domain.Notificacion{
		{
			ID:             "notif-101",
			IdempotencyKey: "pago-transaccion-001", // Clave A
			Canal:          domain.CanalEmail,
			Destinatario:   "ana.gomez@empresa.com",
			Mensaje:        "Su transferencia de $500 ha sido exitosa.",
			Estado:         domain.EstadoPendiente,
		},
		{
			ID:             "notif-102",
			IdempotencyKey: "pago-transaccion-002", // Clave B
			Canal:          domain.CanalSMS,
			Destinatario:   "+573001234567",
			Mensaje:        "Código de seguridad: 849201.",
			Estado:         domain.EstadoPendiente,
		},
		{
			ID:             "notif-103",
			IdempotencyKey: "pago-transaccion-001", //¡CLAVE A REPETIDA! Simula reenvío
			Canal:          domain.CanalEmail,
			Destinatario:   "ana.gomez@empresa.com",
			Mensaje:        "Su transferencia de $500 ha sido exitosa.",
			Estado:         domain.EstadoPendiente,
		},
	}

	for _, evento := range eventos {
		if err := rabbitAdapter.Publicar(ctx, evento); err != nil {
			log.Fatalf("Error publicando notificación: %v", err)
		}
		fmt.Printf("    -> Notificación %s publicada en el Exchange.\n", evento.ID)
	}

	// Esperamos 4 segundos para que el Worker procese los mensajes
	time.Sleep(4 * time.Second)
	fmt.Println("\n[Main]: Prueba completada exitosamente. Cerrando sistema.")
}

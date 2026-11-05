package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"event-notifier/internal/adapters/broker"
	httpadapter "event-notifier/internal/adapters/http"

	"event-notifier/internal/adapters/cache"
	"event-notifier/internal/middleware"
)

func main() {
	log.Println("==========================================================")
	log.Println("   Event Notifier - API REST con Health & Readiness       ")
	log.Println("==========================================================")

	amqpURL := os.Getenv("RABBITMQ_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}

	exchange := "notificaciones_exchange"
	queue := "cola_notificaciones"
	routingKey := "notificaciones.envio"

	rabbitAdapter, err := broker.NewRabbitMQAdapter(amqpURL, exchange, queue, routingKey)
	if err != nil {
		log.Fatalf("Error conectando con RabbitMQ desde la API: %v", err)
	}
	defer rabbitAdapter.Cerrar()

	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "event_notifier_redis:6379"
	}

	// Instanciamos el adaptador de Redis (ajusta el constructor según tu implementación en internal/adapters/cache)
	redisAdapter, err := cache.NewRedisIdempotencyStorage(redisAddr, "", 0)
	if err != nil {
		log.Fatalf("Error conectando con Redis desde la API: %v", err)
	}
	handler := httpadapter.NewNotificationHandler(rabbitAdapter)

	mux := http.NewServeMux()

	// 1. Ruta principal para enviar notificaciones
	mux.HandleFunc("POST /api/v1/notifications", handler.HandleSendNotification)

	// 2. Liveness Probe (/healthz)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"alive","service":"event-notifier-api"}`))
	})

	// Readiness Probe (/readyz) - Verifica RabbitMQ y Redis
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		w.Header().Set("Content-Type", "application/json")

		// 1. Verificar RabbitMQ
		if err := rabbitAdapter.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]any{
				"status": "not ready",
				"error":  "RabbitMQ no disponible: " + err.Error(),
			})
			return
		}

		// 2. Verificar Redis
		if err := redisAdapter.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]any{
				"status": "not ready",
				"error":  "Redis no disponible: " + err.Error(),
			})
			return
		}

		// Si ambas dependencias responden correctamente
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"status":   "ready",
			"rabbitmq": "connected",
			"redis":    "connected",
		})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("[HTTP Server]: API REST escuchando en el puerto %s...\n", port)
	handlerWithMiddleware := middleware.RequestIDMiddleware(mux)

	if err := http.ListenAndServe(":"+port, handlerWithMiddleware); err != nil {
		log.Fatalf("Error en el servidor HTTP: %v", err)
	}
}

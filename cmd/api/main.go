package main

import (
	"log"
	"net/http"
	"os"

	"event-notifier/internal/adapters/broker"
	httpadapter "event-notifier/internal/adapters/http"
)

func main() {
	log.Println("==========================================================")
	log.Println("   Event Notifier - API REST Adaptador de Entrada         ")
	log.Println("==========================================================")

	// 1. Conectar con RabbitMQ usando las variables de entorno
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
	log.Println("[RabbitMQ]: Conexión establecida para la API REST.")

	// 2. Inicializar Handler HTTP inyectando el adaptador de RabbitMQ
	handler := httpadapter.NewNotificationHandler(rabbitAdapter)

	// 3. Configurar Rutas con el enrutador estándar de Go
	mux := http.NewServeMux()

	// Endpoint principal para enviar notificaciones
	mux.HandleFunc("POST /api/v1/notifications", handler.HandleSendNotification)

	// Health check básico de la API
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"API REST funcionando correctamente"}`))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("[HTTP Server]: API REST escuchando en el puerto %s...\n", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Error en el servidor HTTP: %v", err)
	}
}

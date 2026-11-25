package httpadapter

import (
	"encoding/json"
	"net/http"
	"time"

	"event-notifier/internal/core/domain"
	"event-notifier/internal/core/ports"
)

type NotificationHandler struct {
	publisher ports.EventPublisher
}

func NewNotificationHandler(publisher ports.EventPublisher) *NotificationHandler {
	return &NotificationHandler{publisher: publisher}
}

// NotificationRequest define el JSON que recibirá el cliente (ej. Insomnia)
type NotificationRequest struct {
	Destinatario   string                   `json:"destinatario"`
	Canal          domain.CanalNotificacion `json:"canal"`
	Mensaje        string                   `json:"mensaje"`
	IdempotencyKey string                   `json:"idempotency_key"`
}

func (h *NotificationHandler) HandleSendNotification(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	var req NotificationRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Payload JSON inválido: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Validaciones básicas
	if req.Destinatario == "" || req.Mensaje == "" || req.Canal == "" {
		http.Error(w, "Los campos 'destinatario', 'canal' y 'mensaje' son obligatorios", http.StatusBadRequest)
		return
	}

	// Si el cliente no envía una IdempotencyKey, podemos generar una por defecto
	idempotencyKey := req.IdempotencyKey
	if idempotencyKey == "" {
		idempotencyKey = generateUUID()
	}

	// Construir la entidad de dominio usando tu estructura exacta
	notification := domain.Notificacion{
		ID:             generateUUID(),
		IdempotencyKey: idempotencyKey,
		Canal:          req.Canal,
		Destinatario:   req.Destinatario,
		Mensaje:        req.Mensaje,
		Estado:         domain.EstadoPendiente,
		Intentos:       0,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	// Publicar el evento al Broker (RabbitMQ)
	err = h.publisher.Publicar(r.Context(), &notification)
	if err != nil {
		http.Error(w, "Error al encolar la notificación: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Responder con éxito (202 Accepted - Procesamiento asíncrono)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	response := map[string]any{
		"status":  "success",
		"message": "Notificación encolada correctamente para procesamiento asíncrono",
		"data":    notification,
	}

	json.NewEncoder(w).Encode(response)
}

func generateUUID() string {
	return time.Now().Format("20060102150405.000000") + "-event"
}

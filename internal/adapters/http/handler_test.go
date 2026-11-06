package httpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"event-notifier/internal/core/domain"
)

// mockPublisher simula el comportamiento de ports.EventPublisher para pruebas unitarias
type mockPublisher struct {
	err error
}

func (m *mockPublisher) Publicar(ctx context.Context, n *domain.Notificacion) error {
	return m.err
}

func TestNotificationHandler_HandleSendNotification(t *testing.T) {
	t.Run("Debe rechazar solicitudes con métodos distintos a POST", func(t *testing.T) {
		handler := NewNotificationHandler(&mockPublisher{})
		req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications", nil)
		rec := httptest.NewRecorder()

		handler.HandleSendNotification(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("Se esperaba estado %d, se obtuvo %d", http.StatusMethodNotAllowed, rec.Code)
		}
		//*Verification:* La respuesta HTTP retorna código 405 Method Not Allowed.
	})

	t.Run("Debe retornar 400 Bad Request si el JSON es inválido o malformado", func(t *testing.T) {
		handler := NewNotificationHandler(&mockPublisher{})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBuffer([]byte("{invalid-json")))
		rec := httptest.NewRecorder()

		handler.HandleSendNotification(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Se esperaba estado %d, se obtuvo %d", http.StatusBadRequest, rec.Code)
		}
		//*Verification:* La respuesta HTTP retorna código 400 Bad Request.
	})

	t.Run("Debe retornar 400 Bad Request si faltan campos obligatorios", func(t *testing.T) {
		handler := NewNotificationHandler(&mockPublisher{})
		body, _ := json.Marshal(NotificationRequest{
			Destinatario: "",
			Mensaje:      "Mensaje de prueba",
			Canal:        domain.CanalEmail,
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBuffer(body))
		rec := httptest.NewRecorder()

		handler.HandleSendNotification(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Se esperaba estado %d, se obtuvo %d", http.StatusBadRequest, rec.Code)
		}
		//*Verification:* Valida que los campos requeridos no estén vacíos y rechaza con 400.
	})

	t.Run("Debe retornar 500 Internal Server Error si falla la publicación en el broker", func(t *testing.T) {
		handler := NewNotificationHandler(&mockPublisher{err: errors.New("error de conexión con broker")})
		body, _ := json.Marshal(NotificationRequest{
			Destinatario: "cliente@empresa.com",
			Mensaje:      "Mensaje de prueba",
			Canal:        domain.CanalEmail,
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBuffer(body))
		rec := httptest.NewRecorder()

		handler.HandleSendNotification(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("Se esperaba estado %d, se obtuvo %d", http.StatusInternalServerError, rec.Code)
		}
		//*Verification:* Si el publicador retorna un error, el handler responde con código 500.
	})

	t.Run("Debe procesar la notificación exitosamente y retornar 202 Accepted", func(t *testing.T) {
		handler := NewNotificationHandler(&mockPublisher{err: nil})
		body, _ := json.Marshal(NotificationRequest{
			Destinatario:   "cliente@empresa.com",
			Mensaje:        "Mensaje de prueba",
			Canal:          domain.CanalEmail,
			IdempotencyKey: "key-12345",
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBuffer(body))
		rec := httptest.NewRecorder()

		handler.HandleSendNotification(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Errorf("Se esperaba estado %d, se obtuvo %d", http.StatusAccepted, rec.Code)
		}

		var resp map[string]any
		json.NewDecoder(rec.Body).Decode(&resp)
		if resp["status"] != "success" {
			t.Errorf("Se esperaba status 'success', se obtuvo %v", resp["status"])
		}
		//*Verification:* El handler responde con 202 Accepted y el JSON de confirmación de éxito.
	})
}

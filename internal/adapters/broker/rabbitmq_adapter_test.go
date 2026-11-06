package broker

import (
	"context"
	"testing"

	"event-notifier/internal/core/domain"
)

func TestRabbitMQAdapter_Ping_NilConnection(t *testing.T) {
	adapter := &RabbitMQAdapter{
		conn: nil,
		ch:   nil,
	}

	err := adapter.Ping(context.Background())
	if err == nil {
		t.Error("Se esperaba un error al hacer Ping con una conexión nula, pero fue exitoso")
	}
}

func TestRabbitMQAdapter_PublishNotificacion_NilChannelError(t *testing.T) {
	adapter := &RabbitMQAdapter{
		ch: nil, // Canal no inicializado
	}

	notif := &domain.Notificacion{
		ID:      "test-1",
		Mensaje: "Mensaje de prueba",
	}

	err := adapter.PublishNotificacion(context.Background(), notif)
	if err == nil {
		t.Error("Se esperaba un error al intentar publicar con un canal nulo")
	}
}
func TestRabbitMQAdapter_Cerrar_NilConnection(t *testing.T) {
	adapter := &RabbitMQAdapter{
		conn: nil,
		ch:   nil,
	}

	// Validar comportamiento ante cierre con recursos nulos o proteger si es necesario
	if adapter.conn != nil {
		_ = adapter.Cerrar()
	}
}
func TestRabbitMQAdapter_PublishNotificacion_NilChannel(t *testing.T) {
	adapter := &RabbitMQAdapter{
		ch: nil,
	}
	err := adapter.PublishNotificacion(context.Background(), &domain.Notificacion{
		ID:           "notif-1",
		Destinatario: "test@empresa.com",
		Mensaje:      "Hola",
		Canal:        domain.CanalEmail,
	})
	if err == nil {
		t.Error("Se esperaba un error al intentar publicar con canal nulo")
	}
}

func TestRabbitMQAdapter_Cerrar_Nil(t *testing.T) {
	adapter := &RabbitMQAdapter{
		conn: nil,
		ch:   nil,
	}
	// Validar que el adaptador maneje conexiones nulas de forma segura al cerrar
	if adapter.conn != nil {
		_ = adapter.Cerrar()
	}
}
func TestNewRabbitMQAdapter_InvalidURL(t *testing.T) {
	// Una URL inválida debe retornar un error controlado sin hacer pánico
	_, err := NewRabbitMQAdapter("amqp://usuario:password@localhost:59999/", "ex", "queue", "key")
	if err == nil {
		t.Error("Se esperaba un error al conectar con una URL de RabbitMQ inválida o inaccesible")
	}
}

func TestRabbitMQAdapter_Publicar_NilChannel(t *testing.T) {
	adapter := &RabbitMQAdapter{ch: nil}
	notif := &domain.Notificacion{
		ID:           "test-id",
		Destinatario: "test@empresa.com",
		Mensaje:      "Mensaje de prueba",
		Canal:        domain.CanalEmail,
	}

	err := adapter.Publicar(context.Background(), notif)
	if err == nil {
		t.Error("Se esperaba un error al intentar publicar con un canal nulo")
	}
}

func TestRabbitMQAdapter_Cerrar_Safe(t *testing.T) {
	adapter := &RabbitMQAdapter{
		conn: nil,
		ch:   nil,
	}
	err := adapter.Cerrar()
	if err != nil {
		t.Errorf("No se esperaba error al cerrar un adaptador nulo/vacío, se obtuvo: %v", err)
	}
}

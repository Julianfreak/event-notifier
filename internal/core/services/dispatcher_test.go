package services_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"event-notifier/internal/core/domain"
	"event-notifier/internal/core/services"
)

// ==========================================================
// 1. MOCK DE IDEMPOTENCIA (Simulador de Redis)
// ==========================================================

type MockIdempotencyRepo struct {
	esDuplicadoResp        bool
	esDuplicadoErr         error
	marcarProcesadoErr     error
	marcarProcesadoLlamado bool
	claveMarcada           string
}

func (m *MockIdempotencyRepo) EsDuplicado(ctx context.Context, key string) (bool, error) {
	if m.esDuplicadoErr != nil {
		return false, m.esDuplicadoErr
	}
	return m.esDuplicadoResp, nil
}

func (m *MockIdempotencyRepo) MarcarProcesado(ctx context.Context, key string, ttl time.Duration) error {
	m.marcarProcesadoLlamado = true
	m.claveMarcada = key
	return m.marcarProcesadoErr
}

// ==========================================================
// 2. SUITE DE PRUEBAS UNITARIAS PARA EL DESPACHADOR
// ==========================================================

func TestDespacharConResiliencia(t *testing.T) {

	// CASO 1: Idempotencia - Mensaje duplicado detectado en Redis
	t.Run("Debe descartar mensaje duplicado sin reintentar ni marcar", func(t *testing.T) {
		mockRepo := &MockIdempotencyRepo{
			esDuplicadoResp: true, // Simula que la clave ya existe en Redis
		}

		// Usamos 1ms de backoff para velocidad instantánea
		dispatcher := services.NewNotificationDispatcher(mockRepo, 3, 1*time.Millisecond)

		notif := &domain.Notificacion{
			ID:             "notif-dup-01",
			IdempotencyKey: "pago-100",
			Destinatario:   "usuario@empresa.com",
			Canal:          domain.CanalEmail,
			Estado:         domain.EstadoPendiente,
		}

		err := dispatcher.DespacharConResiliencia(context.Background(), notif)

		// 1. No debe arrojar error (debe dar Ack a RabbitMQ)
		if err != nil {
			t.Fatalf("No se esperaba error en descarte por duplicado, obtenido: %v", err)
		}
		// 2. No debe haber intentado guardar en Redis otra vez
		if mockRepo.marcarProcesadoLlamado {
			t.Errorf("No debía llamarse a MarcarProcesado para un duplicado")
		}
		// 3. El estado de la entidad no debe haber cambiado a ENVIADO
		if notif.Estado == domain.EstadoEnviado {
			t.Errorf("El estado de la notificación no debió cambiar a ENVIADO")
		}
	})

	// CASO 2: Envío Exitoso en el primer intento
	t.Run("Debe procesar exitosamente y registrar la clave en Redis", func(t *testing.T) {
		mockRepo := &MockIdempotencyRepo{
			esDuplicadoResp: false, // Mensaje nuevo
		}

		dispatcher := services.NewNotificationDispatcher(mockRepo, 3, 1*time.Millisecond)

		notif := &domain.Notificacion{
			ID:             "notif-ok-01",
			IdempotencyKey: "pago-200",
			Destinatario:   "cliente.valido@empresa.com",
			Canal:          domain.CanalEmail,
			Estado:         domain.EstadoPendiente,
		}

		err := dispatcher.DespacharConResiliencia(context.Background(), notif)

		if err != nil {
			t.Fatalf("Se esperaba éxito pero ocurrió error: %v", err)
		}
		if notif.Estado != domain.EstadoEnviado {
			t.Errorf("Estado esperado ENVIADO, obtenido: %s", notif.Estado)
		}
		if !mockRepo.marcarProcesadoLlamado {
			t.Errorf("Debió registrarse la clave de idempotencia en Redis")
		}
		if mockRepo.claveMarcada != "pago-200" {
			t.Errorf("Clave guardada en Redis esperada 'pago-200', obtenida: %s", mockRepo.claveMarcada)
		}
	})

	// CASO 3: Agotamiento de reintentos (Falla persistente y desvío a DLQ)
	t.Run("Debe agotar reintentos con backoff y retornar ErrMaxReintentosExcedido", func(t *testing.T) {
		mockRepo := &MockIdempotencyRepo{
			esDuplicadoResp: false,
		}

		// 3 reintentos con 1ms de separación
		dispatcher := services.NewNotificationDispatcher(mockRepo, 3, 1*time.Millisecond)

		notif := &domain.Notificacion{
			ID:             "notif-fallo-01",
			IdempotencyKey: "pago-error-300",
			Destinatario:   "servidor.caido@error.com", // Dispara el error simulado en dispatcher
			Canal:          domain.CanalEmail,
			Estado:         domain.EstadoPendiente,
		}

		err := dispatcher.DespacharConResiliencia(context.Background(), notif)

		// Verificamos que el error final contenga la causa de dominio
		if !errors.Is(err, domain.ErrMaxReintentosExcedido) {
			t.Errorf("Se esperaba error '%v', obtenido: '%v'", domain.ErrMaxReintentosExcedido, err)
		}
		// Verificamos que la entidad haya registrado los 3 intentos fallidos
		if notif.Intentos != 3 {
			t.Errorf("Se esperaban 3 intentos registrados, obtenidos: %d", notif.Intentos)
		}
		// El estado final de la entidad debe ser FALLIDO
		if notif.Estado != domain.EstadoFallido {
			t.Errorf("Estado esperado FALLIDO, obtenido: %s", notif.Estado)
		}
		// No debió marcarse como procesado exitosamente en Redis
		if mockRepo.marcarProcesadoLlamado {
			t.Errorf("Un mensaje fallido no debe ser marcado como procesado exitoso en Redis")
		}
	})

	// CASO 4: Cancelación de Contexto (Graceful Shutdown durante la espera)
	t.Run("Debe interrumpir reintentos inmediatamente si el contexto es cancelado", func(t *testing.T) {
		mockRepo := &MockIdempotencyRepo{
			esDuplicadoResp: false,
		}

		// Configuramos un backoff largo (500ms) para comprobar la interrupción inmediata
		dispatcher := services.NewNotificationDispatcher(mockRepo, 5, 500*time.Millisecond)

		// Creamos un contexto cancelable y lo cancelamos de inmediato
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancelado antes o durante el intento

		notif := &domain.Notificacion{
			ID:             "notif-cancel-01",
			IdempotencyKey: "pago-cancel-400",
			Destinatario:   "servidor.caido@error.com",
			Canal:          domain.CanalEmail,
			Estado:         domain.EstadoPendiente,
		}

		err := dispatcher.DespacharConResiliencia(ctx, notif)

		// Debe retornar context.Canceled sin esperar los 500ms
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Se esperaba error 'context.Canceled', obtenido: '%v'", err)
		}
	})
}

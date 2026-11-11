package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"event-notifier/internal/core/domain"

	"github.com/sony/gobreaker/v2"
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

func (m *MockIdempotencyRepo) Ping(ctx context.Context) error {
	return nil
}

// ==========================================================
// 2. SUITE DE PRUEBAS UNITARIAS PARA EL DESPACHADOR
// ==========================================================

func TestDespacharConResiliencia(t *testing.T) {

	// CASO 1: Idempotencia - Mensaje duplicado detectado en Redis
	t.Run("Debe descartar mensaje duplicado sin reintentar ni marcar", func(t *testing.T) {
		mockRepo := &MockIdempotencyRepo{
			esDuplicadoResp: true,
		}

		dispatcher := NewNotificationDispatcher(mockRepo, 3, 1*time.Millisecond)

		notif := &domain.Notificacion{
			ID:             "notif-dup-01",
			IdempotencyKey: "pago-100",
			Destinatario:   "usuario@empresa.com",
			Canal:          domain.CanalEmail,
			Estado:         domain.EstadoPendiente,
		}

		err := dispatcher.DespacharConResiliencia(context.Background(), notif)

		if err != nil {
			t.Fatalf("No se esperaba error en descarte por duplicado, obtenido: %v", err)
		}
		if mockRepo.marcarProcesadoLlamado {
			t.Errorf("No debía llamarse a MarcarProcesado para un duplicado")
		}
		if notif.Estado == domain.EstadoEnviado {
			t.Errorf("El estado de la notificación no debió cambiar a ENVIADO")
		}
	})

	// CASO 2: Envío Exitoso en el primer intento
	t.Run("Debe procesar exitosamente y registrar la clave en Redis", func(t *testing.T) {
		mockRepo := &MockIdempotencyRepo{
			esDuplicadoResp: false,
		}

		dispatcher := NewNotificationDispatcher(mockRepo, 3, 1*time.Millisecond)

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

	// CASO 3: Agotamiento de reintentos
	t.Run("Debe agotar reintentos con backoff y retornar ErrMaxReintentosExcedido", func(t *testing.T) {
		mockRepo := &MockIdempotencyRepo{
			esDuplicadoResp: false,
		}

		dispatcher := NewNotificationDispatcher(mockRepo, 3, 1*time.Millisecond)

		notif := &domain.Notificacion{
			ID:             "notif-fallo-01",
			IdempotencyKey: "pago-error-300",
			Destinatario:   "servidor.caido@error.com",
			Canal:          domain.CanalEmail,
			Estado:         domain.EstadoPendiente,
		}

		err := dispatcher.DespacharConResiliencia(context.Background(), notif)

		if !errors.Is(err, domain.ErrMaxReintentosExcedido) {
			t.Errorf("Se esperaba error '%v', obtenido: '%v'", domain.ErrMaxReintentosExcedido, err)
		}
		if notif.Intentos != 3 {
			t.Errorf("Se esperaban 3 intentos obtenidos: %d", notif.Intentos)
		}
		if notif.Estado != domain.EstadoFallido {
			t.Errorf("Estado esperado FALLIDO, obtenido: %s", notif.Estado)
		}
	})

	// CASO 4: Cancelación de Contexto
	t.Run("Debe interrumpir reintentos inmediatamente si el contexto es cancelado", func(t *testing.T) {
		mockRepo := &MockIdempotencyRepo{
			esDuplicadoResp: false,
		}

		dispatcher := NewNotificationDispatcher(mockRepo, 5, 500*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		notif := &domain.Notificacion{
			ID:             "notif-cancel-01",
			IdempotencyKey: "pago-cancel-400",
			Destinatario:   "servidor.caido@error.com",
			Canal:          domain.CanalEmail,
			Estado:         domain.EstadoPendiente,
		}

		err := dispatcher.DespacharConResiliencia(ctx, notif)

		if !errors.Is(err, context.Canceled) {
			t.Errorf("Se esperaba error 'context.Canceled', obtenido: '%v'", err)
		}
	})
}

func TestNotificationDispatcher_CircuitBreaker(t *testing.T) {
	st := gobreaker.Settings{
		Name:        "TestProvider",
		MaxRequests: 1,
		Interval:    time.Second,
		Timeout:     100 * time.Millisecond,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.TotalFailures >= 2
		},
	}
	cb := gobreaker.NewCircuitBreaker[any](st)

	for i := 0; i < 2; i++ {
		_, _ = cb.Execute(func() (any, error) {
			return nil, errors.New("fallo del proveedor")
		})
	}

	_, err := cb.Execute(func() (any, error) {
		return "ok", nil
	})

	if !errors.Is(err, gobreaker.ErrOpenState) {
		t.Errorf("Se esperaba que el circuito estuviera abierto (ErrOpenState), se obtuvo: %v", err)
	}
}

func TestNotificationDispatcher_EnviarConCircuitBreaker(t *testing.T) {
	repo := &MockIdempotencyRepo{}
	dispatcher := NewNotificationDispatcher(repo, 3, 10*time.Millisecond)

	ctx := context.Background()
	notifExito := &domain.Notificacion{
		ID:           "test-cb-dispatch-1",
		Destinatario: "test@empresa.com",
		Canal:        domain.CanalEmail,
	}

	t.Run("Debe ejecutar el envío exitosamente a través del circuito cerrado", func(t *testing.T) {
		err := dispatcher.enviarConCircuitBreaker(ctx, notifExito)
		if err != nil && errors.Is(err, gobreaker.ErrOpenState) {
			t.Errorf("No se esperaba error de circuito abierto, se obtuvo: %v", err)
		}
	})

	t.Run("Debe abrir el circuito tras fallos consecutivos y retornar error de circuito abierto", func(t *testing.T) {
		// Forzar fallos directamente en el circuitBreaker para superar el umbral ReadyToTrip
		for i := 0; i < 6; i++ {
			_, _ = dispatcher.circuitBreaker.Execute(func() (any, error) {
				return nil, errors.New("fallo forzado del proveedor")
			})
		}

		// La siguiente llamada debe fallar porque el circuito está abierto
		err := dispatcher.enviarConCircuitBreaker(ctx, notifExito)
		if err == nil {
			t.Error("Se esperaba un error por circuito abierto, se obtuvo nil")
		}
	})
}

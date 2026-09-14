package domain

import (
	"errors"
	"time"
)

// Errores de Dominio
var (
	ErrDestinatarioVacio     = errors.New("el destinatario de la notificación no puede estar vacío")
	ErrMensajeVacio          = errors.New("el mensaje de la notificación no puede estar vacío")
	ErrCanalInvalido         = errors.New("el canal de notificación especificado no es soportado")
	ErrMaxReintentosExcedido = errors.New("se ha superado el límite máximo de reintentos de envío")
)

// CanalNotificacion define las vías de entrega
type CanalNotificacion string

const (
	CanalEmail CanalNotificacion = "EMAIL"
	CanalSMS   CanalNotificacion = "SMS"
	CanalPush  CanalNotificacion = "PUSH"
)

// EstadoNotificacion define el ciclo de vida del evento
type EstadoNotificacion string

const (
	EstadoPendiente EstadoNotificacion = "PENDIENTE"
	EstadoEnviado   EstadoNotificacion = "ENVIADO"
	EstadoFallido   EstadoNotificacion = "FALLIDO"
)

// Notificacion es la entidad central del sistema
type Notificacion struct {
	ID             string             `json:"id"`
	IdempotencyKey string             `json:"idempotency_key"` // Clave única para evitar duplicados
	Canal          CanalNotificacion  `json:"canal"`
	Destinatario   string             `json:"destinatario"`
	Mensaje        string             `json:"mensaje"`
	Estado         EstadoNotificacion `json:"estado"`
	Intentos       int                `json:"intentos"`
	CreatedAt      time.Time          `json:"created_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
}

// Validar verifica que la notificación cumpla con las reglas esenciales de negocio
func (n *Notificacion) Validar() error {
	if n.Destinatario == "" {
		return ErrDestinatarioVacio
	}
	if n.Mensaje == "" {
		return ErrMensajeVacio
	}
	switch n.Canal {
	case CanalEmail, CanalSMS, CanalPush:
		return nil
	default:
		return ErrCanalInvalido
	}
}

func (n *Notificacion) MarcarEnviado() {
	n.Estado = EstadoEnviado
	n.UpdatedAt = time.Now()
}

func (n *Notificacion) RegistrarFallo(maxIntentos int) error {
	n.Intentos++
	n.UpdatedAt = time.Now()
	if n.Intentos >= maxIntentos {
		n.Estado = EstadoFallido
		return ErrMaxReintentosExcedido
	}
	return nil
}

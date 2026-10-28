package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNotificacion_Validar(t *testing.T) {
	tests := []struct {
		name        string
		n           Notificacion
		expectedErr error
	}{
		{
			name: "Debe ser válida con canal EMAIL y datos completos",
			n: Notificacion{
				Destinatario: "test@empresa.com",
				Mensaje:      "Hola mundo",
				Canal:        CanalEmail,
			},
			expectedErr: nil,
		},
		{
			name: "Debe ser válida con canal SMS",
			n: Notificacion{
				Destinatario: "+573001234567",
				Mensaje:      "Código OTP: 1234",
				Canal:        CanalSMS,
			},
			expectedErr: nil,
		},
		{
			name: "Debe ser válida con canal PUSH",
			n: Notificacion{
				Destinatario: "device-token-xyz",
				Mensaje:      "Nueva alerta",
				Canal:        CanalPush,
			},
			expectedErr: nil,
		},
		{
			name: "Debe fallar si el destinatario está vacío",
			n: Notificacion{
				Destinatario: "",
				Mensaje:      "Hola mundo",
				Canal:        CanalEmail,
			},
			expectedErr: ErrDestinatarioVacio,
		},
		{
			name: "Debe fallar si el mensaje está vacío",
			n: Notificacion{
				Destinatario: "test@empresa.com",
				Mensaje:      "",
				Canal:        CanalEmail,
			},
			expectedErr: ErrMensajeVacio,
		},
		{
			name: "Debe fallar si el canal no es soportado",
			n: Notificacion{
				Destinatario: "test@empresa.com",
				Mensaje:      "Hola mundo",
				Canal:        CanalNotificacion("FAX"),
			},
			expectedErr: ErrCanalInvalido,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.n.Validar()
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestNotificacion_RegistrarFallo(t *testing.T) {
	t.Run("Debe incrementar intentos sin alcanzar el máximo", func(t *testing.T) {
		n := &Notificacion{Intentos: 0}
		maxIntentos := 3

		err := n.RegistrarFallo(maxIntentos)
		assert.NoError(t, err)
		assert.Equal(t, 1, n.Intentos)
		assert.NotEqual(t, EstadoFallido, n.Estado)
	})

	t.Run("Debe marcar como fallido y retornar error al alcanzar el máximo de reintentos", func(t *testing.T) {
		n := &Notificacion{Intentos: 2}
		maxIntentos := 3

		err := n.RegistrarFallo(maxIntentos)
		assert.ErrorIs(t, err, ErrMaxReintentosExcedido)
		assert.Equal(t, 3, n.Intentos)
		assert.Equal(t, EstadoFallido, n.Estado)
	})
}
func TestNotificacion_MarcarEnviado(t *testing.T) {
	n := &Notificacion{
		Estado: EstadoPendiente,
	}

	n.MarcarEnviado()

	assert.Equal(t, EstadoEnviado, n.Estado)
	assert.False(t, n.UpdatedAt.IsZero(), "El timestamp UpdatedAt debe ser actualizado")
}

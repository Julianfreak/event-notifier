package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

func TestRedisIdempotencyStorage(t *testing.T) {
	// 1. Iniciamos el servidor simulado de miniredis
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	// 2. Instanciamos nuestro adaptador apuntando al servidor simulado
	// Nota: password vacío y db 0 por defecto para el test
	repo, err := NewRedisIdempotencyStorage(mr.Addr(), "", 0)
	assert.NoError(t, err)

	ctx := context.Background()
	key := "transaccion-123"

	t.Run("Debe retornar false cuando la clave no existe", func(t *testing.T) {
		duplicado, err := repo.EsDuplicado(ctx, key)
		assert.NoError(t, err)
		assert.False(t, duplicado)
	})

	t.Run("Debe marcar como procesado exitosamente con SetNX", func(t *testing.T) {
		err := repo.MarcarProcesado(ctx, key, 5*time.Minute)
		assert.NoError(t, err)

		// Verificamos que miniredis realmente guardó la clave con el prefijo correcto
		assert.True(t, mr.Exists("idempotency:"+key))
	})

	t.Run("Debe retornar true cuando la clave ya existe", func(t *testing.T) {
		duplicado, err := repo.EsDuplicado(ctx, key)
		assert.NoError(t, err)
		assert.True(t, duplicado)
	})

	t.Run("Debe fallar al intentar marcar nuevamente la misma clave (concurrencia)", func(t *testing.T) {
		err := repo.MarcarProcesado(ctx, key, 5*time.Minute)
		assert.Error(t, err, "Debería retornar error porque SetNX detecta que la clave ya existe")
	})
}

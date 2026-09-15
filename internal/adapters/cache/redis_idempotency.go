package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"event-notifier/internal/core/ports"
)

// redisIdempotencyStorage implementa el puerto ports.IdempotencyRepository
type redisIdempotencyStorage struct {
	client *redis.Client
}

// NewRedisIdempotencyStorage es el constructor que inicializa la conexión con Redis
func NewRedisIdempotencyStorage(addr string, password string, db int) (ports.IdempotencyRepository, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	// Verificamos conectividad inmediata mediante un Ping con timeout de 3 segundos
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("fallo de conexión con Redis en %s: %w", addr, err)
	}

	return &redisIdempotencyStorage{
		client: rdb,
	}, nil
}

// EsDuplicado verifica si una IdempotencyKey ya fue registrada previamente en Redis
func (r *redisIdempotencyStorage) EsDuplicado(ctx context.Context, key string) (bool, error) {
	claveCompleta := fmt.Sprintf("idempotency:%s", key)

	// Exists devuelve 1 si la clave existe, o 0 si no existe
	resultado, err := r.client.Exists(ctx, claveCompleta).Result()
	if err != nil {
		return false, fmt.Errorf("error al consultar clave en Redis: %w", err)
	}

	// Si resultado es mayor a 0, la clave ya existe (es un duplicado)
	return resultado > 0, nil
}

// MarcarProcesado registra la clave usando SETNX de forma atómica con expiración (TTL)
func (r *redisIdempotencyStorage) MarcarProcesado(ctx context.Context, key string, ttl time.Duration) error {
	claveCompleta := fmt.Sprintf("idempotency:%s", key)

	// SetNX (Set if Not eXists) intenta guardar el valor "PROCESADO" con el tiempo de vida (TTL).
	// Si la clave ya existía, 'grabado' será false.
	grabado, err := r.client.SetNX(ctx, claveCompleta, "PROCESADO", ttl).Result()
	if err != nil {
		return fmt.Errorf("error al registrar idempotencia en Redis: %w", err)
	}

	if !grabado {
		return fmt.Errorf("la clave %s ya fue procesada concurrentemente por otro hilo", key)
	}

	return nil
}

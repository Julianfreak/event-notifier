package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"event-notifier/internal/adapters/cache"
)

func main() {
	fmt.Println("==================================================")
	fmt.Println("  Probando Adaptador de Idempotencia con Redis")
	fmt.Println("==================================================")

	// 1. Conectamos con el Redis que corre en el contenedor (localhost:6379)
	redisRepo, err := cache.NewRedisIdempotencyStorage("localhost:6379", "", 0)
	if err != nil {
		log.Fatalf("Error al inicializar Redis: %v", err)
	}
	fmt.Println("[Redis]: Conexión establecida exitosamente.")

	ctx := context.Background()
	clavePrueba := "pago-orden-9988"

	// 2. Primera verificación: ¿Es duplicado?
	esDuplicado, err := redisRepo.EsDuplicado(ctx, clavePrueba)
	if err != nil {
		log.Fatalf("Error al consultar clave: %v", err)
	}
	fmt.Printf("1. ¿La clave '%s' es duplicada?: %v (Esperado: false)\n", clavePrueba, esDuplicado)

	// 3. Marcamos la clave como procesada con un TTL de 10 segundos
	fmt.Println("2. Marcando clave como procesada en Redis con TTL de 10 segundos...")
	err = redisRepo.MarcarProcesado(ctx, clavePrueba, 10*time.Second)
	if err != nil {
		log.Fatalf("Fallo al marcar procesado: %v", err)
	}
	fmt.Println("   --> Clave registrada exitosamente en Redis.")

	// 4. Segunda verificación inmediata: Debe ser duplicada
	esDuplicado, err = redisRepo.EsDuplicado(ctx, clavePrueba)
	if err != nil {
		log.Fatalf("Error al consultar clave: %v", err)
	}
	fmt.Printf("3. ¿La clave '%s' es duplicada ahora?: %v (Esperado: true)\n", clavePrueba, esDuplicado)

	// 5. Intento de reprocesamiento (Simulando que llega el mismo evento otra vez)
	fmt.Println("4. Intentando registrar la misma clave nuevamente (Simulando reenvío)...")
	err = redisRepo.MarcarProcesado(ctx, clavePrueba, 10*time.Second)
	if err != nil {
		fmt.Printf("   --> ¡BLOQUEO EXITOSO!: %v\n", err)
	} else {
		fmt.Println("   --> ERROR: Redis permitió registrar una clave duplicada.")
	}
}

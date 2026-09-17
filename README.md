# Event Notifier - Motor Asíncrono de Notificaciones y Eventos Distribuidos

Motor de procesamiento asíncrono de eventos y notificaciones masivas de alta concurrencia desarrollado en Go (Golang), diseñado bajo los principios de Arquitectura Hexagonal (Ports and Adapters), Clean Code y patrones avanzados de sistemas distribuidos.

---

## Arquitectura del Sistema

El proyecto separa la lógica central del negocio de los mecanismos de transporte y brokers externos:

* internal/core/domain: Entidades puras (Notificación, Canales EMAIL/SMS/PUSH, estados y validaciones de ciclo de vida).
* internal/core/ports: Contratos e interfaces para el Broker de Mensajería, la persistencia de idempotencia y los proveedores de envío.
* internal/core/services: Lógica de orquestación, patrón Worker Pool concurrente y despacho de notificaciones.
* internal/adapters/broker: Adaptador de RabbitMQ (Exchange directo durable, colas persistentes, control QoS y manual Ack/Nack).
* internal/adapters/cache: Adaptador de Redis para control de idempotencia distribuida (prevención estricta de mensajes duplicados con SetNX).
* cmd/worker: Punto de entrada del servicio consumidor con soporte de Graceful Shutdown (SIGINT/SIGTERM).

---

## Características Técnicas y Patrones

* Lenguaje: Go 1.23+
* Broker de Mensajería: RabbitMQ con protocolo AMQP 0-9-1, colas y exchanges durables, mensajes persistentes en disco y acuse de recibo manual (Manual Ack).
* Concurrencia: Patrón Worker Pool con Goroutines y Canales para limitar el consumo de CPU y memoria ante cargas masivas.
* Calidad de Servicio (QoS): Configuración de prefetch count en 1 para garantizar un reparto equilibrado de mensajes entre workers concurrentes.
* Apagado Controlado (Graceful Shutdown): Captura de señales del sistema operativo (SIGINT, SIGTERM) mediante contextos cancelables y sync.WaitGroup para evitar pérdida de datos en tránsito.
* Idempotencia Distribuida: Bloqueo atómico y verificación de duplicados mediante Idempotency Keys en Redis utilizando la operación SetNX con expiración automática (TTL).
* Resiliencia y DLQ: Enrutamiento automático de mensajes fallidos a una Dead Letter Queue (DLQ) mediante directivas x-dead-letter-exchange al agotar reintentos.
* Exponential Backoff: Algoritmo de reintentos con duplicación de tiempo de espera (1s -> 2s -> 4s) para mitigar caídas de servicios externos.
* Contenedorización: Docker Compose para orquestar la infraestructura distribuida (Redis y RabbitMQ).

---

## Cómo ejecutar el proyecto y sus componentes

### 1. Iniciar la infraestructura de soporte (Redis y RabbitMQ)

Comando: `docker compose up -d`

* Panel de Administración Web de RabbitMQ: http://localhost:15672 (Usuario: guest | Contraseña: guest)

### 2. Ejecutar la prueba del Consumidor, Broker e Idempotencia

Comando: `go run cmd/worker/main.go`

---

## Estado del Proyecto

* [x] Fase 1: Inicialización, Entidades de Dominio y Reglas de Negocio.
* [x] Fase 2: Definición de Puertos e implementación de Worker Pool concurrente.
* [x] Fase 3: Integración de Redis para control de Idempotencia.
* [x] Fase 4: Integración de RabbitMQ (Productor, Consumidor, Exchanges y Colas con Manual Ack).
* [x] Fase 5: Patrón de Resiliencia con Reintentos Exponenciales, DLQ y Graceful Shutdown.
* [ ] Fase 6: Pruebas Unitarias con Mocks y verificación de cobertura.

---

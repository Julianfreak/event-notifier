# Event Notifier - Motor Asíncrono de Notificaciones y Eventos Distribuidos

Motor de procesamiento asíncrono de eventos y notificaciones masivas de alta concurrencia desarrollado en Go (Golang), diseñado bajo los principios de Arquitectura Hexagonal (Ports and Adapters), Clean Code y patrones avanzados de sistemas distribuidos[cite: 1].

---

## Arquitectura del Sistema

El proyecto separa la lógica central del negocio de los mecanismos de transporte y brokers externos[cite: 1]:

* internal/core/domain: Entidades puras (Notificación, Canales EMAIL/SMS/PUSH, estados y validaciones de ciclo de vida)[cite: 1].
* internal/core/ports: Contratos e interfaces para el Broker de Mensajería, la persistencia de idempotencia y los proveedores de envío[cite: 1].
* internal/core/services: Lógica de orquestación, patrón Worker Pool concurrente y despacho de notificaciones[cite: 1].
* internal/adapters/broker: Adaptador de RabbitMQ (Exchange directo durable, colas persistentes, control QoS y manual Ack/Nack)[cite: 1].
* internal/adapters/cache: Adaptador de Redis para control de idempotencia distribuida (prevención estricta de mensajes duplicados con SetNX)[cite: 1].
* cmd/worker: Punto de entrada del servicio consumidor con soporte de Graceful Shutdown (SIGINT/SIGTERM)[cite: 1].

---

## Características Técnicas y Patrones

* Lenguaje: Go 1.23+[cite: 1]
* Broker de Mensajería: RabbitMQ con protocolo AMQP 0-9-1, colas y exchanges durables, mensajes persistentes en disco y acuse de recibo manual (Manual Ack)[cite: 1].
* Concurrencia: Patrón Worker Pool con Goroutines y Canales para limitar el consumo de CPU y memoria ante cargas masivas[cite: 1].
* Calidad de Servicio (QoS): Configuración de prefetch count en 1 para garantizar un reparto equilibrado de mensajes entre workers concurrentes[cite: 1].
* Apagado Controlado (Graceful Shutdown): Captura de señales del sistema operativo (SIGINT, SIGTERM) mediante contextos cancelables y sync.WaitGroup para evitar pérdida de datos en tránsito[cite: 1].
* Idempotencia Distribuida: Bloqueo atómico y verificación de duplicados mediante Idempotency Keys en Redis utilizando la operación SetNX con expiración automática (TTL)[cite: 1].
* Resiliencia y DLQ: Enrutamiento automático de mensajes fallidos a una Dead Letter Queue (DLQ) mediante directivas x-dead-letter-exchange al agotar reintentos[cite: 1].
* Exponential Backoff: Algoritmo de reintentos con duplicación de tiempo de espera (1s -> 2s -> 4s) para mitigar caídas de servicios externos[cite: 1].
* Testing Automatizado: Pruebas unitarias mediante Mocks e interfaces implícitas (testing nativo) evaluando idempotencia, fallas de red, agotamiento de reintentos y cancelaciones de contexto con alta cobertura (>85%)[cite: 1].
* Contenedorización: Docker Compose para orquestar la infraestructura distribuida (Redis y RabbitMQ)[cite: 1].
* **CI/CD Automatizado:** Pipeline en GitHub Actions para ejecución continua de pruebas unitarias (>85% estricto) y construcción/publicación automatizada de imágenes Docker multi-stage.
*Observabilidad (NUEVO): Instrumentación con Prometheus exponiendo métricas clave de negocio (Counters), rendimiento/latencia (Histograms) y saturación del sistema (Gauges).

---

## Cómo ejecutar el proyecto y sus componentes

### 1. Iniciar la infraestructura de soporte (Broker, Caché y Observabilidad)

Comando: `docker compose up -d`

El stack levantará los siguientes servicios de forma efímera:
* **RabbitMQ (Broker):** http://localhost:15672 (guest/guest)
* **Grafana (Visualización unificada):** http://localhost:3000 (Sin login - Admin por defecto)
* **Grafana Alloy (Colector):** http://localhost:12345
* **Prometheus (Métricas):** http://localhost:9091
* **Loki (Logs Estructurados):** Operando en background en el puerto 3100

### 2. Ejecutar la aplicación (Consumidor y Generador de Telemetría)

Comando: `go run cmd/worker/main.go`

* El worker expondrá sus métricas internas en `http://localhost:2112/metrics` (consumidas por Alloy).
* Generará logs estructurados en formato JSON (usando `log/slog`) tanto en consola como en `app.log`.

### 3. Ejecutar las Pruebas Unitarias y Cobertura

Comando: `go test -v -cover ./internal/core/services/...`

---

## Estado del Proyecto

* [x] Fase 1: Inicialización, Entidades de Dominio y Reglas de Negocio[cite: 1].
* [x] Fase 2: Definición de Puertos e implementación de Worker Pool concurrente[cite: 1].
* [x] Fase 3: Integración de Redis para control de Idempotencia[cite: 1].
* [x] Fase 4: Integración de RabbitMQ (Productor, Consumidor, Exchanges y Colas con Manual Ack)[cite: 1].
* [x] Fase 5: Patrón de Resiliencia con Reintentos Exponenciales, DLQ y Graceful Shutdown[cite: 1].
* [x] Fase 6: Pruebas Unitarias con Mocks y verificación de cobertura[cite: 1].
* [x] Fase 7: Implementación de Pipeline CI/CD con GitHub Actions y Docker.
* [x] Fase 8: Integración de Observabilidad (Métricas con Prometheus).
* [ ] Fase 9: Dashboards de Grafana e Integración con Testcontainers.

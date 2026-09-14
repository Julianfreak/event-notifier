# Event Notifier - Motor Asíncrono de Notificaciones y Eventos Distribuidos

Motor de procesamiento asíncrono de eventos y notificaciones masivas de alta concurrencia desarrollado en **Go (Golang)**, diseñado bajo los principios de **Arquitectura Hexagonal (Ports and Adapters)**, **Clean Code** y patrones avanzados de sistemas distribuidos.

Arquitectura del Sistema
El proyecto separa la lógica central del negocio de los mecanismos de transporte y brokers externos:

internal/core/domain: Entidades puras (Notificación, Canales EMAIL/SMS/PUSH, estados y validaciones de ciclo de vida).

internal/core/ports: Contratos e interfaces para el Broker de Mensajería, la persistencia de idempotencia y los proveedores de envío.

internal/core/services: Lógica de orquestación, reintentos con Exponential Backoff y despacho de notificaciones.

internal/adapters/broker: Adaptadores para RabbitMQ (Colas, Exchanges y Dead Letter Queues).

internal/adapters/cache: Adaptador de Redis para control de idempotencia (prevención estricta de mensajes duplicados).

cmd/worker: Punto de entrada del servicio consumidor (Background Worker).

Características Técnicas y Patrones
Lenguaje: Go 1.23+

Concurrencia: Patrón Worker Pool con Goroutines y Canales para limitar el consumo de CPU y memoria ante cargas masivas.

Apagado Controlado (Graceful Shutdown): Manejo de señales del sistema operativo para evitar la pérdida de eventos en tránsito al detener el servicio.

Idempotencia Distribuida: Bloqueo y verificación de duplicados mediante Idempotency Keys en Redis.

Resiliencia: Reintentos con Exponential Backoff y enrutamiento de mensajes irrecuperables a una Dead Letter Queue (DLQ).

Contenedorización: Docker Multi-stage build con imagen final basada en Alpine Linux (< 15MB).

Estado del Proyecto
[x] Fase 1: Inicialización, Entidades de Dominio y Reglas de Negocio.

[ ] Fase 2: Definición de Puertos e implementación de Worker Pool concurrente.

[ ] Fase 3: Integración de Redis para control de Idempotencia.

[ ] Fase 4: Integración de RabbitMQ (Productor, Consumidor y Dead Letter Queue).

[ ] Fase 5: Patrón de Resiliencia con Reintentos Exponenciales y Graceful Shutdown.

[ ] Fase 6: Pruebas Unitarias con Mocks y verificación de cobertura.
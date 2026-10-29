# Event Notifier - Motor Asíncrono de Notificaciones y Eventos Distribuidos

Motor de procesamiento asíncrono de eventos y notificaciones masivas de alta concurrencia desarrollado en Go (Golang), diseñado bajo los principios de Arquitectura Hexagonal (Ports and Adapters), Clean Code y patrones avanzados de sistemas distribuidos.

---

## Arquitectura del Sistema

El proyecto separa la lógica central del negocio de los mecanismos de transporte y brokers externos:

- `internal/core/domain`: Entidades puras (Notificación, Canales EMAIL/SMS/PUSH, estados y validaciones de ciclo de vida).
- `internal/core/ports`: Contratos e interfaces para el Broker de Mensajería, la persistencia de idempotencia y los proveedores de envío.
- `internal/core/services`: Lógica de orquestación, patrón Worker Pool concurrente y despacho de notificaciones.
- `internal/adapters/broker`: Adaptador de RabbitMQ (Exchange directo durable, colas persistentes, control QoS y manual Ack/Nack).
- `internal/adapters/cache`: Adaptador de Redis para control de idempotencia distribuida (prevención estricta de mensajes duplicados con SetNX).
- `cmd/worker`: Punto de entrada del servicio consumidor con soporte de Graceful Shutdown (SIGINT/SIGTERM).

---

## Características Técnicas y Patrones

- **Lenguaje:** Go 1.25+
- **Broker de Mensajería:** RabbitMQ con protocolo AMQP 0-9-1, colas y exchanges durables, mensajes persistentes en disco y acuse de recibo manual (Manual Ack).
- **Concurrencia:** Patrón Worker Pool con Goroutines y Canales para limitar el consumo de CPU y memoria ante cargas masivas.
- **Calidad de Servicio (QoS):** Configuración de prefetch count en 1 para garantizar un reparto equilibrado de mensajes entre workers concurrentes.
- **Apagado Controlado (Graceful Shutdown):** Captura de señales del sistema operativo (SIGINT, SIGTERM) mediante contextos cancelables y sync.WaitGroup para evitar pérdida de datos en tránsito.
- **Idempotencia Distribuida:** Bloqueo atómico y verificación de duplicados mediante Idempotency Keys en Redis utilizando la operación SetNX con expiración automática (TTL).
- **Resiliencia y DLQ:** Enrutamiento automático de mensajes fallidos a una Dead Letter Queue (DLQ) mediante directivas x-dead-letter-exchange al agotar reintentos.
- **Exponential Backoff:** Algoritmo de reintentos con duplicación de tiempo de espera (1s -> 2s -> 4s) para mitigar caídas de servicios externos.
- **Testing Automatizado:** Pruebas unitarias mediante Mocks e interfaces implícitas (testing nativo) evaluando idempotencia, fallas de red, agotamiento de reintentos y cancelaciones de contexto con alta cobertura (>85%).
- **Contenedorización Completa:** Orquestación total mediante Docker Compose (Worker, Redis, RabbitMQ, Prometheus, Loki, Alloy y Grafana).
- **CI/CD Automatizado:** Pipeline en GitHub Actions para ejecución continua de pruebas unitarias (>85% estricto) y construcción/publicación automatizada de imágenes Docker multi-stage.

---

## 📊 Stack de Observabilidad y Monitoreo

El proyecto cuenta con un ecosistema completo de observabilidad contenerizado y automatizado bajo los principios de aplicaciones nativas de la nube (12-Factor App):

- **Worker de Go (`:2112`):** Servicio backend empaquetado mediante *multi-stage build*, que emite métricas de Prometheus y logs estructurados en JSON (`log/slog`) directamente a la salida estándar (`stdout`).
- **Prometheus (`:9091`):** Recolecta y almacena series temporales de métricas expuestas por el worker.
- **Grafana (`:3000`):** Interfaz visual con aprovisionamiento automático de dashboards y fuentes de datos protegidas con UIDs fijos.
- **Loki (`:3100`):** Sistema de agregación de logs escalable.
- **Grafana Alloy:** Agente colector conectado al socket de Docker (`/var/run/docker.sock`) que descubre automáticamente el contenedor del worker, extrae sus logs de `stdout` en tiempo real y los reenvía a Loki.

### ⚙️ Arquitectura limpia sin archivos locales
El sistema **no requiere archivos de log físicos en el host**. Todo el flujo de logs viaja de manera limpia desde el contenedor hacia Loki mediante el socket de Docker, garantizando portabilidad total al clonar el repositorio.

---

## Cómo ejecutar el proyecto y sus componentes

Todo el ecosistema (incluyendo la infraestructura de soporte y el worker de Go) se levanta de manera unificada mediante Docker Compose:

### 1. Levantar el Stack Completo (Infraestructura + Worker)

Ejecuta el siguiente comando en la raíz del proyecto para construir y poner en marcha todos los servicios:

```bash
docker compose up --build -d

```

Esto desplegará los siguientes servicios accesibles:

* **Grafana (Visualización unificada de métricas y logs):** http://localhost:3000 (Acceso automático con permisos de Administrador)
* **Prometheus (Métricas del Worker):** http://localhost:9091
* **RabbitMQ (Gestión de Colas):** http://localhost:15672 (Usuario/Contraseña: `guest` / `guest`)
* **Worker de Go (Métricas expuestas):** http://localhost:2112/metrics
* **Loki & Alloy:** Operando de forma transparente en background recolectando telemetría.

### 2. Pruebas y Calidad de Código

El proyecto cuenta con una suite robusta de pruebas unitarias y de integración ligera:

* **Pruebas de Dominio (`internal/core/domain`):** 100% de cobertura de sentencias en reglas de negocio y transiciones de estado mediante *Table-driven tests*.
* **Pruebas de Caché e Idempotencia (`internal/adapters/cache`):** Validadas utilizando un servidor Redis en memoria con **`miniredis`**.
* **Pruebas de Resiliencia y Dispatcher (`internal/core/services`):** Cobertura de reintentos con *Exponential Backoff*, DLQ y contextos.
* **Seguridad Concurrente (Race Detector):** Verificado exitosamente bajo el comando **`go test -race ./...`** para garantizar cero condiciones de carrera (*data races*).

---

## Estado del Proyecto

* [x] Fase 1: Inicialización, Entidades de Dominio y Reglas de Negocio.
* [x] Fase 2: Definición de Puertos e implementación de Worker Pool concurrente.
* [x] Fase 3: Integración de Redis para control de Idempotencia.
* [x] Fase 4: Integración de RabbitMQ (Productor, Consumidor, Exchanges y Colas con Manual Ack).
* [x] Fase 5: Patrón de Resiliencia con Reintentos Exponenciales, DLQ y Graceful Shutdown.
* [x] Fase 6: Pruebas Unitarias con Mocks y verificación de cobertura.
* [x] Fase 7: Pipeline CI/CD con GitHub Actions y Docker.
* [x] Fase 8: Stack de Observabilidad completo (Prometheus, Loki, Grafana, Alloy).
* [x] Fase 9: Contenedorización avanzada del Worker y aprovisionamiento automático por código.

```
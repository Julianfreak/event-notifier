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
- `cmd/api`: Punto de entrada del servicio HTTP REST (Productor) para recibir peticiones externas y encolarlas en RabbitMQ.
- `cmd/worker`: Punto de entrada del servicio consumidor asíncrono con soporte de Graceful Shutdown (SIGINT/SIGTERM).
- `internal/adapters/http`: Adaptador de entrada HTTP para la validación y transformación de payloads REST hacia el dominio.

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

---

## ⚙️ Configuración del Entorno (Variables de Entorno)

Este proyecto utiliza un archivo `.env` para gestionar de forma segura las credenciales y los parámetros de configuración de la infraestructura (Docker Compose).

### 1. Configurar las variables locales

Antes de levantar los servicios por primera vez, debes crear tu propio archivo `.env` a partir de la plantilla de ejemplo provista (`.env.example`):

```bash
# Copia la plantilla de ejemplo
cp .env.example .env
```

---

## 🩺 Monitoreo de Salud y Readiness (Sondas)

La API REST incluye endpoints dedicados para integrarse con orquestadores de contenedores (como Kubernetes, Docker Swarm y ECS) y garantizar la alta disponibilidad mediante observabilidad nativa.

### Endpoints disponibles

- **`GET /healthz` (Liveness Probe):**
  - **Descripción:** Verifica que el proceso HTTP de la API esté activo, vivo y respondiendo peticiones.
  - **Respuesta exitosa (`200 OK`):**

    ```json
    {
      "status": "alive",
      "service": "event-notifier-api"
    }
    ```

- **`GET /readyz` (Readiness Probe):**
  - **Descripción:** Comprueba de forma activa y en tiempo real la conectividad con las dependencias críticas de infraestructura: **RabbitMQ** y **Redis**.
  - **Respuesta exitosa (`200 OK`):**

    ```json
    {
      "status": "ready",
      "rabbitmq": "connected",
      "redis": "connected"
    }
    ```

  - **Respuesta de fallo (`503 Service Unavailable`):** Se activa automáticamente si alguna de las dependencias no responde dentro del tiempo límite, previniendo que el balanceador envíe tráfico al contenedor afectado.

    ```json
    {
      "status": "not ready",
      "error": "RabbitMQ no disponible: ..."
    }
    ```

---

## 🔍 Trazabilidad Distribuida (Correlation IDs y Request IDs)

El sistema implementa trazabilidad de extremo a extremo para seguir el ciclo de vida completo de una solicitud a través de los microservicios y canales asíncronos:

1. **API REST Middleware (`RequestIDMiddleware`)**: Intercepta cada petición HTTP entrante. Si el cliente incluye el header `X-Request-ID`, lo reutiliza; de lo contrario, genera un **UUID v4** único. Este ID se inyecta en el contexto de Go y se devuelve en la respuesta HTTP.
2. **Propagación en RabbitMQ (`X-Correlation-ID`)**: Al publicar un evento desde la API, el adaptador AMQP extrae el identificador del contexto y lo inyecta automáticamente en las cabeceras (*Headers*) del mensaje bajo la clave `X-Correlation-ID`.
3. **Consumo y Logs en el Worker**: El worker extrae esta cabecera al procesar el mensaje, vinculándola al contexto de ejecución y configurando un logger estructurado (`slog`) con el campo `correlation_id`, facilitando su consulta y filtrado en Grafana / Loki.

### 🧪 Cómo realizar pruebas (Clientes HTTP y herramientas)

Puedes validar el funcionamiento de las sondas utilizando tu herramienta o técnica preferida.

> **Nota:** Asegúrate de utilizar el puerto `8081` si configuraste un reenvío personalizado en tu entorno local.

#### 1. Clientes HTTP (Insomnia, Postman, Thunder Client)

Ideal para el desarrollo diario y las pruebas interactivas:

1. Crea una nueva petición HTTP seleccionando el método **`GET`**.
2. Ingresa la URL correspondiente según la sonda que desees probar:
   - **Liveness:** `http://localhost:8081/healthz`
   - **Readiness:** `http://localhost:8081/readyz`
3. Haz clic en **Send** para inspeccionar la respuesta JSON y el código de estado (`200 OK` o `503 Service Unavailable`).

#### 2. Desde la terminal con `curl` (CLI)

Útil para scripts de automatización, diagnósticos rápidos o entornos sin interfaz gráfica:

```bash
# Probar Liveness
curl -i http://localhost:8081/healthz

# Probar Readiness
curl -i http://localhost:8081/readyz
```

### ⚙️ Arquitectura limpia sin archivos locales

El sistema **no requiere archivos de log físicos en el host**. Todo el flujo de logs viaja de manera limpia desde el contenedor hacia Loki mediante el socket de Docker, garantizando portabilidad total al clonar el repositorio.

---

## 🚀 Cómo ejecutar el proyecto y sus componentes

Todo el ecosistema (incluyendo la infraestructura de soporte y el worker de Go) se levanta de manera unificada mediante Docker Compose.

### 1. Levantar el Stack Completo (Infraestructura, Worker y API REST)

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
* **API REST (:8080):** Punto de entrada HTTP para enviar notificaciones (POST /api/v1/notifications).

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
* [x] Fase 10: Monitoreo de Salud y Readiness (Sondas).

```
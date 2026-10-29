# --- ETAPA 1: Construcción del binario ---
FROM golang:alpine AS builder

# Instalar git y dependencias necesarias para compilar si aplica
RUN apk add --no-cache git

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copilar dependencias primero para aprovechar la caché de Docker
COPY go.mod go.sum ./
RUN go mod download

# Copiar el código fuente del proyecto
COPY . .

# Compilar el binario estático de Go
# -ldflags="-s -w" elimina información de depuración para reducir el tamaño del binario
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/bin/worker ./cmd/worker/main.go

# --- ETAPA 2: Imagen final ligera para producción ---
FROM alpine:3.19

# Instalar certificados CA por si se requieren llamadas TLS externas
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copiar el binario compilado desde la etapa anterior
COPY --from=builder /app/bin/worker /app/worker

# Exponer el puerto de métricas Prometheus de la aplicación
EXPOSE 2112

# Comando por defecto para ejecutar el worker
CMD ["/app/worker"]
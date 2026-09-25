# Etapa 1: Compilación de la aplicación Go
FROM golang:1.23-alpine AS builder

# Directorio de trabajo dentro del contenedor de compilación
WORKDIR /app

# Copiar archivos de dependencias primero para aprovechar el caché de capas de Docker
COPY go.mod go.sum ./
RUN go mod download

# Copiar el resto del código fuente del proyecto
COPY . .

# Compilar el binario estático del worker asegurando compatibilidad con Linux
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/worker ./cmd/worker/main.go

# Etapa 2: Imagen final ligera para ejecución en producción
FROM alpine:latest

WORKDIR /app

# Instalar certificados raíz por si el worker realiza conexiones HTTPS externas
RUN apk --no-cache add ca-certificates

# Copiar el binario compilado desde la primera etapa
COPY --from=builder /app/bin/worker /app/worker

# Comando por defecto para ejecutar el motor asíncrono de notificaciones
CMD ["./worker"]
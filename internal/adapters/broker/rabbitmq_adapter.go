package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"event-notifier/internal/core/domain"
)

type RabbitMQAdapter struct {
	conn       *amqp.Connection
	ch         *amqp.Channel
	exchange   string
	queueName  string
	routingKey string
}

func NewRabbitMQAdapter(url string, exchange string, queueName string, routingKey string) (*RabbitMQAdapter, error) {
	// 1. Abrimos conexión TCP física
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("fallo al conectar con RabbitMQ en %s: %w", url, err)
	}

	// 2. Abrimos canal virtual sobre la conexión
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("fallo al abrir canal con RabbitMQ: %w", err)
	}

	// -------------------------------------------------------------
	// CONFIGURACIÓN DE LA DEAD LETTER QUEUE (DLQ)
	// -------------------------------------------------------------
	dlxExchange := exchange + "_dlx"
	dlqQueueName := queueName + "_dlq"
	dlqRoutingKey := routingKey + ".dead"

	// A. Declaramos el Dead Letter Exchange (DLX)
	err = ch.ExchangeDeclare(
		dlxExchange,
		"direct",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("fallo al declarar DLX: %w", err)
	}

	// B. Declaramos la Cola de Mensajes Muertos (DLQ)
	_, err = ch.QueueDeclare(
		dlqQueueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("fallo al declarar DLQ: %w", err)
	}

	// C. Enlazamos la DLQ con el DLX
	err = ch.QueueBind(
		dlqQueueName,
		dlqRoutingKey,
		dlxExchange,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("fallo al enlazar DLQ con DLX: %w", err)
	}

	// -------------------------------------------------------------
	// CONFIGURACIÓN DE LA COLA PRINCIPAL (Vinculada al DLX)
	// -------------------------------------------------------------
	// D. Declaramos el Exchange Principal
	err = ch.ExchangeDeclare(
		exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("fallo al declarar exchange principal: %w", err)
	}

	// E. Declaramos la Cola Principal con argumentos de redirección a DLX
	// amqp.Table define los metadatos 'x-dead-letter-*'
	queueArgs := amqp.Table{
		"x-dead-letter-exchange":    dlxExchange,
		"x-dead-letter-routing-key": dlqRoutingKey,
	}

	_, err = ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		queueArgs,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("fallo al declarar cola principal con DLX: %w", err)
	}

	// F. Enlazamos la Cola Principal al Exchange Principal
	err = ch.QueueBind(
		queueName,
		routingKey,
		exchange,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("fallo al enlazar cola principal: %w", err)
	}

	return &RabbitMQAdapter{
		conn:       conn,
		ch:         ch,
		exchange:   exchange,
		queueName:  queueName,
		routingKey: routingKey,
	}, nil
}

func (r *RabbitMQAdapter) Publicar(ctx context.Context, n *domain.Notificacion) error {
	cuerpoJSON, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("error al serializar notificación: %w", err)
	}

	mensaje := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         cuerpoJSON,
		MessageId:    n.ID,
	}

	return r.ch.PublishWithContext(
		ctx,
		r.exchange,
		r.routingKey,
		false,
		false,
		mensaje,
	)
}

func (r *RabbitMQAdapter) IniciarConsumo(ctx context.Context, handler func(n *domain.Notificacion) error) error {
	err := r.ch.Qos(1, 0, false)
	if err != nil {
		return fmt.Errorf("error configurando QoS: %w", err)
	}

	mensajes, err := r.ch.Consume(
		r.queueName,
		"",
		false, // autoAck: false obligatorio
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("error registrando consumidor: %w", err)
	}

	log.Printf("[RabbitMQ]: Consumidor escuchando en '%s' (DLQ activa en '%s_dlq')...", r.queueName, r.queueName)

	for {
		select {
		case <-ctx.Done():
			log.Println("[RabbitMQ]: Contexto cancelado. Cerrando bucle de consumo.")
			return nil

		case entrega, abierta := <-mensajes:
			if !abierta {
				log.Println("[RabbitMQ]: Canal cerrado por el broker.")
				return nil
			}

			var notif domain.Notificacion
			if err := json.Unmarshal(entrega.Body, &notif); err != nil {
				log.Printf("[RabbitMQ]: Mensaje corrupto. Descartando a DLQ: %v", err)
				// Nack(requeue = false): Envía automáticamente el mensaje corrupto a la DLQ
				entrega.Nack(false, false)
				continue
			}

			// Ejecutamos la función de procesamiento que incluye los reintentos
			if err := handler(&notif); err != nil {
				log.Printf("[RabbitMQ]: Fallo definitivo en notificación %s tras reintentos: %v", notif.ID, err)
				// ¡PUNTO CLAVE DE RESILIENCIA!:
				// Al agotar los reintentos, ejecutamos Nack(requeue: false).
				// Como la cola tiene configurado x-dead-letter-exchange, RabbitMQ NO destruye el mensaje,
				// sino que lo rutea a 'cola_notificaciones_dlq'.
				entrega.Nack(false, false)
				continue
			}

			// Confirmación exitosa
			entrega.Ack(false)
		}
	}
}

func (r *RabbitMQAdapter) Cerrar() error {
	if err := r.ch.Close(); err != nil {
		return err
	}
	return r.conn.Close()
}

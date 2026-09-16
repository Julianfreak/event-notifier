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

// NewRabbitMQAdapter conecta con RabbitMQ y asegura la infraestructura (Exchange, Cola y Binding)
func NewRabbitMQAdapter(url string, exchange string, queueName string, routingKey string) (*RabbitMQAdapter, error) {
	// 1. Abrimos la conexión TCP con el servidor RabbitMQ
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("fallo al conectar con RabbitMQ en %s: %w", url, err)
	}

	// 2. Abrimos un canal de comunicación multiplexado sobre la conexión TCP
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("fallo al abrir canal con RabbitMQ: %w", err)
	}

	// 3. Declaramos el Exchange (de tipo 'direct': enruta según coincidencia exacta de la routing key)
	// durable: true garantiza que si RabbitMQ se reinicia, el Exchange no se borra
	err = ch.ExchangeDeclare(
		exchange, // nombre del exchange
		"direct", // tipo
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("fallo al declarar exchange %s: %w", exchange, err)
	}

	// 4. Declaramos la Cola física
	// durable: true garantiza que los mensajes pendientes sobrevivan a caídas del broker
	_, err = ch.QueueDeclare(
		queueName, // nombre de la cola
		true,      // durable
		false,     // delete when unused
		false,     // exclusive (no se limita a una sola conexión)
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("fallo al declarar cola %s: %w", queueName, err)
	}

	// 5. Enlazamos la Cola con el Exchange mediante la Routing Key
	err = ch.QueueBind(
		queueName,  // cola destino
		routingKey, // clave de enrutamiento
		exchange,   // exchange origen
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("fallo al enlazar cola con exchange: %w", err)
	}

	return &RabbitMQAdapter{
		conn:       conn,
		ch:         ch,
		exchange:   exchange,
		queueName:  queueName,
		routingKey: routingKey,
	}, nil
}

// Publicar serializa la notificación a JSON y la entrega al Exchange
func (r *RabbitMQAdapter) Publicar(ctx context.Context, n *domain.Notificacion) error {
	cuerpoJSON, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("error al serializar notificación a JSON: %w", err)
	}

	// Creamos el mensaje con DeliveryMode: Persistent para que se guarde en disco
	mensaje := amqp.Publishing{
		DeliveryMode: amqp.Persistent, // Mensaje persistente en disco
		ContentType:  "application/json",
		Body:         cuerpoJSON,
		MessageId:    n.ID,
	}

	err = r.ch.PublishWithContext(
		ctx,
		r.exchange,   // exchange al que va dirigido
		r.routingKey, // clave de enrutamiento
		false,        // mandatory
		false,        // immediate
		mensaje,
	)
	if err != nil {
		return fmt.Errorf("error al publicar mensaje en RabbitMQ: %w", err)
	}

	return nil
}

// IniciarConsumo se queda escuchando la cola y procesa mensajes con Acuse de Recibo manual
func (r *RabbitMQAdapter) IniciarConsumo(ctx context.Context, handler func(n *domain.Notificacion) error) error {
	// Configuramos QoS (Quality of Service): prefetchCount = 1
	// Le dice a RabbitMQ: "No le entregues más de 1 mensaje a la vez a este worker hasta que dé Ack"
	err := r.ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return fmt.Errorf("error configurando QoS de RabbitMQ: %w", err)
	}

	// Registramos el consumidor con autoAck: false (Manual Ack obligatorio)
	mensajes, err := r.ch.Consume(
		r.queueName, // nombre de la cola
		"",          // consumer tag (generado automáticamente)
		false,       // autoAck: false (clave para no perder mensajes)
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return fmt.Errorf("error al registrar consumidor en cola %s: %w", r.queueName, err)
	}

	log.Printf("[RabbitMQ]: Consumidor escuchando en cola '%s'...", r.queueName)

	// Bucle continuo de lectura de mensajes
	for {
		select {
		case <-ctx.Done():
			log.Println("[RabbitMQ]: Contexto cancelado. Deteniendo consumo.")
			return nil

		case entrega, abierta := <-mensajes:
			if !abierta {
				log.Println("[RabbitMQ]: Canal de mensajes cerrado.")
				return nil
			}

			// Deserializamos el JSON entrante a la entidad de dominio
			var notif domain.Notificacion
			if err := json.Unmarshal(entrega.Body, &notif); err != nil {
				log.Printf("[RabbitMQ]: Mensaje corrupto descartado: %v", err)
				// Nack(false, false) descarta el mensaje sin reencolarlo si no es un JSON válido
				entrega.Nack(false, false)
				continue
			}

			// Ejecutamos la función de negocio del handler
			if err := handler(&notif); err != nil {
				log.Printf("[RabbitMQ]: Error procesando notificación %s: %v. Reencolando...", notif.ID, err)
				// Si la lógica falló por red externa, Nack(false, true) le pide a RabbitMQ reencolar el mensaje
				entrega.Nack(false, true)
				continue
			}

			// Si el handler terminó exitosamente, confirmamos a RabbitMQ
			entrega.Ack(false)
		}
	}
}

// Cerrar libera los recursos de red de forma limpia
func (r *RabbitMQAdapter) Cerrar() error {
	if err := r.ch.Close(); err != nil {
		return err
	}
	return r.conn.Close()
}

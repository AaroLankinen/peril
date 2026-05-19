package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SimpleQueueType defines if a queue is durable or transient/exclusive.
type SimpleQueueType int

const (
	// SimpleQueueDurable is a shared, persistent queue.
	SimpleQueueDurable SimpleQueueType = iota
	// SimpleQueueTransient is an exclusive, auto-delete queue.
	SimpleQueueTransient
)

// AckType represents the acknowledgement action for a consumed message.
type AckType int

const (
	// Ack acknowledges the message, removing it from the queue.
	Ack AckType = iota
	// NackRequeue negatively acknowledges the message and puts it back on the queue.
	NackRequeue
	// NackDiscard negatively acknowledges the message and discards it (or sends to DLX).
	NackDiscard
)

// DeadLetterExchange is the name of the exchange for discarded messages.
const DeadLetterExchange = "peril_dlx"

// PrefetchCount limits the number of unacknowledged messages on a channel.
const PrefetchCount = 100

// DeclareAndBind is a helper that creates a channel, declares a queue with the
// specified properties, and binds it to an exchange with a routing key.
func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := ch.QueueDeclare(
		queueName,                               // name
		simpleQueueType == SimpleQueueDurable,   // durable
		simpleQueueType == SimpleQueueTransient, // autoDelete
		simpleQueueType == SimpleQueueTransient, // exclusive
		false,                                   // noWait
		amqp.Table{"x-dead-letter-exchange": DeadLetterExchange}, // args
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return ch, queue, nil
}

// PublishJSON marshals a value to JSON and publishes it to the specified exchange and key.
func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	valAsBytes, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        valAsBytes,
	})

}

// SubscribeJSON is a helper function that combines DeclareAndBind and Consume to
// subscribe to JSON messages. It runs the consumer in a background goroutine
// and executes the handler for each message.
func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	// Ensure the channel is closed when the function exits, or when the goroutine finishes
	// This defer should ideally be inside the goroutine if the channel is exclusive to the consumer,
	// but for simplicity and common patterns, it's often placed here if the channel is shared or managed by the caller.
	// For a single consumer per channel, closing it when the consumer stops is good practice.
	// However, for this specific use case, the channel is passed in, so it's up to the caller to close it.
	ch.Qos(PrefetchCount, 0, true) // Adjust the prefetch count as needed
	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			var val T
			err := json.Unmarshal(msg.Body, &val)
			if err != nil {
				// If unmarshaling fails, NackDiscard the message as it's likely malformed
				msg.Nack(false, false)
				return // Skip processing this message
			}
			ackType := handler(val)
			switch ackType {
			case Ack:
				msg.Ack(false)
			case NackRequeue:
				msg.Nack(false, true) // Requeue the message
			case NackDiscard:
				msg.Nack(false, false) // Discard the message
			}
		}
	}()

	return nil
}

// PublishGob serializes a value using GOB and publishes it to the specified exchange and key.
func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        buf.Bytes(),
	})
}

// SubscribeGob is a helper function that combines DeclareAndBind and Consume to
// subscribe to GOB messages. It runs the consumer in a background goroutine
// and executes the handler for each message.
func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	ch.Qos(PrefetchCount, 0, true) // Adjust the prefetch count as needed
	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			var val T
			buf := bytes.NewBuffer(msg.Body)
			dec := gob.NewDecoder(buf)
			err := dec.Decode(&val)
			if err != nil {
				// If decoding fails, NackDiscard the message as it's likely malformed
				msg.Nack(false, false)
				return // Skip processing this message
			}
			ackType := handler(val)
			switch ackType {
			case Ack:
				msg.Ack(false)
			case NackRequeue:
				msg.Nack(false, true) // Requeue the message
			case NackDiscard:
				msg.Nack(false, false) // Discard the message
			}
		}
	}()

	return nil
}

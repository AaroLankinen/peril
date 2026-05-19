package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

const DeadLetterExchange = "peril_dlx"

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

/*
funcSubscribeJSON is a helper function that combines DeclareAndBind and Consume to subscribe to messages of a specific type from a RabbitMQ exchange.
It takes a handler function that will be called with the deserialized message value whenever a new message is received.
*/
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

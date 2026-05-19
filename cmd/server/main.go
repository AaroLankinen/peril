// Package main for the server acts as the game orchestrator.
// It manages global game state such as pausing/resuming and
// acts as a centralized logger for all client events.
package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerLog() func(routing.GameLog) pubsub.AckType {
	return func(gl routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gl)
		if err != nil {
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}

func main() {
	connectionString := "amqp://guest:guest@localhost:5672/"
	fmt.Println("Starting Peril server...")
	fmt.Println("Using connection string:", connectionString)
	AMQPConnection, error := amqp.Dial(connectionString)
	if error != nil {
		panic(error)
	}
	fmt.Println("Connected to RabbitMQ server...")
	channel, error := AMQPConnection.Channel()
	if error != nil {
		panic(error)
	}
	defer channel.Close()
	defer AMQPConnection.Close()
	defer fmt.Println("Peril server is shutting down...")

	err := pubsub.SubscribeGob(
		AMQPConnection,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.SimpleQueueDurable,
		handlerLog(),
	)
	if err != nil {
		panic(err)
	}

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("Sending pause message...")
			err := pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true},
			)
			if err != nil {
				fmt.Printf("error publishing message: %v\n", err)
			}
		case "resume":
			fmt.Println("Sending resume message...")
			err := pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false},
			)
			if err != nil {
				fmt.Printf("error publishing message: %v\n", err)
			}
		case "quit":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Command not recognized")
		}
	}
}

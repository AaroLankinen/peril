package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connectionString := "amqp://guest:guest@localhost:5672/"
	AMQPConnection, err := amqp.Dial(connectionString)
	if err != nil {
		panic(err)
	}
	defer AMQPConnection.Close()
	fmt.Println("Connected to RabbitMQ server...")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		panic(err)
	}

	channel, queue, err := pubsub.DeclareAndBind(
		AMQPConnection,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+username,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
	)
	if err != nil {
		panic(err)
	}
	defer channel.Close()
	fmt.Printf("Queue %v declared and bound to %v\n", queue.Name, routing.PauseKey)

	gameState := gamelogic.NewGameState(username)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err := gameState.CommandSpawn(words)
			if err != nil {
				fmt.Printf("error: %v\n", err)
			}
		case "move":
			_, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Printf("error: %v\n", err)
			} else {
				fmt.Println("Move successful!")
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unrecognized command")
		}
	}
}

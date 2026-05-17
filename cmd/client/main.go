package main

import (
	"bufio"
	"fmt"
	"os"

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

	// Wait for user input to exit
	fmt.Println("Press Enter to exit...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}

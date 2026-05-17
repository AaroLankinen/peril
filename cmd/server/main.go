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

	gamelogic.PrintServerHelp()
	pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	// Wait for user input to exit
	fmt.Println("Press Enter to exit...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}

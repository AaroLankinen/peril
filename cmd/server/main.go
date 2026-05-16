package main

import (
	"bufio"
	"fmt"
	"os"

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
	defer AMQPConnection.Close()
	defer fmt.Println("Peril server is shutting down...")
	fmt.Println("Peril server is running...")
	// Wait for user input to exit
	fmt.Println("Press Enter to exit...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}

# Peril - Multi-player Strategy Game

Peril is a backend implementation of a multi-player strategy game that leverages RabbitMQ for distributed communication between a central server and multiple game clients.

## Architecture

The system is composed of three main components:

1.  **RabbitMQ Broker**: Handles message routing and delivery using various exchange types (`direct` and `topic`).
2.  **Peril Server**: 
    *   Sends global game commands (Pause/Resume) to all clients.
    *   Consumes and persists game logs from all clients into a local `game.log` file.
    *   Uses GOB serialization for efficient log transmission.
3.  **Peril Client**:
    *   Represents a unique player.
    *   Publishes army moves and reacts to moves from other players.
    *   Handles "War" logic when units collide, publishing results back to the server.
    *   Supports a `spam` command for backpressure and load testing.

## Communication Flow

*   **Pausing**: The server publishes to a `direct` exchange (`peril_direct`). Each client has an exclusive queue bound to the `pause` key.
*   **Moves**: Clients publish moves to a `topic` exchange (`peril_topic`) using `army_moves.<username>`. Clients subscribe using a wildcard `army_moves.*` to see everyone's actions.
*   **War**: When collisions occur, clients publish to the `war` topic. This queue is **durable and shared**, meaning only one client processes a specific war event (round-robin).
*   **Logging**: Clients send activity logs to the server using the `game_logs.<username>` routing key.

## Setup and Execution

### Prerequisites

*   Go 1.20+
*   RabbitMQ server running locally (default port 5672)

### Running the Server

```bash
go run ./cmd/server
```
Available server commands:
*   `pause`: Pause the game for all players.
*   `resume`: Resume the game.
*   `quit`: Shut down the server.

### Running a Client

```bash
go run ./cmd/client
```
Available client commands:
*   `spawn <location> <rank>`: Spawn a new unit.
*   `move <unit_id> <location>`: Move a unit to a new coordinate.
*   `status`: View your current game state.
*   `spam <n>`: Publish N malicious logs to test server backpressure.
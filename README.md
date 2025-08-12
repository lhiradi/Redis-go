[![progress-banner](https://backend.codecrafters.io/progress/redis/9cbac660-a366-41b5-a7ca-aad0b01117ab)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)



# Redis Go (Redis Clone in Go)

## Overview
This repository provides a **toy Redis server** written in **Go**. It implements a subset of the Redis protocol and data structures, including:
> **Note:** This is a challenge in codecrafters, you can explore it with your own choosing language (https://app.codecrafters.io/courses/redis/overview)

- Core commands: `PING`, `GET`, `SET`, `INCR`, etc.
- Data types: **strings**, **streams**, **lists**.
- Replication (master/replica) support.
- RDB file parsing and persistence.
- Pub/Sub (publish/subscribe) functionality.

The project is intentionally minimal, serving as a learning tool for understanding how Redis works internally and as a starting point for extending functionality.

> **Note:** This is **not** a production-ready Redis server. It is meant for educational purposes and to help you explore Go networking and concurrency.

---

## Features

| Feature | Description |
|---------|-------------|
| **Basic key‑value** | `SET`, `GET`, `DEL`, `INCR`, `EXPIRE` |
| **Streams** | `XADD`, `XRANGE`, `XREAD` |
| **Lists** | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `BLPOP` (blocking pop) |
| **Replication** | Master/replica (replication offsets, ACKs, full sync) |
| **Persistence** | RDB file loading and empty‑RDB generation for replication |
| **Pub/Sub** | `SUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE` |
| **Transaction** | `MULTI`, `EXEC`, `DISCARD`, command queuing |
| **RESP Protocol** | Fully supports RESP serialization & parsing |
| **Tests** | The code base includes unit tests for most components (not included in this snippet). |

---

## Getting Started

### Prerequisites

- **Go 1.24** (or newer)
- A POSIX‑compatible system (Linux/macOS/WSL)

### Clone the repository

```sh
git clone https://github.com/codecrafters-io/redis-starter-go
cd redis-starter-go
```

> The repository is organized under the `app/` folder.

---

## Building

The repository includes a `your_program.sh` helper that builds the server into a temporary binary.

```sh
./your_program.sh
```

The compiled binary is placed at `/tmp/codecrafters-build-redis-go`.

---

## Running

```sh
# Run as a master (default)
./your_program.sh -port 6379 -dir /tmp -dbfilename redis-data.rdb
```

### Replica mode

```sh
# Connect to a master at 127.0.0.1:6379
./your_program.sh -port 6380 -replicaof "127.0.0.1 6379"
```

- `-port`    – TCP port to listen on  
- `-replicaof` – `"host port"` for the master (e.g., `127.0.0.1 6379`)  

---

## Commands supported

### Core

| Command | Description |
|---------|------------|
| `PING` | Health check – returns `+PONG` |
| `SET key value [EX seconds]` | Store a string (optional TTL) |
| `GET key` | Retrieve a string |
| `INCR key` | Increment integer value |
| `DEL key` | Remove a key |
| `EXPIRE key seconds` | Set TTL on a key |

### Lists

| Command | Description |
|--------|------------|
| `LPUSH key element [element ...]` | Push values to the left |
| `RPUSH key element [element ...]` | Push values to the right |
| `LPOP key [count]` | Pop from left |
| `RPOP key [count]` | Pop from right |
| `BLPOP key timeout` | Blocking left pop (client blocks until element arrives or timeout) |

### Streams

| Command | Description |
|--------|------------|
| `XADD key ID field value [field value ...]` | Append entry to stream |
| `XRANGE key start end` | Range query |
| `XREAD key ID` | Read entries after a given ID |
| `XREADGROUP` (not yet implemented) | Placeholder for future group reads |

### Replication

- **Master** – handles client commands and replicates them to connected replicas.
- **Replica** – performs handshake, receives RDB snapshot, and stays in sync.

**Key replication commands**:

- `REPLCONF listening-port <port>`
- `REPLCONF capa psync2`
- `PSYNC ? -1` (full resync)
- `REPLCONF ACK <offset>`
- `REPLCONF GETACK`

---

## Project Structure

```
├─ .codecrafters          # Build & run scripts for the challenge platform
├─ app/
│   ├─ internal/
│   │   ├─ db/            # DB structures, RDB parser, replication
│   │   ├─ exchange/    # Pub/Sub implementation
│   │   ├─ handlers/    # Command handling and replication handshake
│   │   ├─ server/      # TCP server & connection handling
│   │   ├─ transaction/   # Transaction support
│   │   └─ utils/        # Helpers: ID generation, RESP formatting, etc.
│   └─ main.go           # Entry point
├─ go.mod / go.sum
└─ your_program.sh     # Convenience script for local run
```

---

## Testing

The project includes unit tests for many components (`*_test.go`). To run them:

```sh
go test ./...
```

> Tests use the standard `testing` package; they cover database operations, RDB parsing, and stream ID validation.

---

## Extending the Server

The codebase is deliberately modular. To add a new command:

1. **Add a handler** in `app/internal/handlers/command_handler.go`.
2. **Register the command** in `app/internal/handlers/router.go`.
3. **Add tests** in a new `_test.go` file.
4. (Optional) Update the **README** to document the new command.

---

## Contributing

Contributions are welcome! If you find a bug or want to add a feature, feel free to:

1. Fork the repository.
2. Create a new branch.
3. Open a Pull Request describing your changes.

---

## License

This project is licensed under the **MIT License**. See the `LICENSE` file for details.

---

Happy hacking! 🚀



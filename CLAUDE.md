# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
# Build
cargo build [--release]

# Run the block server v2
cargo run --bin block_server_v2 -- [options]

# Test
cargo test
```

**Usage:**
```
block_server_v2
  --logDir <directory>        Log directory (auto-rotates daily)
  --cpus <ranges>             CPU affinity (e.g., "0-3,5-6"), default: all
  --enableMqtt <0|1>          Enable MQTT last-will (default: 1)
  --mqttAddr <host>           MQTT broker (default: 127.0.0.1)
  --mqttPort <port>           MQTT port (default: 1883)
  --mqttUsername <name>       MQTT username
  --mqttPwd <password>        MQTT password
  --mqttSoftwareName <name>   Software name in MQTT (default: block_server)
  --mqttClientId <id>         MQTT client ID
```

## Architecture

**block_server** is a high-performance data server for raw signal files:

### Core Components

| File | Purpose |
|------|---------|
| `src/bin/block_server_v2.rs` | Main server: TCP listener on port 30002, serves binary signal data |
| `src/net/mod.rs` | Network protocol: 4-byte length-prefixed JSON messages |
| `src/net/protocol.rs` | Data structures: `ClientFpReq`, `ClientDataReq`, `DataMetaResp` |
| `src/lib.rs` | Shared types re-exported |
| `src/file/mod.rs` | File utility functions |
| `src/mqtt_last_will.rs` | MQTT LWT (Last Will Testament) for health monitoring |

### Protocol

1. **Client connects** → sends `{"FP": "/path/to/file"}` (4-byte length prefix)
2. **Server responds** with file metadata from file header
3. **Client sends** `ClientDataReq` with channel range, batch size, data offsets
4. **Server streams** data in batches with back-pressure support (optional)

### Key Design

- **Multi-threaded tokio runtime**: I/O threads + blocking threads with CPU affinity
- **Back-pressure**: Client can send ACK bytes to throttle server
- **MQTT health monitoring**: Publishes online/offline status with LWT
- **Log rotation**: Daily rolling logs in specified directory

### Python Clients

- `data_request_only_v2_multiprocess.py` — multi-process bandwidth test client
- `binary_check.py` — file validation
- `h5_to_binary.py` — data conversion utilities

## Development Notes

- Edition 2024 (Rust 2024 preview)
- Uses `tracing` for structured logging with `tracing-appender` for file output
- Memory-mapped file access not used—sequential reads with seek for interleaved channel access

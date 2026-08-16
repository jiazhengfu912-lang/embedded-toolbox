# Embedded Toolbox V1 Architecture

## Runtime ownership

```text
AppCore actor / critical section
  ├─ StateStore: runtimeInstanceId, EventCursor, Snapshot, 4096 events
  ├─ SourceRegistry<SourceId, SourceRuntime>
  │    ├─ Transport reader
  │    ├─ SessionClock + connection Epoch
  │    ├─ TxScheduler
  │    ├─ RecorderQueue ─────────► SessionManager / SQLite
  │    ├─ ParserQueue ─► Pipeline ─► Packet/Sample IPC
  │    └─ TerminalQueue ──────────► Raw IPC
  ├─ DataHub: independent Tauri raw-channel subscribers
  └─ SessionManager: Session/Epoch state and WAL lifecycle
```

状态更新和 EventCursor 分配只发生在 `StateStore` 的同一个锁内。因此 `app_get_snapshot` 返回的状态与 `eventCursor` 对应。控制事件不会占用 Raw、Packet 或 Sample 的序号。

## Queue contracts

| Consumer | Hard limit | Overflow action |
|---|---:|---|
| Recorder | 16 MiB / 4096 chunks | stop this recording, emit `E_RECORDING_BACKPRESSURE` |
| Parser | 4 MiB / 2048 chunks | clear to low water, mark StreamGap, reset parser state |
| Terminal | 2 MiB / 1024 chunks | drop oldest display chunks and count truncation |
| Sample/Packet IPC | 4 MiB / 256 batches | drop oldest visual batch |
| TX | 1 MiB / 1024 jobs | reject with `E_TX_QUEUE_FULL` |

Queues account for bytes and item count separately. Recorder, parser and UI workers never wait on one another.

## Control-plane synchronization

The React client performs this sequence:

1. Subscribe to `app_subscribe_events` and buffer arrivals.
2. Call `app_get_snapshot`.
3. Apply the snapshot and discard buffered events through its cursor.
4. Apply later events in cursor order.
5. Fetch a full snapshot after a gap, expired cursor or changed `runtimeInstanceId`.

The event ring retains 4096 control events. High-frequency stream messages use:

```text
magic ETBX | envelopeVersion | payloadType | payloadVersion
sourceId | sourceEpoch | sequence | monotonicOffsetNs | payloadLength | payload
```

Envelope and payload versions are validated independently. Raw payloads remain binary; Packet, Sample and Diagnostic payloads are JSON inside the binary envelope for V1.

## Epoch and time semantics

Every successful transport open increments `sourceEpoch` and creates a UUIDv7 `epochId`. Unique data identity is:

```text
runtimeInstanceId + sourceId + sourceEpoch + sequence
```

A recording Session may span multiple connection Epochs. Disconnect closes the active Epoch, cancels its TX work, adds a StreamGap and suspends the Session. Reconnect of the same logical Source resumes recording with a new Epoch. Framer partial bytes and all stateful Transform state are reset at boundaries.

Session and Epoch rows store UTC and monotonic anchors. Raw chunks store Epoch-local monotonic offsets. Replay scheduling uses monotonic offsets; UTC presentation is rebuilt from the Epoch anchor.

## SQLite lifecycle

- `journal_mode=WAL`, `synchronous=FULL`, `wal_autocheckpoint=0`.
- Recorder commits at 100 ms or 64 KiB, whichever happens first.
- Recorder worker performs PASSIVE checkpoint every 30 s or at 32 MiB WAL.
- `session_stop` stops admissions, drains RecorderQueue, commits final Epoch/Session state, releases statements/readers, retries TRUNCATE checkpoint within five seconds, then closes.
- A busy or failed final checkpoint keeps `.etdb`, `-wal` and `-shm` intact and returns the specific checkpoint error.
- Opening a prior interrupted database performs WAL recovery before another TRUNCATE attempt. WAL presence alone is never corruption evidence.

## Frontend memory contracts

- Terminal state: 16 MiB or 100,000 chunks; DOM renders only the newest 2,000.
- Packets state: 32 MiB or 50,000 packets; table renders only the newest 2,000 matches.
- Plot/PID: 200,000 points per channel, 1,000,000 total, 64 MiB allocation ceiling.
- Diagnostics: 2,000 entries.
- Samples live in dynamically growing TypedArray circular buffers. Each plot refresh reads at most two min/max points per pixel bucket per channel.

These limits only evict UI history. Recorder fidelity is governed solely by RecorderQueue and session errors.


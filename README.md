# FalconDB

A distributed key-value database built in Node.js, implementing the **Raft consensus algorithm** for leader election and **Two-Phase Commit (2PC)** for distributed writes. Designed as a learning/experimental system for understanding distributed database fundamentals.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Directory Structure](#directory-structure)
3. [Dependencies](#dependencies)
4. [Configuration](#configuration)
5. [Common Modules](#common-modules)
6. [Data Nodes (DN)](#data-nodes-dn)
7. [Reverse Proxy (RP)](#reverse-proxy-rp)
8. [Raft Consensus — Deep Dive](#raft-consensus--deep-dive)
9. [Two-Phase Commit (2PC) — Deep Dive](#two-phase-commit-2pc--deep-dive)
10. [Sharding & Consistent Hashing](#sharding--consistent-hashing)
11. [Storage Layer](#storage-layer)
12. [API Reference](#api-reference)
13. [Data Flow Walkthroughs](#data-flow-walkthroughs)
14. [Logging](#logging)
15. [Startup & Lifecycle](#startup--lifecycle)
16. [Running with Podman](#running-with-podman)
17. [Error Code System](#error-code-system)
18. [Known Limitations & Design Decisions](#known-limitations--design-decisions)

---

## Architecture Overview

```
                        ┌─────────────────────────────────────┐
                        │           Client (HTTP)              │
                        └────────────────┬────────────────────┘
                                         │ :8000
                        ┌────────────────▼────────────────────┐
                        │         Reverse Proxy (RP)           │
                        │   - Routes requests by shard key      │
                        │   - Orchestrates 2PC for writes       │
                        │   - Tracks current leader per shard   │
                        └────┬───────────┬───────────┬────────┘
                             │           │           │
                    :9001    │   :9002   │   :9003   │
          ┌──────────────────▼┐  ┌──────▼──────┐  ┌▼─────────────────┐
          │   Data Node 1      │  │ Data Node 2  │  │  Data Node 3      │
          │   (dn0s1)          │  │ (dn0s2)      │  │  (dn0s3)          │
          │                    │  │              │  │                   │
          │  ┌──────────────┐  │  │              │  │                   │
          │  │  Raft State  │  │  │  Raft State  │  │  Raft State       │
          │  │  - Leader    │  │  │  - Follower  │  │  - Follower       │
          │  │  - Term      │  │  │  - Term      │  │  - Term           │
          │  └──────────────┘  │  └──────────────┘  └───────────────────┘
          │                    │
          │  ┌──────────────┐  │
          │  │  File Store  │◄─┼─── Replication ──────────────────────►
          │  │  DBdata/*.json│  │
          │  └──────────────┘  │
          └────────────────────┘
```

FalconDB has three layers:

| Layer | Component | Role |
|---|---|---|
| Entry point | Reverse Proxy (`RP/server.js`) | Single HTTP endpoint for all client requests |
| Coordination | Raft consensus between DNs | Elects a single leader, routes all writes through it |
| Storage | File System DB (`common/fsdb.js`) | Persists each key-value pair as a JSON file on disk |

All three Data Nodes hold the same data (full replication, no data partitioning despite the sharding module existing).

---

## Directory Structure

```
falconDB/
├── package.json              # npm manifest — dependencies and scripts
├── etc/
│   └── configure.json        # Topology config (currently unused at runtime)
├── common/
│   ├── fsdb.js               # File-system key-value storage primitives
│   ├── logger.js             # Winston logger factory
│   ├── response.js           # Standardized success/failure response builders
│   └── shard.js              # Key-to-DN routing via consistent hashing
├── RP/
│   └── server.js             # Reverse proxy — port 8000
├── DN/
│   ├── dn0s1/server.js       # Data Node 1 — port 9001
│   ├── dn0s2/server.js       # Data Node 2 — port 9002
│   └── dn0s3/server.js       # Data Node 3 — port 9003
└── DBdata/
    └── *.json                # One file per key (named by MD5 hash of the key)
```

---

## Dependencies

| Package | Version | Why it's used |
|---|---|---|
| `express` | v5.2.1 | HTTP server framework for both RP and DN endpoints |
| `axios` | v1.16.0 | HTTP client for inter-service calls (RP→DN, DN→DN) |
| `md5` | v2.3.0 | Hashes keys deterministically for file naming and sharding |
| `winston` | v3.19.0 | Structured logging to both file and console |
| `forever` | v4.0.3 | Process manager — keeps services alive if they crash |

---

## Configuration

**`etc/configure.json`** — static topology definition:

```json
{
  "reverse_proxy": {
    "host": "127.0.0.1",
    "port": 8000
  },
  "dns": [
    {
      "id": 0,
      "servers": [
        { "id": "dn0s1", "host": "127.0.0.1", "port": 9001 },
        { "id": "dn0s2", "host": "127.0.0.1", "port": 9002 },
        { "id": "dn0s3", "host": "127.0.0.1", "port": 9003 }
      ]
    }
  ]
}
```

> **Note:** This file is not loaded at runtime. All addresses and ports are hardcoded inside each server file. This is a known limitation — see [Known Limitations](#known-limitations--design-decisions).

The only runtime configuration is the `PORT` environment variable, set per DN:

| Node | `PORT` value |
|---|---|
| dn0s1 | 9001 |
| dn0s2 | 9002 |
| dn0s3 | 9003 |

---

## Common Modules

### `common/fsdb.js` — File System Database

The lowest layer. Stores every key-value pair as an individual JSON file under `DBdata/`.

**Key → filename mapping:**

```
key "user1"  →  MD5("user1") = "24c9e15e52afc47c225b757e7bee1f9d"
             →  DBdata/24c9e15e52afc47c225b757e7bee1f9d.json
```

Using MD5 for filenames means:
- Filenames are fixed-length (32 hex chars) regardless of key length.
- The same key always maps to the same file (deterministic).
- No filename collisions for different keys (MD5 collision risk is negligible at this scale).

**File content format:**

```json
{
  "key": "user1",
  "value": { "name": "joao", "age": 30 }
}
```

The `value` field is any JSON-serializable type: object, string, number, array, boolean.

**API:**

| Function | Signature | Behavior |
|---|---|---|
| `getFile(key)` | `key → string` | Returns full path to the JSON file for this key |
| `create(key, value)` | `(key, value) → void` | Writes `{key, value}` to disk (overwrites if exists) |
| `read(key)` | `key → {key, value} \| null` | Reads and parses the file; returns `null` if missing |
| `remove(key)` | `key → void` | Deletes the file for the key |

There is no `update` function — `create` is used for both create and update operations (it overwrites).

---

### `common/logger.js` — Logger Factory

Wraps Winston to produce a pre-configured logger per service:

```js
const logger = createLogger('dn0s1.log');
logger.info('server started');
logger.debug('received vote from 9002');
```

Configuration applied to every logger:
- **Level:** `debug` (all messages including debug are recorded)
- **Format:** Timestamp + simple (human-readable, not JSON)
- **Transports:** File (appends to named file) + Console (stdout)

---

### `common/response.js` — Response Formatter

Every HTTP response from RP and DN follows one of two shapes:

**Success:**
```json
{
  "data": { "key": "user1", "value": { "name": "joao" } },
  "error": 0
}
```

**Failure:**
```json
{
  "data": 0,
  "error": {
    "code": "eDNCRUD002",
    "errno": 0,
    "message": "key not found"
  }
}
```

**Functions:**

| Function | Signature |
|---|---|
| `success(data)` | Returns `{ data, error: 0 }` |
| `failure(code, message, errno)` | Returns `{ data: 0, error: { code, errno, message } }` |

---

### `common/shard.js` — Sharding / Routing

Determines which DN group a key belongs to:

```js
getDN(key, totalDNs)
```

**Algorithm:**
1. Hash the key with MD5 → 32-character hex string
2. Take the first 8 hex characters
3. Parse as a base-16 integer
4. `integer % totalDNs` = shard index (0-based)

**Example:**
```
key = "user1"
MD5("user1") = "24c9e15e52afc47c225b757e7bee1f9d"
First 8 chars = "24c9e15e" → 616268126 (decimal)
616268126 % 1 = 0  ← shard 0 (only one DN group exists)
```

Currently `totalDNs` is always `1` (there is only one DN group — `dn0`), so all keys map to shard 0. The infrastructure exists to support multiple shards, but is not used.

---

## Data Nodes (DN)

Each DN is a self-contained Express server. All three are **identical code** — differentiated only by their `PORT` environment variable and their hardcoded peer lists.

### State per Node

```js
let state = 'follower';        // 'follower' | 'candidate' | 'leader'
let currentTerm = 0;           // Raft logical clock — increments on each election
let votedFor = null;           // Port of candidate this node voted for in currentTerm
let leader = null;             // Port of the known current leader
let lastHeartbeat = Date.now(); // Timestamp of last heartbeat received (ms)

const ELECTION_TIMEOUT = random between 5000ms and 10000ms
```

These are **in-memory only** — not persisted to disk. A node restart resets all Raft state.

### Operation Statistics

Each node independently counts operations it has processed:

```js
const stats = { create: 0, read: 0, update: 0, delete: 0 };
```

---

## Reverse Proxy (RP)

**Port:** 8000

The RP is the single entry point for all external clients. It:

1. **Routes** every request to the correct DN group using `shard.getDN(key)`.
2. **Tracks the current leader** per shard — updated via `POST /set_master` when a DN wins an election.
3. **Orchestrates 2PC** for all write operations (create, update, delete).
4. **Forwards reads** directly to the shard leader.

### RP State

```js
const leaders = {
  0: 'http://127.0.0.1:9001'   // shard_id → leader URL
};

const stats = { create: 0, read: 0, update: 0, delete: 0 };
```

When a new leader is elected, it calls `POST /set_master` and `leaders[dnId]` is updated.

---

## Raft Consensus — Deep Dive

Raft is a consensus protocol that guarantees a single leader among a cluster of nodes. FalconDB uses it to ensure only one DN accepts writes at any time.

### Node States

```
            timeout / no heartbeat
Follower ──────────────────────────► Candidate
    ▲                                     │
    │ receive heartbeat from leader        │ win majority vote
    │                                     ▼
    └──────────────────────────────── Leader
              step down (term < peer's term)
```

- **Follower** — passive; waits for heartbeats; can vote in elections.
- **Candidate** — actively seeking votes; increments term and solicits peers.
- **Leader** — the single authoritative node; sends heartbeats; handles all writes.

### Election Trigger

A background loop (`startElectionMonitor`) runs every **3 seconds** on every non-leader node. It checks:

```
if (now - lastHeartbeat > ELECTION_TIMEOUT) → startElection()
```

`ELECTION_TIMEOUT` is randomized between 5–10 seconds per node to reduce simultaneous elections (split votes).

There is an initial **5-second grace period** after startup before the monitor begins, allowing all nodes to come online before elections start.

### Election Process (`startElection`)

1. Transition to `candidate`.
2. Increment `currentTerm`.
3. Vote for self (`votedFor = ownPort`).
4. Send `GET /election?term=<currentTerm>` to both peers.
5. Each peer responds `{ vote: true }` if `term > peer.currentTerm` (and peer hasn't voted this term).
6. Count votes. Need **≥ 2** (majority of 3) to win.
7. **If won:**
   - Set `state = 'leader'`.
   - Call `POST /set_master` on the RP with own port and URL.
   - Start `startHeartbeat()` loop.
8. **If lost:** revert to `follower`.

### Heartbeat Mechanism (`startHeartbeat`)

The leader sends `POST /heartbeat { leaderId: ownPort }` to both peers **every 2 seconds**.

When a follower receives a heartbeat:
- Updates `lastHeartbeat = Date.now()`.
- Sets `state = 'follower'`.
- Records `leader = leaderId`.

This resets the election timeout clock. If a leader crashes, followers stop receiving heartbeats. After `ELECTION_TIMEOUT` ms elapses, one of them triggers a new election.

### Vote Logic (`GET /election`)

A node grants a vote if:
- The requesting term is **greater than** its own `currentTerm`.
- It has not already voted in this term for someone else.

It updates its own `currentTerm` and `votedFor` before responding.

---

## Two-Phase Commit (2PC) — Deep Dive

2PC ensures that writes are either committed on **all** replicas or on **none**. FalconDB uses an optimistic version — Phase 1 always succeeds (no actual locking), but the pattern provides a clean two-step hook for future enhancement.

### Phase 1 — Prepare (`POST /prepare`)

The RP calls this on the leader before any write. The leader returns success unconditionally. This is the gate — if the leader is unreachable, the write is aborted before any data changes.

### Phase 2 — Commit (`POST /commit`) or Delete (`POST /delete`)

Only called if Phase 1 succeeded. The leader:

1. Writes the data locally via `fsdb.create(key, value)`.
2. Asynchronously replicates to all followers via `POST /replicate` (for writes) or `POST /delete` (for deletes).
3. Returns success to the RP.

**For delete:** the RP sends `POST /prepare` first, then `POST /delete` (which both deletes locally and replicates).

### Replication (`POST /replicate`)

Followers receive `{ key, value }` and call `fsdb.create(key, value)` locally. This is **fire-and-forget** from the commit handler — the leader does not wait for follower acknowledgement before returning success.

This means replication is **eventually consistent** — there is a window where the leader has committed but followers haven't yet replicated.

### 2PC Flow Diagram

```
RP                    Leader (DN1)          Follower (DN2)    Follower (DN3)
│                          │                      │                 │
│ POST /prepare             │                      │                 │
│──────────────────────────►│                      │                 │
│       { ok }              │                      │                 │
│◄──────────────────────────│                      │                 │
│                          │                      │                 │
│ POST /commit {key,value}  │                      │                 │
│──────────────────────────►│                      │                 │
│                          │ fsdb.create(key,val)  │                 │
│                          │──────────────────────►│ POST /replicate │
│                          │──────────────────────────────────────► │
│       { ok }              │  (async, no wait)    │                 │
│◄──────────────────────────│                      │                 │
│                          │                      │                 │
```

---

## Sharding & Consistent Hashing

The sharding module (`common/shard.js`) uses MD5-based consistent hashing to route keys to DN groups.

```
shard_id = parseInt(MD5(key).substring(0, 8), 16) % totalDNGroups
```

In the current deployment there is only **1 DN group** (`dn0`, containing `dn0s1`/`dn0s2`/`dn0s3`), so `totalDNGroups = 1` and every key maps to shard 0. The leader of that group handles all requests.

The architecture is designed to scale horizontally — adding a second DN group (`dn1`) would automatically split the keyspace between the two groups based on the hash.

---

## Storage Layer

**Location:** `DBdata/` directory (relative to where the DN server process runs)

**File per key:** Each key maps to exactly one file, named by `MD5(key)`:

```
DBdata/
├── 24c9e15e52afc47c225b757e7bee1f9d.json   ← key: "user1"
├── 5a105e8b9d40e1329780d62ea2265d8a.json   ← key: "test1"
├── 698dc19d489c4e4db73e28a713eab07b.json   ← key: "teste"
└── 900150983cd24fb0d6963f7d28e17f72.json   ← key: "abc"
```

**File contents:**
```json
{
  "key": "user1",
  "value": { "name": "joao", "age": 30 }
}
```

**Storage characteristics:**
- **No schema enforcement** — any JSON value is accepted.
- **No indexing** — lookup is O(1) (direct filename from MD5 hash), but range queries or full scans are not supported.
- **No WAL (Write-Ahead Log)** — a crash mid-write could corrupt the JSON file.
- **Overwrites silently** — `create` and `update` both call the same underlying `fsdb.create`, which overwrites the file completely.

---

## API Reference

### Reverse Proxy (port 8000) — Client-facing

#### `GET /status`
Returns the status of all three DNs (polled in real time).

**Response:**
```json
[
  { "dn": 0, "status": { "node": 9001, "uptime": 12345, "state": "leader" } },
  { "dn": 1, "status": { "node": 9002, "uptime": 12300, "state": "follower" } },
  { "dn": 2, "status": { "node": 9003, "uptime": 12290, "state": "follower" } }
]
```
If a DN is unreachable: `{ "dn": N, "status": "DOWN" }`

---

#### `GET /stat`
Returns the RP's own operation counters.

**Response:**
```json
{ "data": { "create": 5, "read": 12, "update": 2, "delete": 1 }, "error": 0 }
```

---

#### `POST /db/c` — Create
Runs a full 2PC cycle to write a new key-value pair.

**Request body:**
```json
{ "key": "user1", "value": { "name": "joao" } }
```

**Success response:**
```json
{ "data": { "key": "user1", "value": { "name": "joao" } }, "error": 0 }
```

**Failure — prepare rejected:**
```json
{ "data": 0, "error": { "code": "e2PC001", "errno": 0, "message": "prepare failed" } }
```

---

#### `GET /db/r?key=<key>` — Read
Reads a key directly from the shard leader (no 2PC needed for reads).

**Success response:**
```json
{ "data": { "key": "user1", "value": { "name": "joao" } }, "error": 0 }
```

**Failure — key not found:**
```json
{ "data": 0, "error": { "code": "eRPCRUD002", "errno": 0, "message": "key not found" } }
```

---

#### `POST /db/u` — Update
Same as `POST /db/c` but semantically an update. Increments `stats.update`. Physically identical (calls `prepare` + `commit` which overwrites the file).

---

#### `GET /db/d?key=<key>` — Delete
Runs a 2PC cycle to delete a key.

**Success response:**
```json
{ "data": { "deleted": "user1" }, "error": 0 }
```

---

#### `POST /set_master` — Internal: leader announcement
Called by a DN when it wins an election. Not intended for external clients.

**Request body:**
```json
{ "dnId": 0, "leaderUrl": "http://127.0.0.1:9001" }
```

---

### Data Node (ports 9001–9003) — Internal

#### `GET /status`
```json
{ "node": 9001, "uptime": 12345, "state": "leader" }
```

#### `GET /stat`
```json
{ "data": { "create": 3, "read": 8, "update": 1, "delete": 0 }, "error": 0 }
```

#### `GET /election?term=<term>` — Raft vote request
Returns `{ "vote": true }` or `{ "vote": false }`.

#### `POST /heartbeat` — Leader heartbeat
Body: `{ "leaderId": 9001 }`. Updates follower state and resets election timer.

#### `POST /prepare` — 2PC Phase 1
Body: `{ "key": "...", "value": "..." }`. Always returns success in current implementation.

#### `POST /commit` — 2PC Phase 2 (write + replicate)
Body: `{ "key": "...", "value": "..." }`. Writes locally, replicates to peers asynchronously.

#### `POST /replicate` — Follower replication target
Body: `{ "key": "...", "value": "..." }`. Calls `fsdb.create` and returns success.

#### `POST /delete` — Delete + replicate
Body: `{ "key": "..." }`. Deletes locally, replicates delete to peers.

#### `POST /db/c` — Legacy create endpoint
Direct write without 2PC. Still available but the RP routes through 2PC instead.

#### `GET /db/r?key=<key>` — Read
Reads directly from local file store.

#### `GET /db/d?key=<key>` — Legacy delete endpoint

---

## Data Flow Walkthroughs

### Write: `POST /db/c { key: "user1", value: { name: "joao" } }`

```
1. Client → RP:8000 POST /db/c
2. RP: shard = getDN("user1", 1) = 0
3. RP: leaderUrl = leaders[0] = "http://127.0.0.1:9001"
4. RP → DN1:9001 POST /prepare { key, value }
5. DN1 responds { data: "ok", error: 0 }
6. RP checks: if response.error !== 0 → return failure (abort)
7. RP → DN1:9001 POST /commit { key, value }
8. DN1: fsdb.create("user1", { name: "joao" })
         → writes DBdata/24c9e15e52afc47c225b757e7bee1f9d.json
9. DN1 → DN2:9002 POST /replicate { key, value }  (async)
   DN1 → DN3:9003 POST /replicate { key, value }  (async)
10. DN1 responds { data: { key, value }, error: 0 } to RP
11. RP increments stats.create
12. RP → Client: { data: { key, value }, error: 0 }
```

---

### Read: `GET /db/r?key=user1`

```
1. Client → RP:8000 GET /db/r?key=user1
2. RP: shard = getDN("user1", 1) = 0
3. RP: leaderUrl = leaders[0] = "http://127.0.0.1:9001"
4. RP → DN1:9001 GET /db/r?key=user1
5. DN1: fsdb.read("user1")
         → reads DBdata/24c9e15e52afc47c225b757e7bee1f9d.json
         → returns { key: "user1", value: { name: "joao" } }
6. DN1 → RP: { data: { key, value }, error: 0 }
7. RP increments stats.read
8. RP → Client: { data: { key, value }, error: 0 }
```

---

### Delete: `GET /db/d?key=user1`

```
1. Client → RP:8000 GET /db/d?key=user1
2. RP: shard = getDN("user1", 1) = 0
3. RP → DN1:9001 POST /prepare { key }
4. DN1 responds ok
5. RP → DN1:9001 POST /delete { key }
6. DN1: fsdb.remove("user1")
         → deletes DBdata/24c9e15e52afc47c225b757e7bee1f9d.json
7. DN1 → DN2:9002 POST /delete { key }  (replication)
   DN1 → DN3:9003 POST /delete { key }  (replication)
8. DN1 → RP: { data: { deleted: "user1" }, error: 0 }
9. RP increments stats.delete
10. RP → Client: { data: { deleted: "user1" }, error: 0 }
```

---

### Leader Election (triggered by follower timeout)

```
1. DN2 detects: now - lastHeartbeat > ELECTION_TIMEOUT
2. DN2: state = 'candidate', currentTerm++, votedFor = 9002
3. DN2 → DN1:9001 GET /election?term=2
   DN2 → DN3:9003 GET /election?term=2
4. DN1: term 2 > currentTerm(1) → vote = true, currentTerm = 2
   DN3: term 2 > currentTerm(1) → vote = true, currentTerm = 2
5. DN2 receives 2 votes (self + 2 peers) = majority
6. DN2: state = 'leader'
7. DN2 → RP:8000 POST /set_master { dnId: 0, leaderUrl: "http://127.0.0.1:9002" }
8. RP: leaders[0] = "http://127.0.0.1:9002"
9. DN2: startHeartbeat() → every 2s sends POST /heartbeat to DN1, DN3
```

---

## Logging

Each service writes to its own log file:

| Service | Log file |
|---|---|
| Reverse Proxy | `rp.log` |
| Data Node 1 | `dn0s1.log` |
| Data Node 2 | `dn0s2.log` |
| Data Node 3 | `dn0s3.log` |

All logs also print to console. Format: `YYYY-MM-DD HH:mm:ss level: message`

**Common log messages:**

| Context | Message |
|---|---|
| Election | `[TERM 3] became candidate` |
| Election | `[TERM 3] received vote from 9002` |
| Election | `[TERM 3] became leader` |
| Election | `[TERM 3] master announced to RP` |
| Heartbeat | `heartbeat from 9001` |
| Heartbeat | `leader timeout` |
| CRUD | `CREATE key=user1` |
| CRUD | `READ key=user1` |
| Leader change | `new master for dn 0: http://127.0.0.1:9002` |

---

## Startup & Lifecycle

### Starting the cluster

The `falconDBd` shell script starts services in order with delays:

```bash
node RP/server.js &          # Start RP first (DNs will register with it)
sleep 2
PORT=9001 node DN/dn0s1/server.js &
PORT=9002 node DN/dn0s2/server.js &
PORT=9003 node DN/dn0s3/server.js &
```

### Per-node startup sequence

```
t=0s    Express server starts listening on PORT
t=0s    Logger initialized, "server started" logged
t=5s    startElectionMonitor() begins (3s polling interval)
t=8s    First election check — if no heartbeat in last 5-10s, election starts
```

Because all three nodes start simultaneously with no pre-configured leader, **one of them will always trigger an election** within the first 5–15 seconds. The randomized timeout means elections usually resolve quickly without split votes.

---

## Running with Podman

All 4 services (RP + 3 DN nodes) correm num **Podman Pod**, o que faz com que partilhem o mesmo namespace de rede. Isto significa que os endereços `127.0.0.1` que já existem no código continuam a funcionar sem qualquer alteração.

### Pré-requisitos

- Podman instalado
- Podman Machine a correr: `podman machine start`

### 1. Primeira vez (build da imagem)

Na raiz do projeto:

```powershell
podman build -t falcondb:latest .
```

Só é necessário repetir este passo se alterares código.

### 2. Arrancar o cluster

```powershell
podman pod create --name falcondb -p 8000:8000

$LOGS = "$PWD\logs"
New-Item -ItemType Directory -Force -Path $LOGS

podman run -d --pod falcondb --name rp -v "${LOGS}:/app/logs" falcondb:latest node RP/server.js
podman run -d --pod falcondb --name dn0s1 -v falcondb-dn0s1:/app/DBdata -v "${LOGS}:/app/logs" falcondb:latest node DN/dn0s1/server.js
podman run -d --pod falcondb --name dn0s2 -v falcondb-dn0s2:/app/DBdata -v "${LOGS}:/app/logs" falcondb:latest node DN/dn0s2/server.js
podman run -d --pod falcondb --name dn0s3 -v falcondb-dn0s3:/app/DBdata -v "${LOGS}:/app/logs" falcondb:latest node DN/dn0s3/server.js
```

Aguarda ~10 segundos para o Raft eleger um leader.

### 3. Verificar que está tudo up

```powershell
podman ps --pod
```

Devem aparecer 5 linhas (4 containers + 1 infra do pod), todas com `STATUS = Up`.

### 4. Verificar o estado do cluster via API

```powershell
(Invoke-WebRequest -Uri http://localhost:8000/status -UseBasicParsing).Content
```

Resposta esperada (um dos nós com `"state":"leader"`):

```json
{"data":[{"dn":"0","status":{"data":{"node":"9001","uptime":12.3,"state":"leader"},"error":0}}],"error":0}
```

### 5. Ver os logs

Os logs ficam na pasta `logs/` na raiz do projeto:

| Ficheiro | Serviço |
|---|---|
| `logs/rp.log` | Reverse Proxy |
| `logs/dn0s1.log` | Data Node 1 |
| `logs/dn0s2.log` | Data Node 2 |
| `logs/dn0s3.log` | Data Node 3 |

Para ver em tempo real:

```powershell
podman logs -f dn0s1
```

### 6. Parar o cluster

```powershell
podman pod stop falcondb
podman pod rm falcondb
```

Os dados persistem nos volumes `falcondb-dn0s1`, `falcondb-dn0s2`, `falcondb-dn0s3`. Na próxima vez que arrancares, os dados continuam lá.

Para apagar os dados também:

```powershell
podman volume rm falcondb-dn0s1 falcondb-dn0s2 falcondb-dn0s3
```

### Teste rápido CRUD

```powershell
# CREATE
Invoke-WebRequest -Uri http://localhost:8000/db/c -Method POST -ContentType "application/json" -Body '{"key":"teste","value":"funciona"}' -UseBasicParsing | Select-Object -ExpandProperty Content

# READ
(Invoke-WebRequest -Uri "http://localhost:8000/db/r?key=teste" -UseBasicParsing).Content

# DELETE
(Invoke-WebRequest -Uri "http://localhost:8000/db/d?key=teste" -UseBasicParsing).Content
```

---

## Error Code System

Error codes follow a namespace convention:

| Prefix | Scope |
|---|---|
| `eDNCRUD###` | Data Node CRUD operation failures |
| `e2PC###` | Two-phase commit protocol failures |
| `eRP###` | Reverse proxy internal errors |
| `eRPCRUD###` | Reverse proxy CRUD forwarding errors |

**Known error codes:**

| Code | Meaning |
|---|---|
| `eDNCRUD002` | Key not found (DN read) |
| `e2PC001` | Prepare phase failed |
| `eRP002` | RP internal error |
| `eRPCRUD002` | Key not found (RP read) |

---

## Known Limitations & Design Decisions

### 1. Raft state is not persisted
`currentTerm`, `votedFor`, and leadership status live only in memory. A node restart resets these, which violates a core Raft safety guarantee (a node could vote twice in the same term across restarts). This is acceptable for a learning project but would be a correctness bug in production.

**Fix:** Persist `currentTerm` and `votedFor` to disk on every change (a simple JSON file suffices).

---

### 2. Hardcoded topology
DN peers, RP address, and port numbers are all literals in the source files. The `etc/configure.json` file exists but is never loaded. Changing the topology requires editing multiple source files.

**Fix:** Load `configure.json` at startup and derive all addresses from it.

---

### 3. 2PC Phase 1 always succeeds
The `POST /prepare` handler returns success unconditionally. True 2PC would check for locks, capacity, or other preconditions in Phase 1 and reserve a slot. Here, Phase 1 only verifies the leader is reachable.

**Why it's like this:** The current design is about durability and replication ordering, not lock-based coordination. Acceptable for a single-writer (leader-only) model.

---

### 4. Async replication (eventual consistency)
After the leader commits, it fires replication requests to followers and immediately returns success to the RP — it does not wait for followers to acknowledge. This means:

- A client may read stale data from a follower immediately after a write.
- If a follower is down and the leader crashes after commit but before replication, data is lost on that follower.

**Fix for strict consistency:** Use synchronous replication with quorum acknowledgement before responding to the client.

---

### 5. Only one shard group
Despite the sharding module supporting multiple DN groups, only `dn0` is defined. All keys map to shard 0. The system is effectively a 3-node replicated store, not a sharded one.

---

### 6. No authentication or authorization
All endpoints on both RP and DNs are openly accessible. Internal DN endpoints (`/prepare`, `/commit`, `/replicate`, `/election`, `/heartbeat`) should be protected from external access.

---

### 7. No read consistency guarantees
Reads go directly to the leader (via RP), but followers can also serve reads through their `GET /db/r` endpoint if called directly. A stale follower would return old data. The RP always routes reads to the leader, so through the RP, reads are consistent with the latest committed write.

---

### 8. Split-brain risk (no lease mechanism)
If the network partitions and two nodes both believe they are the leader (e.g., old leader hasn't received enough heartbeat failures yet), both could accept writes. True Raft uses leader leases to prevent this. This implementation has no such mechanism.

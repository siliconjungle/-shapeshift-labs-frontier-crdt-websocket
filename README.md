# Frontier CRDT WebSocket

WebSocket client and server transports for Frontier CRDT sync providers.

This package sits above [`@shapeshift-labs/frontier-crdt-sync`](https://www.npmjs.com/package/@shapeshift-labs/frontier-crdt-sync). The sync package owns protocol state, document handles, storage, and encoded sync messages; this package only moves those messages over WebSocket rooms.

- npm: [`@shapeshift-labs/frontier-crdt-websocket`](https://www.npmjs.com/package/@shapeshift-labs/frontier-crdt-websocket)
- source: [`siliconjungle/-shapeshift-labs-frontier-crdt-websocket`](https://github.com/siliconjungle/-shapeshift-labs-frontier-crdt-websocket)
- license: MIT

## Related Packages

- [`@shapeshift-labs/frontier-state-cache-idb`](https://www.npmjs.com/package/@shapeshift-labs/frontier-state-cache-idb): IndexedDB persistence adapter for Frontier state-cache snapshots.
- [`@shapeshift-labs/frontier-state-cache-file`](https://www.npmjs.com/package/@shapeshift-labs/frontier-state-cache-file): Structured file persistence adapter for Frontier state-cache snapshots and change logs.
- [`@shapeshift-labs/frontier-state-cache-sql`](https://www.npmjs.com/package/@shapeshift-labs/frontier-state-cache-sql): SQL persistence adapter for Frontier state-cache snapshots and change logs.
- [`@shapeshift-labs/frontier`](https://www.npmjs.com/package/@shapeshift-labs/frontier): core JSON diff/apply primitives below the CRDT layer.
- [`@shapeshift-labs/frontier-crdt`](https://www.npmjs.com/package/@shapeshift-labs/frontier-crdt): native CRDT document and update layer.
- [`@shapeshift-labs/frontier-crdt-sync`](https://www.npmjs.com/package/@shapeshift-labs/frontier-crdt-sync): transport-agnostic sync protocol, provider, repo, storage, and document handles.

Package source repositories:

- [`siliconjungle/-shapeshift-labs-frontier-state-cache-idb`](https://github.com/siliconjungle/-shapeshift-labs-frontier-state-cache-idb)
- [`siliconjungle/-shapeshift-labs-frontier-state-cache-file`](https://github.com/siliconjungle/-shapeshift-labs-frontier-state-cache-file)
- [`siliconjungle/-shapeshift-labs-frontier-state-cache-sql`](https://github.com/siliconjungle/-shapeshift-labs-frontier-state-cache-sql)
- [`siliconjungle/-shapeshift-labs-frontier`](https://github.com/siliconjungle/-shapeshift-labs-frontier)
- [`siliconjungle/-shapeshift-labs-frontier-crdt`](https://github.com/siliconjungle/-shapeshift-labs-frontier-crdt)
- [`siliconjungle/-shapeshift-labs-frontier-crdt-sync`](https://github.com/siliconjungle/-shapeshift-labs-frontier-crdt-sync)
- [`siliconjungle/-shapeshift-labs-frontier-crdt-websocket`](https://github.com/siliconjungle/-shapeshift-labs-frontier-crdt-websocket)

## Install

```sh
npm install @shapeshift-labs/frontier-crdt @shapeshift-labs/frontier-crdt-sync @shapeshift-labs/frontier-crdt-websocket
```

## Usage

Server:

```ts
import { createCrdtWebSocketServer } from '@shapeshift-labs/frontier-crdt-websocket/server';

const server = createCrdtWebSocketServer({
  host: '127.0.0.1',
  port: 8787,
  frameEncoding: 'binary',
  heartbeatIntervalMs: 30000,
  heartbeatTimeoutMs: 10000,
  authenticate: () => true,
  authorizeRoom: ({ peerId, documentId }) => {
    console.log(`admit peer=${peerId} document=${documentId}`);
    return true;
  }
});

await server.ready;
console.log(server.address());
```

Client:

```ts
import { createCrdtDocument } from '@shapeshift-labs/frontier-crdt';
import { createCrdtSyncEndpoint } from '@shapeshift-labs/frontier-crdt-sync';
import { createCrdtWebSocketProvider } from '@shapeshift-labs/frontier-crdt-websocket';

const doc = createCrdtDocument({ actorId: 'alice' });
const endpoint = createCrdtSyncEndpoint(doc, {
  documentId: 'doc-1',
  senderId: 'alice',
  actorRangeSync: true
});

const provider = createCrdtWebSocketProvider(endpoint, {
  url: 'ws://127.0.0.1:8787',
  documentId: 'doc-1',
  peerId: 'alice',
  syncOnConnect: true,
  reconnect: true
});

await provider.connect();
doc.set('/title', 'Draft');
await provider.sync();
```

## API

```ts
import {
  createCrdtWebSocketClientTransport,
  createCrdtWebSocketProvider,
  encodeCrdtWebSocketBinaryFrame,
  encodeCrdtWebSocketFrame,
  encodeCrdtWebSocketTransportFrame,
  decodeCrdtWebSocketFrame,
  type CrdtWebSocketClientTransport,
  type CrdtWebSocketFrameEncoding,
  type CrdtWebSocketProvider
} from '@shapeshift-labs/frontier-crdt-websocket';

import {
  createCrdtWebSocketServer,
  type CrdtWebSocketServer
} from '@shapeshift-labs/frontier-crdt-websocket/server';
```

### `createCrdtWebSocketProvider(endpoint, options)`

Creates a Frontier sync provider with a WebSocket transport attached.

Useful options:

- `url`: WebSocket server URL.
- `documentId`: room/document id.
- `peerId`: local peer id.
- `syncOnConnect`: asks known peers for state as soon as the provider connects.
- `autoSyncOnPeerJoin`: defaults to `true`; syncs with peers announced by the server.
- `WebSocket`: optional constructor injection for Node tests, browser polyfills, and custom runtimes.
- `frameEncoding`: defaults to `'binary'`; set `'json'` for readable compatibility frames.
- `auth`: static value or async function copied into the initial `hello` frame for your server-side admission hook.
- `reconnect`: opt-in reconnect loop with bounded exponential delay.
- `reconnectDelayMs`, `maxReconnectDelayMs`, `reconnectMaxAttempts`, `reconnectBackoffFactor`, `reconnectJitterRatio`: reconnect backoff controls.
- `heartbeatIntervalMs` and `heartbeatTimeoutMs`: app-level ping/pong watchdog. Set the interval to `0` to disable it.
- `maxFrameBytes`: hard limit for inbound and outbound frames.
- `maxQueuedFrames` and `maxQueuedBytes`: bounds for messages queued while disconnected.
- `maxBufferedAmount` and `sendTimeoutMs`: browser/Node WebSocket backpressure guard.

### `createCrdtWebSocketClientTransport(options)`

Creates only the transport. Use this when you want to pass a WebSocket transport into `createCrdtSyncProvider` yourself.

### `createCrdtWebSocketServer(options)`

Creates a room-routing WebSocket server. Peers join by sending a `hello` frame containing `{ peerId, documentId }`. Sync frames are routed to peers in the same document room.

Useful options:

- `frameEncoding`: defaults to `'binary'`; set `'json'` when debugging frames manually.
- `maxFrameBytes`: passed to the Node `ws` server as `maxPayload` and checked by the Frontier frame decoder.
- `maxBufferedAmount`: closes slow peers with the backpressure close code before unbounded buffering.
- `heartbeatIntervalMs` and `heartbeatTimeoutMs`: server-side app ping/pong timeout.
- `authenticate(context)`: connection-level hook slot. Return `false` or `{ ok: false }` to reject the socket.
- `authorizeRoom(context)`: room admission hook slot. It receives `{ request, peerId, documentId, auth, principal }` and may reject a `hello`.

The auth hooks are deliberately just slots. This package does not implement tokens, sessions, ACLs, tenancy, or authorization policy.

### Wire Helpers

`encodeCrdtWebSocketTransportFrame(frame)` emits the default binary frame. `encodeCrdtWebSocketFrame(frame)` emits the JSON compatibility frame. `decodeCrdtWebSocketFrame(input, { maxFrameBytes })` accepts either format.

Binary is the default for provider/server transport because sync frames carry byte payloads and avoid base64 expansion. JSON remains useful for debugging, proxies, and tiny control-heavy workloads where human-readable frames matter more than wire size.

## Subpath Imports

```ts
import { createCrdtWebSocketProvider } from '@shapeshift-labs/frontier-crdt-websocket';
import { createCrdtWebSocketClientTransport } from '@shapeshift-labs/frontier-crdt-websocket/client';
import { createCrdtWebSocketServer } from '@shapeshift-labs/frontier-crdt-websocket/server';
import { encodeCrdtWebSocketTransportFrame } from '@shapeshift-labs/frontier-crdt-websocket/wire';
```

The root import is client-safe and does not import the Node `ws` server. The `./server` subpath owns the Node WebSocket server dependency.

## Package Scope

This package owns:

- WebSocket client transports for Frontier sync providers.
- WebSocket provider helper wiring peer announcements into provider sync.
- Node WebSocket room server for document/peer routing.
- WebSocket frame encode/decode helpers.
- Binary WebSocket frame transport plus JSON compatibility frames.
- Heartbeat, reconnect, backpressure, queue, and frame-size guard rails.
- Hook slots for connection authentication and room admission.
- Transport-level tests, fuzzers, and benchmarks.

It does not own:

- CRDT document semantics.
- Sync state vectors, actor-range anti-entropy, or update encoding.
- Storage adapters, repos, document handles, or editor bindings.
- Rich text, awareness, undo, branch, or conflict APIs.

## TypeScript

The package ships ESM JavaScript plus `.d.ts` declarations for root, `./client`, `./server`, and `./wire`. The package-local TypeScript source lives in `src/` and compiles directly to `dist/`.

## Validation

```sh
npm test
npm run fuzz
npm run bench
npm run pack:dry
```

The package test suite covers root and subpath imports, frame validation, binary and JSON frame round trips, client/server handshakes, auth/admission rejection, heartbeat timeout, reconnect after server restart, many-peer convergence, slow-client backpressure, malformed and oversized frames, queue limits, peer join/leave announcements, two-peer document convergence, and randomized WebSocket sync schedules.

## Examples

The package includes runnable examples:

```sh
node examples/node-server.mjs
node examples/reconnect-client.mjs
```

Browser examples:

- `examples/browser-client.html`: one browser client connected to a WebSocket room.
- `examples/two-tabs.html`: open the file in two tabs and edit the same document.

The examples are intentionally small transport examples. Production apps should supply their own authentication, admission, persistence, TLS, origin checks, rate limits, and tenant policy.

## Benchmarks

Run the package-local benchmark:

```sh
npm run bench
```

Latest local package benchmark on Node v26.1.0, darwin arm64, 9 rounds:

| Fixture | Median | p95 | Bytes |
| --- | ---: | ---: | ---: |
| WebSocket binary frame encode/decode | 2.46 us | 2.71 us | 165 |
| WebSocket JSON frame encode/decode | 0.76 us | 0.79 us | 247 |
| WebSocket client/server handshake | 377.39 us | 580.35 us | - |
| WebSocket two-peer CRDT sync | 1.79 ms | 2.27 ms | - |

These are Frontier-only package measurements, not competitor comparisons.

## License

MIT. See [LICENSE](./LICENSE).

# Frontier CRDT WebSocket

WebSocket client and server transports for Frontier CRDT sync providers.

This package sits above [`@shapeshift-labs/frontier-crdt-sync`](https://www.npmjs.com/package/@shapeshift-labs/frontier-crdt-sync). The sync package owns protocol state, document handles, storage, and encoded sync messages; this package only moves those messages over WebSocket rooms.

- npm: [`@shapeshift-labs/frontier-crdt-websocket`](https://www.npmjs.com/package/@shapeshift-labs/frontier-crdt-websocket)
- source: [`siliconjungle/-shapeshift-labs-frontier-crdt-websocket`](https://github.com/siliconjungle/-shapeshift-labs-frontier-crdt-websocket)
- license: MIT

## Related Packages

The published Frontier package family is generated from one shared package catalog so READMEs stay in sync across packages:

- [`@shapeshift-labs/frontier`](https://www.npmjs.com/package/@shapeshift-labs/frontier): Core JSON diff/apply, compact patch tuples, JSON Pointer, equality, clone, validation, Unicode helpers.
- [`@shapeshift-labs/frontier-query`](https://www.npmjs.com/package/@shapeshift-labs/frontier-query): Shared query-key, selector path, condition, entity identity, and table-shape primitives.
- [`@shapeshift-labs/frontier-codec`](https://www.npmjs.com/package/@shapeshift-labs/frontier-codec): Patch serialization, binary frames, canonical JSON, and patch-history codecs.
- [`@shapeshift-labs/frontier-engine`](https://www.npmjs.com/package/@shapeshift-labs/frontier-engine): Stateful planned diff engine, adaptive profiles, schema plans, and engine-level history helpers.
- [`@shapeshift-labs/frontier-state`](https://www.npmjs.com/package/@shapeshift-labs/frontier-state): Patch-routed app-state subscriptions, owned commits, maintained views, and path mapping.
- [`@shapeshift-labs/frontier-state-cache`](https://www.npmjs.com/package/@shapeshift-labs/frontier-state-cache): Normalized query-result cache with entity/query watchers, persistence, change logs, optimistic layers, and mutation bridge.
- [`@shapeshift-labs/frontier-state-cache-idb`](https://www.npmjs.com/package/@shapeshift-labs/frontier-state-cache-idb): IndexedDB persistence adapter for Frontier state-cache snapshots.
- [`@shapeshift-labs/frontier-state-cache-file`](https://www.npmjs.com/package/@shapeshift-labs/frontier-state-cache-file): Structured file persistence adapter for Frontier state-cache snapshots and change logs.
- [`@shapeshift-labs/frontier-state-cache-sql`](https://www.npmjs.com/package/@shapeshift-labs/frontier-state-cache-sql): SQL persistence adapter for Frontier state-cache snapshots and change logs.
- [`@shapeshift-labs/frontier-schema`](https://www.npmjs.com/package/@shapeshift-labs/frontier-schema): JSON Schema validation, Frontier profile generation, CloudEvent envelopes, and query/table schema helpers.
- [`@shapeshift-labs/frontier-event-log`](https://www.npmjs.com/package/@shapeshift-labs/frontier-event-log): Bounded event logs, replay cursors, consumer acknowledgements, keyed compaction, checkpoints, and Frontier patch event records.
- [`@shapeshift-labs/frontier-scheduler`](https://www.npmjs.com/package/@shapeshift-labs/frontier-scheduler): Deterministic work scheduling, lanes, cancellation, backpressure, frame policies, replay snapshots, and work graphs.
- [`@shapeshift-labs/frontier-logging`](https://www.npmjs.com/package/@shapeshift-labs/frontier-logging): Opt-in structured logging, browser telemetry, file sinks, exporters, benchmark traces, and Frontier patch/update summaries.
- [`@shapeshift-labs/frontier-mutation`](https://www.npmjs.com/package/@shapeshift-labs/frontier-mutation): Explicit mutation and selector plans compiled to Frontier patches or CRDT operations.
- [`@shapeshift-labs/frontier-virtual`](https://www.npmjs.com/package/@shapeshift-labs/frontier-virtual): DOM-neutral virtualization, layout providers, range materialization, grids, spatial culling, frustum culling, and serializable layout state.
- [`@shapeshift-labs/frontier-dom`](https://www.npmjs.com/package/@shapeshift-labs/frontier-dom): Patch-native DOM and host renderer bindings, manifest hydration, JSX runtime/compiler helpers, SSR, devtools, and logging bridges.
- [`@shapeshift-labs/frontier-crdt`](https://www.npmjs.com/package/@shapeshift-labs/frontier-crdt): Native CRDT documents, update tooling, awareness, branches, conflict introspection, version frames, and undo.
- [`@shapeshift-labs/frontier-crdt-sync`](https://www.npmjs.com/package/@shapeshift-labs/frontier-crdt-sync): CRDT sync endpoints, repo/storage/provider contracts, document URLs, local networks, model checking, forensics, and text binding contracts.
- [`@shapeshift-labs/frontier-react`](https://www.npmjs.com/package/@shapeshift-labs/frontier-react): React external-store hooks and adapters for Frontier state, cache, and CRDT surfaces.
- [`@shapeshift-labs/frontier-richtext`](https://www.npmjs.com/package/@shapeshift-labs/frontier-richtext): Rich text Delta normalization/application, marks, embeds, ranges, and cursor/selection transforms for local editor integrations.
- [`@shapeshift-labs/frontier-realtime`](https://www.npmjs.com/package/@shapeshift-labs/frontier-realtime): Shared realtime command, tick, snapshot, prediction, reconciliation, interpolation, rollback, message, and delta primitives.
- [`@shapeshift-labs/frontier-realtime-server`](https://www.npmjs.com/package/@shapeshift-labs/frontier-realtime-server): Authoritative realtime room, tick, command validation, rate-limit, session, and snapshot-history runtime.
- [`@shapeshift-labs/frontier-realtime-websocket`](https://www.npmjs.com/package/@shapeshift-labs/frontier-realtime-websocket): WebSocket client, wire, and Node room-server transport for Frontier realtime.
- [`@shapeshift-labs/frontier-game`](https://www.npmjs.com/package/@shapeshift-labs/frontier-game): Game-facing entity, component, player, room, ownership, spatial interest, rollback, physics, and replication helpers above realtime.

Package source repositories:

- [`siliconjungle/-shapeshift-labs-frontier`](https://github.com/siliconjungle/-shapeshift-labs-frontier)
- [`siliconjungle/-shapeshift-labs-frontier-query`](https://github.com/siliconjungle/-shapeshift-labs-frontier-query)
- [`siliconjungle/-shapeshift-labs-frontier-codec`](https://github.com/siliconjungle/-shapeshift-labs-frontier-codec)
- [`siliconjungle/-shapeshift-labs-frontier-engine`](https://github.com/siliconjungle/-shapeshift-labs-frontier-engine)
- [`siliconjungle/-shapeshift-labs-frontier-state`](https://github.com/siliconjungle/-shapeshift-labs-frontier-state)
- [`siliconjungle/-shapeshift-labs-frontier-state-cache`](https://github.com/siliconjungle/-shapeshift-labs-frontier-state-cache)
- [`siliconjungle/-shapeshift-labs-frontier-state-cache-idb`](https://github.com/siliconjungle/-shapeshift-labs-frontier-state-cache-idb)
- [`siliconjungle/-shapeshift-labs-frontier-state-cache-file`](https://github.com/siliconjungle/-shapeshift-labs-frontier-state-cache-file)
- [`siliconjungle/-shapeshift-labs-frontier-state-cache-sql`](https://github.com/siliconjungle/-shapeshift-labs-frontier-state-cache-sql)
- [`siliconjungle/-shapeshift-labs-frontier-schema`](https://github.com/siliconjungle/-shapeshift-labs-frontier-schema)
- [`siliconjungle/-shapeshift-labs-frontier-event-log`](https://github.com/siliconjungle/-shapeshift-labs-frontier-event-log)
- [`siliconjungle/-shapeshift-labs-frontier-scheduler`](https://github.com/siliconjungle/-shapeshift-labs-frontier-scheduler)
- [`siliconjungle/-shapeshift-labs-frontier-logging`](https://github.com/siliconjungle/-shapeshift-labs-frontier-logging)
- [`siliconjungle/-shapeshift-labs-frontier-mutation`](https://github.com/siliconjungle/-shapeshift-labs-frontier-mutation)
- [`siliconjungle/-shapeshift-labs-frontier-virtual`](https://github.com/siliconjungle/-shapeshift-labs-frontier-virtual)
- [`siliconjungle/-shapeshift-labs-frontier-dom`](https://github.com/siliconjungle/-shapeshift-labs-frontier-dom)
- [`siliconjungle/-shapeshift-labs-frontier-crdt`](https://github.com/siliconjungle/-shapeshift-labs-frontier-crdt)
- [`siliconjungle/-shapeshift-labs-frontier-crdt-sync`](https://github.com/siliconjungle/-shapeshift-labs-frontier-crdt-sync)
- [`siliconjungle/-shapeshift-labs-frontier-crdt-websocket`](https://github.com/siliconjungle/-shapeshift-labs-frontier-crdt-websocket)
- [`siliconjungle/-shapeshift-labs-frontier-react`](https://github.com/siliconjungle/-shapeshift-labs-frontier-react)
- [`siliconjungle/-shapeshift-labs-frontier-richtext`](https://github.com/siliconjungle/-shapeshift-labs-frontier-richtext)
- [`siliconjungle/-shapeshift-labs-frontier-realtime`](https://github.com/siliconjungle/-shapeshift-labs-frontier-realtime)
- [`siliconjungle/-shapeshift-labs-frontier-realtime-server`](https://github.com/siliconjungle/-shapeshift-labs-frontier-realtime-server)
- [`siliconjungle/-shapeshift-labs-frontier-realtime-websocket`](https://github.com/siliconjungle/-shapeshift-labs-frontier-realtime-websocket)
- [`siliconjungle/-shapeshift-labs-frontier-game`](https://github.com/siliconjungle/-shapeshift-labs-frontier-game)

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

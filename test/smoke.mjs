import assert from 'node:assert';
import { WebSocket } from 'ws';
import { createCrdtDocument } from '@shapeshift-labs/frontier-crdt';
import { createCrdtSyncEndpoint } from '@shapeshift-labs/frontier-crdt-sync';
import {
  createCrdtWebSocketClientTransport,
  createCrdtWebSocketProvider,
  createCrdtWebSocketSyncFrame,
  decodeCrdtWebSocketFrame,
  decodeCrdtWebSocketSyncPayload,
  encodeCrdtWebSocketFrame
} from '../dist/index.js';
import { createCrdtWebSocketClientTransport as createClientSubpath } from '../dist/client.js';
import { createCrdtWebSocketServer } from '../dist/server.js';
import { encodeCrdtWebSocketFrame as encodeWireSubpath } from '../dist/wire.js';

for (const value of [
  createCrdtWebSocketClientTransport,
  createCrdtWebSocketProvider,
  createClientSubpath,
  createCrdtWebSocketServer,
  encodeWireSubpath
]) {
  assert.strictEqual(typeof value, 'function');
}

const payloadFrame = createCrdtWebSocketSyncFrame('doc-a', 'alice', 'bob', {
  type: 'ack',
  stateVector: { alice: 1 },
  senderId: 'alice',
  documentId: 'doc-a'
});
const roundTripFrame = decodeCrdtWebSocketFrame(encodeCrdtWebSocketFrame(payloadFrame));
assert.strictEqual(roundTripFrame.kind, 'sync');
assert.strictEqual(decodeCrdtWebSocketSyncPayload(roundTripFrame).byteLength > 0, true);

const server = createCrdtWebSocketServer({ host: '127.0.0.1', port: 0 });
await server.ready;
const address = server.address();
assert.ok(address && typeof address === 'object' && 'port' in address);
const url = `ws://127.0.0.1:${address.port}`;

const alice = createCrdtDocument({ actorId: 'ws-alice' });
const bob = createCrdtDocument({ actorId: 'ws-bob' });
alice.set('/title', 'Draft');

const aliceProvider = createCrdtWebSocketProvider(
  createCrdtSyncEndpoint(alice, { documentId: 'doc-a', senderId: 'alice', actorRangeSync: true }),
  { url, documentId: 'doc-a', peerId: 'alice', WebSocket, syncOnConnect: true }
);
const bobProvider = createCrdtWebSocketProvider(
  createCrdtSyncEndpoint(bob, { documentId: 'doc-a', senderId: 'bob', actorRangeSync: true }),
  { url, documentId: 'doc-a', peerId: 'bob', WebSocket, syncOnConnect: true }
);

await aliceProvider.connect();
await bobProvider.connect();
await waitFor(() => bob.toJSON()?.title === 'Draft');
assert.deepStrictEqual(server.getPeerIds('doc-a'), ['alice', 'bob']);

bob.text('/body').insert(0, 'hello');
await bobProvider.sync('alice');
await waitFor(() => alice.toJSON()?.body === 'hello');

const transportOnly = createCrdtWebSocketClientTransport({
  url,
  documentId: 'doc-a',
  peerId: 'charlie',
  WebSocket
});
const seen = [];
transportOnly.subscribe((message, peerId) => {
  seen.push([peerId, message.byteLength]);
});
await transportOnly.connect();
await waitFor(() => server.getPeerIds('doc-a').includes('charlie'));
await aliceProvider.sync('charlie');
await waitFor(() => seen.length > 0);
await transportOnly.disconnect();

await bobProvider.disconnect();
await aliceProvider.disconnect();
await server.close();

async function waitFor(predicate, timeoutMs = 1200) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail('timed out waiting for condition');
}

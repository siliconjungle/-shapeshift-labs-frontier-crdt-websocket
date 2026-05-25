import assert from 'node:assert';
import { WebSocket } from 'ws';
import { createCrdtDocument } from '@shapeshift-labs/frontier-crdt';
import { checkCrdtSyncConvergence, createCrdtSyncEndpoint } from '@shapeshift-labs/frontier-crdt-sync';
import {
  createCrdtWebSocketClientTransport,
  createCrdtWebSocketProvider,
  createCrdtWebSocketSyncFrame,
  decodeCrdtWebSocketFrame,
  encodeCrdtWebSocketBinaryFrame,
  encodeCrdtWebSocketFrame,
  encodeCrdtWebSocketTransportFrame
} from '../dist/index.js';
import { createCrdtWebSocketServer } from '../dist/server.js';

const CLOSE_POLICY = 1008;
const CLOSE_UNSUPPORTED = 1003;
const CLOSE_TOO_LARGE = 1009;
const CLOSE_BACKPRESSURE = 1009;
const CLOSE_HEARTBEAT_TIMEOUT = 4000;

await testBinaryFrameRoundTripAndLimits();
await testAuthAdmissionHooks();
await testServerHeartbeatTimeout();
await testServerRestartClientReconnect();
await testManyPeers();
await testSlowClientBackpressure();
await testMalformedFrames();
await testClientQueueLimits();

console.log('frontier crdt-websocket integration passed');

async function testBinaryFrameRoundTripAndLimits() {
  const frame = createCrdtWebSocketSyncFrame('doc-wire', 'alice', 'bob', {
    type: 'ack',
    senderId: 'alice',
    documentId: 'doc-wire',
    stateVector: { alice: 2 }
  });
  const binary = encodeCrdtWebSocketBinaryFrame(frame);
  assert.ok(binary.byteLength < encodeCrdtWebSocketFrame(frame).length);
  assert.deepStrictEqual(decodeCrdtWebSocketFrame(binary), frame);
  assert.throws(
    () => decodeCrdtWebSocketFrame(binary, { maxFrameBytes: Math.max(1, binary.byteLength - 1) }),
    /maxFrameBytes|exceeds/
  );
}

async function testAuthAdmissionHooks() {
  const server = createCrdtWebSocketServer({
    host: '127.0.0.1',
    port: 0,
    authenticate: ({ request }) => ({ principal: { hasRequest: request !== undefined } }),
    authorizeRoom: ({ auth, principal }) => {
      const token = typeof auth === 'object' && auth !== null ? auth.token : undefined;
      return token === 'ok' && principal?.hasRequest === true;
    }
  });
  await server.ready;
  const url = serverUrl(server);

  const accepted = await openRaw(url);
  accepted.send(encodeCrdtWebSocketTransportFrame({
    kind: 'hello',
    peerId: 'accepted',
    documentId: 'doc-auth',
    auth: { token: 'ok' }
  }));
  await waitFor(() => server.getPeerIds('doc-auth').includes('accepted'));
  accepted.close();

  const rejected = await openRaw(url);
  rejected.send(encodeCrdtWebSocketTransportFrame({
    kind: 'hello',
    peerId: 'rejected',
    documentId: 'doc-auth',
    auth: { token: 'no' }
  }));
  const close = await waitForClose(rejected);
  assert.strictEqual(close.code, CLOSE_POLICY);

  await server.close();
}

async function testServerHeartbeatTimeout() {
  const server = createCrdtWebSocketServer({
    host: '127.0.0.1',
    port: 0,
    heartbeatIntervalMs: 10,
    heartbeatTimeoutMs: 20
  });
  await server.ready;
  const socket = await openRaw(serverUrl(server));
  socket.send(encodeCrdtWebSocketTransportFrame({
    kind: 'hello',
    peerId: 'silent',
    documentId: 'doc-heartbeat'
  }));
  const close = await waitForClose(socket, 800);
  assert.strictEqual(close.code, CLOSE_HEARTBEAT_TIMEOUT);
  await server.close();
}

async function testServerRestartClientReconnect() {
  let server = createCrdtWebSocketServer({ host: '127.0.0.1', port: 0, heartbeatIntervalMs: 0 });
  await server.ready;
  const port = server.address().port;
  const url = `ws://127.0.0.1:${port}`;
  const alice = createCrdtDocument({ actorId: 'restart-alice' });
  const bob = createCrdtDocument({ actorId: 'restart-bob' });

  const aliceProvider = makeProvider(alice, 'doc-restart', 'alice', url, { reconnect: true });
  const bobProvider = makeProvider(bob, 'doc-restart', 'bob', url, { reconnect: true });
  await aliceProvider.connect();
  await bobProvider.connect();
  await waitFor(() => server.getPeerIds('doc-restart').length === 2);

  await server.close();
  await delay(30);
  server = createCrdtWebSocketServer({ host: '127.0.0.1', port, heartbeatIntervalMs: 0 });
  await server.ready;
  await waitFor(() => server.getPeerIds('doc-restart').length === 2, 2500);

  bob.text('/body').insert(0, 'after-restart');
  await bobProvider.sync('alice');
  await waitFor(() => alice.toJSON()?.body === 'after-restart', 1500);

  await bobProvider.disconnect();
  await aliceProvider.disconnect();
  await server.close();
}

async function testManyPeers() {
  const server = createCrdtWebSocketServer({ host: '127.0.0.1', port: 0, heartbeatIntervalMs: 0 });
  await server.ready;
  const url = serverUrl(server);
  const peers = [];
  for (let i = 0; i < 8; i++) {
    const peerId = `peer-${i}`;
    const doc = createCrdtDocument({ actorId: `many-${i}` });
    const provider = makeProvider(doc, 'doc-many', peerId, url, { syncOnConnect: true });
    peers.push({ peerId, doc, provider });
  }
  for (const peer of peers) await peer.provider.connect();
  await waitFor(() => server.getPeerIds('doc-many').length === peers.length);

  for (let i = 0; i < peers.length; i++) {
    peers[i].doc.set(`/items/${peers[i].peerId}`, i);
  }
  for (const peer of peers) await peer.provider.sync();
  await waitFor(() => checkCrdtSyncConvergence(peers.map((peer) => peer.doc)).valid, 2500);

  for (const peer of peers) await peer.provider.disconnect();
  await server.close();
}

async function testSlowClientBackpressure() {
  const server = createCrdtWebSocketServer({
    host: '127.0.0.1',
    port: 0,
    maxBufferedAmount: 0,
    heartbeatIntervalMs: 0
  });
  await server.ready;
  const socket = await openRaw(serverUrl(server));
  socket.send(encodeCrdtWebSocketTransportFrame({
    kind: 'hello',
    peerId: 'no-buffer-budget',
    documentId: 'doc-backpressure'
  }));
  const close = await waitForClose(socket);
  assert.strictEqual(close.code, CLOSE_BACKPRESSURE);
  await server.close();
}

async function testMalformedFrames() {
  const malformedServer = createCrdtWebSocketServer({ host: '127.0.0.1', port: 0 });
  await malformedServer.ready;
  const malformed = await openRaw(serverUrl(malformedServer));
  malformed.send('not json');
  const malformedClose = await waitForClose(malformed);
  assert.strictEqual(malformedClose.code, CLOSE_UNSUPPORTED);
  await malformedServer.close();

  const oversizedServer = createCrdtWebSocketServer({ host: '127.0.0.1', port: 0, maxFrameBytes: 32 });
  await oversizedServer.ready;
  const oversized = await openRaw(serverUrl(oversizedServer));
  oversized.send(JSON.stringify({ kind: 'hello', peerId: 'oversized', documentId: 'x'.repeat(80) }));
  const oversizedClose = await waitForClose(oversized);
  assert.ok(
    oversizedClose.code === CLOSE_TOO_LARGE || oversizedClose.code === 1009,
    `expected close 1009, got ${oversizedClose.code}`
  );
  await oversizedServer.close();
}

async function testClientQueueLimits() {
  const transport = createCrdtWebSocketClientTransport({
    url: 'ws://127.0.0.1:9',
    documentId: 'doc-queue',
    peerId: 'queue-a',
    WebSocket,
    maxQueuedFrames: 1,
    maxQueuedBytes: 1024
  });
  await transport.disconnect();
  await transport.send('queue-b', {
    type: 'ack',
    senderId: 'queue-a',
    documentId: 'doc-queue',
    stateVector: {}
  });
  await assert.rejects(
    () => transport.send('queue-b', {
      type: 'ack',
      senderId: 'queue-a',
      documentId: 'doc-queue',
      stateVector: {}
    }),
    /queue limit/
  );
}

function makeProvider(doc, documentId, peerId, url, options = {}) {
  return createCrdtWebSocketProvider(
    createCrdtSyncEndpoint(doc, { documentId, senderId: peerId, actorRangeSync: true }),
    {
      url,
      documentId,
      peerId,
      WebSocket,
      heartbeatIntervalMs: 0,
      reconnectDelayMs: 15,
      maxReconnectDelayMs: 80,
      reconnectMaxAttempts: 80,
      reconnectJitterRatio: 0,
      ...options
    }
  );
}

function serverUrl(server) {
  const address = server.address();
  assert.ok(address && typeof address === 'object' && 'port' in address);
  return `ws://127.0.0.1:${address.port}`;
}

function openRaw(url) {
  const socket = new WebSocket(url);
  return new Promise((resolve, reject) => {
    socket.once('open', () => resolve(socket));
    socket.once('error', reject);
  });
}

function waitForClose(socket, timeoutMs = 1200) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('timed out waiting for WebSocket close')), timeoutMs);
    socket.once('close', (code, reason) => {
      clearTimeout(timer);
      resolve({ code, reason: reason?.toString?.() ?? '' });
    });
  });
}

async function waitFor(predicate, timeoutMs = 1200) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return;
    await delay(10);
  }
  assert.fail('timed out waiting for condition');
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

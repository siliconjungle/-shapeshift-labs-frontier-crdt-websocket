import assert from 'node:assert';
import { WebSocket } from 'ws';
import { createCrdtDocument } from '@shapeshift-labs/frontier-crdt';
import { checkCrdtSyncConvergence, createCrdtSyncEndpoint } from '@shapeshift-labs/frontier-crdt-sync';
import {
  createCrdtWebSocketProvider,
  decodeCrdtWebSocketFrame,
  encodeCrdtWebSocketBinaryFrame,
  encodeCrdtWebSocketFrame
} from '../dist/index.js';
import { createCrdtWebSocketServer } from '../dist/server.js';

const args = parseArgs(process.argv.slice(2));
const cases = readPositiveInt(args.cases, 120);
const steps = readPositiveInt(args.steps, 24);
let seed = readSeed(args.seed, 0x4f7c91d2);

for (let caseIndex = 0; caseIndex < cases; caseIndex++) {
  await runCase(caseIndex);
}

console.log(`frontier crdt-websocket fuzz passed cases=${cases} steps=${steps} seed=${readSeed(args.seed, 0x4f7c91d2)}`);

async function runCase(caseIndex) {
  fuzzWire(caseIndex);
  const server = createCrdtWebSocketServer({ host: '127.0.0.1', port: 0 });
  await server.ready;
  const address = server.address();
  const url = `ws://127.0.0.1:${address.port}`;
  const peerCount = 2 + randInt(3);
  const peers = [];
  for (let i = 0; i < peerCount; i++) {
    const peerId = `peer-${caseIndex}-${i}`;
    const doc = createCrdtDocument({ actorId: `actor-${caseIndex}-${i}` });
    const provider = createCrdtWebSocketProvider(
      createCrdtSyncEndpoint(doc, {
        documentId: `doc-${caseIndex}`,
        senderId: peerId,
        actorRangeSync: true
      }),
      {
        url,
        documentId: `doc-${caseIndex}`,
        peerId,
        peers: Array.from({ length: peerCount }, (_value, index) => `peer-${caseIndex}-${index}`).filter((id) => id !== peerId),
        WebSocket,
        syncOnConnect: true,
        reconnect: randInt(2) === 0
      }
    );
    peers.push({ peerId, doc, provider });
  }

  for (const peer of peers) await peer.provider.connect();
  await settle(peers);

  for (let step = 0; step < steps; step++) {
    const peer = peers[randInt(peerCount)];
    mutate(peer.doc, caseIndex, step);
    if (randInt(3) !== 0) await peer.provider.sync();
    if (randInt(12) === 0) {
      const target = peers[randInt(peerCount)];
      await target.provider.disconnect();
      await delay(1);
      await target.provider.connect();
    }
  }

  const convergence = await syncUntilConverged(peers);
  assert.strictEqual(convergence.valid, true, JSON.stringify(convergence.mismatches));
  for (const peer of peers) await peer.provider.disconnect();
  await server.close();
}

function mutate(doc, caseIndex, step) {
  const view = doc.toJSON() || {};
  switch (randInt(7)) {
    case 0:
      doc.set(`/items/k${randInt(8)}`, { caseIndex, step, value: randInt(1000) });
      break;
    case 1:
      doc.delete(`/items/k${randInt(8)}`);
      break;
    case 2: {
      const text = typeof view.body === 'string' ? view.body : '';
      doc.text('/body').insert(randInt(text.length + 1), String.fromCharCode(97 + randInt(26)));
      break;
    }
    case 3: {
      const text = typeof view.body === 'string' ? view.body : '';
      if (text.length > 0) doc.text('/body').delete(randInt(text.length), 1);
      break;
    }
    case 4:
      doc.counter('/count').increment(randInt(5) - 2);
      break;
    case 5: {
      const list = Array.isArray(view.list) ? view.list : [];
      doc.list('/list').insert(randInt(list.length + 1), { step, n: randInt(32) });
      break;
    }
    default: {
      const list = Array.isArray(view.list) ? view.list : [];
      if (list.length > 0) doc.list('/list').delete(randInt(list.length), 1);
      break;
    }
  }
}

function fuzzWire(caseIndex) {
  const frame = {
    kind: 'sync',
    documentId: `doc-${caseIndex}`,
    from: `peer-${randInt(8)}`,
    to: `peer-${randInt(8)}`,
    payload: Buffer.from(JSON.stringify({ caseIndex, value: randInt(1000) })).toString('base64')
  };
  assert.deepStrictEqual(decodeCrdtWebSocketFrame(encodeCrdtWebSocketFrame(frame)), frame);
  const binary = encodeCrdtWebSocketBinaryFrame(frame);
  assert.deepStrictEqual(normalizeFramePayload(decodeCrdtWebSocketFrame(binary)), frame);
  assert.throws(
    () => decodeCrdtWebSocketFrame(binary, { maxFrameBytes: Math.max(1, binary.byteLength - 1) }),
    /maxFrameBytes|exceeds/
  );
  assert.throws(() => decodeCrdtWebSocketFrame('{"kind":"sync","documentId":"","from":"a","to":"b","payload":""}'), /document id/);
}

function normalizeFramePayload(frame) {
  if (frame.kind !== 'sync' || typeof frame.payload === 'string') return frame;
  return { ...frame, payload: Buffer.from(frame.payload).toString('base64') };
}

async function settle(peers) {
  const deadline = Date.now() + 400;
  while (Date.now() < deadline) {
    let allKnowPeers = true;
    for (const peer of peers) {
      if (peer.provider.getPeerIds().length < peers.length - 1) allKnowPeers = false;
    }
    if (allKnowPeers) return;
    await delay(5);
  }
}

async function syncUntilConverged(peers) {
  let latest = checkCrdtSyncConvergence(peers.map((peer) => peer.doc));
  for (let round = 0; round < peers.length + 8; round++) {
    if (latest.valid) return latest;
    for (const peer of peers) await peer.provider.sync();
    await delay(20);
    latest = checkCrdtSyncConvergence(peers.map((peer) => peer.doc));
  }
  return latest;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randInt(max) {
  return nextRandom() % max;
}

function nextRandom() {
  seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
  return seed;
}

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--cases') out.cases = argv[++i];
    else if (arg === '--steps') out.steps = argv[++i];
    else if (arg === '--seed') out.seed = argv[++i];
    else if (arg === '--help' || arg === '-h') {
      console.log('Usage: node test/fuzz.mjs [--cases 120] [--steps 24] [--seed number]');
      process.exit(0);
    } else {
      throw new Error('unknown argument: ' + arg);
    }
  }
  return out;
}

function readPositiveInt(value, fallback) {
  if (value === undefined) return fallback;
  const number = Number(value);
  if (!Number.isInteger(number) || number <= 0) throw new Error('expected positive integer, got ' + value);
  return number;
}

function readSeed(value, fallback) {
  if (value === undefined) return fallback >>> 0;
  const number = Number(value);
  if (!Number.isInteger(number)) throw new Error('expected integer seed, got ' + value);
  return number >>> 0;
}

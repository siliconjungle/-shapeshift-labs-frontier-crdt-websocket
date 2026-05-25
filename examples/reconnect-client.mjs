import { WebSocket } from 'ws';
import { createCrdtDocument } from '@shapeshift-labs/frontier-crdt';
import { createCrdtSyncEndpoint } from '@shapeshift-labs/frontier-crdt-sync';
import { createCrdtWebSocketProvider } from '@shapeshift-labs/frontier-crdt-websocket';

const url = process.env.FRONTIER_WS_URL || 'ws://127.0.0.1:8787';
const peerId = process.env.PEER_ID || `node-${Math.random().toString(36).slice(2)}`;
const documentId = process.env.DOCUMENT_ID || 'demo';
const doc = createCrdtDocument({ actorId: peerId });
const endpoint = createCrdtSyncEndpoint(doc, { documentId, senderId: peerId, actorRangeSync: true });

const provider = createCrdtWebSocketProvider(endpoint, {
  url,
  documentId,
  peerId,
  WebSocket,
  syncOnConnect: true,
  reconnect: true,
  reconnectDelayMs: 100,
  maxReconnectDelayMs: 2000,
  reconnectMaxAttempts: Number.POSITIVE_INFINITY,
  heartbeatIntervalMs: 30000,
  heartbeatTimeoutMs: 10000,
  onPeerJoin: (peer) => console.log(`peer joined: ${peer}`),
  onPeerLeave: (peer) => console.log(`peer left: ${peer}`),
  onError: (error) => console.error('websocket sync error', error)
});

await provider.connect();
console.log(`connected peer=${peerId} document=${documentId}`);

let counter = 0;
setInterval(async () => {
  doc.set('/lastSeen', { peerId, counter: counter++, at: Date.now() });
  await provider.sync();
  console.log(JSON.stringify(doc.toJSON()));
}, 2000);

process.on('SIGINT', async () => {
  await provider.disconnect();
  process.exit(0);
});

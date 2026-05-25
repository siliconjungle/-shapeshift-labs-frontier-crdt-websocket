import { createCrdtWebSocketServer } from '@shapeshift-labs/frontier-crdt-websocket/server';

const port = Number(process.env.PORT || 8787);

const server = createCrdtWebSocketServer({
  host: '127.0.0.1',
  port,
  frameEncoding: 'binary',
  maxFrameBytes: 1024 * 1024,
  maxBufferedAmount: 16 * 1024 * 1024,
  heartbeatIntervalMs: 30000,
  heartbeatTimeoutMs: 10000,
  authenticate: () => true,
  authorizeRoom: ({ peerId, documentId }) => {
    console.log(`admit peer=${peerId} document=${documentId}`);
    return true;
  }
});

await server.ready;
console.log(`Frontier CRDT WebSocket server listening on ws://127.0.0.1:${port}`);

process.on('SIGINT', async () => {
  await server.close();
  process.exit(0);
});

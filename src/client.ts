import { createCrdtSyncProvider } from '@shapeshift-labs/frontier-crdt-sync/provider';
import type { CrdtSyncEndpoint } from '@shapeshift-labs/frontier-crdt-sync/sync';
import type {
  CrdtSyncMessageReceiver,
  CrdtSyncProviderOptions,
  CrdtSyncTransportPayload
} from '@shapeshift-labs/frontier-crdt-sync/provider';
import {
  createCrdtWebSocketSyncFrame,
  decodeCrdtWebSocketFrame,
  decodeCrdtWebSocketSyncPayload,
  encodeCrdtWebSocketTransportFrame,
  assertDocumentId,
  assertPeerId
} from './wire.js';
import {
  CRDT_WEBSOCKET_CLOSE_HEARTBEAT_TIMEOUT,
  CRDT_WEBSOCKET_DEFAULT_MAX_FRAME_BYTES
} from './constants.js';
import type {
  CrdtWebSocketClientTransport,
  CrdtWebSocketClientTransportOptions,
  CrdtWebSocketFrame,
  CrdtWebSocketFrameEncoding,
  CrdtWebSocketLike,
  CrdtWebSocketProvider,
  CrdtWebSocketProviderOptions
} from './types.js';

const WS_OPEN = 1;
const DEFAULT_MAX_QUEUED_FRAMES = 1024;
const DEFAULT_MAX_QUEUED_BYTES = 4 * 1024 * 1024;
const DEFAULT_MAX_BUFFERED_AMOUNT = 16 * 1024 * 1024;
const DEFAULT_SEND_TIMEOUT_MS = 5000;

export function createCrdtWebSocketClientTransport(options: CrdtWebSocketClientTransportOptions): CrdtWebSocketClientTransport {
  return new FrontierCrdtWebSocketClientTransport(options);
}

export function createCrdtWebSocketProvider(endpoint: CrdtSyncEndpoint, options: CrdtWebSocketProviderOptions): CrdtWebSocketProvider {
  let provider: CrdtWebSocketProvider | undefined;
  const autoSync = options.autoSyncOnPeerJoin !== false;
  const transport = createCrdtWebSocketClientTransport({
    ...options,
    onPeerJoin: async (peerId) => {
      options.onPeerJoin?.(peerId);
      provider?.addPeer(peerId);
      if (autoSync) await provider?.sync(peerId);
    },
    onPeerLeave: async (peerId) => {
      options.onPeerLeave?.(peerId);
      provider?.removePeer(peerId);
    }
  });
  provider = createCrdtSyncProvider(endpoint, {
    peers: options.peers,
    encodeMessages: options.encodeMessages,
    syncOnConnect: options.syncOnConnect,
    transport
  } satisfies CrdtSyncProviderOptions) as CrdtWebSocketProvider;
  Object.defineProperty(provider, 'transport', { value: transport, enumerable: true });
  return provider;
}

class FrontierCrdtWebSocketClientTransport implements CrdtWebSocketClientTransport {
  readonly peerId: string;
  readonly documentId: string;
  readonly url: string;
  private readonly protocols?: string | string[];
  private readonly WebSocketCtor: CrdtWebSocketClientTransportOptions['WebSocket'];
  private readonly auth?: CrdtWebSocketClientTransportOptions['auth'];
  private readonly frameEncoding: CrdtWebSocketFrameEncoding;
  private readonly maxFrameBytes: number;
  private readonly maxQueuedFrames: number;
  private readonly maxQueuedBytes: number;
  private readonly maxBufferedAmount: number;
  private readonly sendTimeoutMs: number;
  private readonly heartbeatIntervalMs: number;
  private readonly heartbeatTimeoutMs: number;
  private readonly reconnect: boolean;
  private readonly reconnectDelayMs: number;
  private readonly maxReconnectDelayMs: number;
  private readonly reconnectMaxAttempts: number;
  private readonly reconnectBackoffFactor: number;
  private readonly reconnectJitterRatio: number;
  private readonly onPeerJoin?: CrdtWebSocketClientTransportOptions['onPeerJoin'];
  private readonly onPeerLeave?: CrdtWebSocketClientTransportOptions['onPeerLeave'];
  private readonly onError?: CrdtWebSocketClientTransportOptions['onError'];
  private readonly peerIds = new Set<string>();
  private readonly pending: Array<string | Uint8Array> = [];
  private pendingBytes = 0;
  private receiver: CrdtSyncMessageReceiver | undefined;
  private socket: CrdtWebSocketLike | undefined;
  private connectPromise: Promise<void> | undefined;
  private cleanup: (() => void)[] = [];
  private closedByUser = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private reconnectAttempt = 0;
  private heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  private heartbeatNonce: string | undefined;
  private heartbeatSentAt = 0;

  constructor(options: CrdtWebSocketClientTransportOptions) {
    assertPeerId(options.peerId);
    assertDocumentId(options.documentId);
    this.peerId = options.peerId;
    this.documentId = options.documentId;
    this.url = String(options.url);
    this.protocols = options.protocols;
    this.WebSocketCtor = options.WebSocket ?? readGlobalWebSocket();
    if (this.WebSocketCtor === undefined) throw new TypeError('WebSocket constructor is required');
    this.auth = options.auth;
    this.frameEncoding = options.frameEncoding ?? 'binary';
    this.maxFrameBytes = Math.max(1, Math.floor(options.maxFrameBytes ?? CRDT_WEBSOCKET_DEFAULT_MAX_FRAME_BYTES));
    this.maxQueuedFrames = Math.max(0, Math.floor(options.maxQueuedFrames ?? DEFAULT_MAX_QUEUED_FRAMES));
    this.maxQueuedBytes = Math.max(0, Math.floor(options.maxQueuedBytes ?? DEFAULT_MAX_QUEUED_BYTES));
    this.maxBufferedAmount = Math.max(0, Math.floor(options.maxBufferedAmount ?? DEFAULT_MAX_BUFFERED_AMOUNT));
    this.sendTimeoutMs = Math.max(1, Math.floor(options.sendTimeoutMs ?? DEFAULT_SEND_TIMEOUT_MS));
    this.heartbeatIntervalMs = Math.max(0, Math.floor(options.heartbeatIntervalMs ?? 30000));
    this.heartbeatTimeoutMs = Math.max(1, Math.floor(options.heartbeatTimeoutMs ?? 10000));
    this.reconnect = options.reconnect === true;
    this.reconnectDelayMs = Math.max(1, options.reconnectDelayMs ?? 100);
    this.maxReconnectDelayMs = Math.max(this.reconnectDelayMs, options.maxReconnectDelayMs ?? 2000);
    this.reconnectMaxAttempts = Math.max(0, Math.floor(options.reconnectMaxAttempts ?? Number.POSITIVE_INFINITY));
    this.reconnectBackoffFactor = Math.max(1, options.reconnectBackoffFactor ?? 2);
    this.reconnectJitterRatio = Math.max(0, Math.min(1, options.reconnectJitterRatio ?? 0.1));
    this.onPeerJoin = options.onPeerJoin;
    this.onPeerLeave = options.onPeerLeave;
    this.onError = options.onError;
  }

  isConnected(): boolean {
    return this.socket?.readyState === WS_OPEN;
  }

  getPeerIds(): string[] {
    return Array.from(this.peerIds).sort();
  }

  connect(): Promise<void> {
    if (this.isConnected()) return Promise.resolve();
    if (this.connectPromise !== undefined) return this.connectPromise;
    this.closedByUser = false;
    const socket = new this.WebSocketCtor!(this.url, this.protocols);
    socket.binaryType = 'arraybuffer';
    this.socket = socket;
    this.connectPromise = new Promise((resolve, reject) => {
      let settled = false;
      this.cleanupSocketListeners();
      this.cleanup.push(addSocketListener(socket, 'open', async () => {
        try {
          this.reconnectAttempt = 0;
          this.startHeartbeat();
          const auth = await resolveAuth(this.auth);
          await this.sendFrame(auth === undefined
            ? { kind: 'hello', peerId: this.peerId, documentId: this.documentId }
            : { kind: 'hello', peerId: this.peerId, documentId: this.documentId, auth });
          await this.flush();
          if (!settled) {
            settled = true;
            resolve();
          }
        } catch (error) {
          if (!settled) {
            settled = true;
            reject(error);
          }
        }
      }));
      this.cleanup.push(addSocketListener(socket, 'message', (event) => {
        this.handleIncoming(event).catch((error) => this.onError?.(error));
      }));
      this.cleanup.push(addSocketListener(socket, 'error', (error) => {
        this.onError?.(error);
        if (!settled) {
          settled = true;
          reject(error);
        }
      }));
      this.cleanup.push(addSocketListener(socket, 'close', () => {
        this.connectPromise = undefined;
        this.stopHeartbeat();
        this.cleanupSocketListeners();
        if (!settled) {
          settled = true;
          reject(new Error('Frontier CRDT WebSocket closed before opening'));
        }
        if (this.reconnect && !this.closedByUser) this.scheduleReconnect();
      }));
    });
    return this.connectPromise;
  }

  async disconnect(): Promise<void> {
    this.closedByUser = true;
    if (this.reconnectTimer !== undefined) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    const socket = this.socket;
    this.socket = undefined;
    this.connectPromise = undefined;
    this.stopHeartbeat();
    this.cleanupSocketListeners();
    if (socket && socket.readyState === WS_OPEN) socket.close();
  }

  async send(peerId: string, message: CrdtSyncTransportPayload): Promise<void> {
    assertPeerId(peerId);
    const encoded = this.encodeFrame(createCrdtWebSocketSyncFrame(this.documentId, this.peerId, peerId, message));
    if (!this.isConnected()) {
      this.enqueue(encoded);
      if (!this.closedByUser) await this.connect();
      return;
    }
    await this.sendEncoded(encoded);
  }

  subscribe(receiver: CrdtSyncMessageReceiver): () => void {
    this.receiver = receiver;
    return () => {
      if (this.receiver === receiver) this.receiver = undefined;
    };
  }

  private async handleIncoming(input: unknown): Promise<void> {
    const frame = decodeCrdtWebSocketFrame(input, { maxFrameBytes: this.maxFrameBytes });
    if ('documentId' in frame && frame.documentId !== undefined && frame.documentId !== this.documentId) return;
    switch (frame.kind) {
      case 'welcome':
        this.peerIds.clear();
        for (let i = 0; i < frame.peers.length; i++) await this.addPeer(frame.peers[i]);
        break;
      case 'peer-join':
        await this.addPeer(frame.peerId);
        break;
      case 'peer-leave':
        if (this.peerIds.delete(frame.peerId)) await this.onPeerLeave?.(frame.peerId);
        break;
      case 'sync':
        if (frame.to === this.peerId && this.receiver !== undefined) {
          await this.receiver(decodeCrdtWebSocketSyncPayload(frame), frame.from);
        }
        break;
      case 'ping':
        await this.sendFrame({ kind: 'pong', documentId: this.documentId, peerId: this.peerId, nonce: frame.nonce, time: frame.time });
        break;
      case 'pong':
        if (frame.nonce === this.heartbeatNonce) {
          this.heartbeatNonce = undefined;
          this.heartbeatSentAt = 0;
        }
        break;
      default:
        break;
    }
  }

  private async addPeer(peerId: string): Promise<void> {
    if (peerId === this.peerId || this.peerIds.has(peerId)) return;
    this.peerIds.add(peerId);
    await this.onPeerJoin?.(peerId);
  }

  private async flush(): Promise<void> {
    while (this.pending.length > 0 && this.isConnected()) {
      const encoded = this.pending.shift()!;
      this.pendingBytes -= encodedFrameBytes(encoded);
      await this.sendEncoded(encoded);
    }
  }

  private sendFrame(frame: CrdtWebSocketFrame): Promise<void> {
    return this.sendEncoded(this.encodeFrame(frame));
  }

  private encodeFrame(frame: CrdtWebSocketFrame): string | Uint8Array {
    return encodeCrdtWebSocketTransportFrame(frame, { encoding: this.frameEncoding });
  }

  private enqueue(encoded: string | Uint8Array): void {
    const bytes = encodedFrameBytes(encoded);
    if (bytes > this.maxFrameBytes) throw new RangeError('Frontier CRDT WebSocket frame exceeds maxFrameBytes');
    if (this.pending.length + 1 > this.maxQueuedFrames || this.pendingBytes + bytes > this.maxQueuedBytes) {
      throw new RangeError('Frontier CRDT WebSocket pending queue limit exceeded');
    }
    this.pending[this.pending.length] = encoded;
    this.pendingBytes += bytes;
  }

  private async sendEncoded(encoded: string | Uint8Array): Promise<void> {
    const socket = this.socket;
    if (socket === undefined || socket.readyState !== WS_OPEN) {
      this.enqueue(encoded);
      return;
    }
    if (encodedFrameBytes(encoded) > this.maxFrameBytes) throw new RangeError('Frontier CRDT WebSocket frame exceeds maxFrameBytes');
    await waitForBackpressure(socket, this.maxBufferedAmount, this.sendTimeoutMs);
    await sendSocket(socket, encoded);
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer !== undefined) return;
    if (this.reconnectAttempt >= this.reconnectMaxAttempts) {
      this.onError?.(new Error('Frontier CRDT WebSocket reconnect attempts exhausted'));
      return;
    }
    const attempt = this.reconnectAttempt++;
    const baseDelay = Math.min(this.maxReconnectDelayMs, this.reconnectDelayMs * Math.max(1, this.reconnectBackoffFactor ** attempt));
    const jitter = this.reconnectJitterRatio === 0 ? 0 : baseDelay * this.reconnectJitterRatio * Math.random();
    const delay = Math.min(this.maxReconnectDelayMs, baseDelay + jitter);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.connect().catch((error) => this.onError?.(error));
    }, delay);
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    if (this.heartbeatIntervalMs === 0) return;
    this.heartbeatTimer = setInterval(() => {
      if (!this.isConnected()) return;
      if (this.heartbeatNonce !== undefined && Date.now() - this.heartbeatSentAt > this.heartbeatTimeoutMs) {
        this.socket?.close(CRDT_WEBSOCKET_CLOSE_HEARTBEAT_TIMEOUT, 'heartbeat timeout');
        return;
      }
      if (this.heartbeatNonce !== undefined) return;
      this.heartbeatNonce = createNonce();
      this.heartbeatSentAt = Date.now();
      this.sendFrame({
        kind: 'ping',
        documentId: this.documentId,
        peerId: this.peerId,
        nonce: this.heartbeatNonce,
        time: this.heartbeatSentAt
      }).catch((error) => this.onError?.(error));
    }, this.heartbeatIntervalMs);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer !== undefined) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
    this.heartbeatNonce = undefined;
    this.heartbeatSentAt = 0;
  }

  private cleanupSocketListeners(): void {
    for (let i = 0; i < this.cleanup.length; i++) this.cleanup[i]();
    this.cleanup = [];
  }
}

async function resolveAuth(auth: CrdtWebSocketClientTransportOptions['auth']): Promise<unknown> {
  return typeof auth === 'function' ? await auth() : auth;
}

function readGlobalWebSocket(): CrdtWebSocketClientTransportOptions['WebSocket'] {
  const ctor = (globalThis as { WebSocket?: CrdtWebSocketClientTransportOptions['WebSocket'] }).WebSocket;
  return ctor;
}

function addSocketListener(socket: CrdtWebSocketLike, event: string, listener: (...args: unknown[]) => void): () => void {
  if (typeof socket.addEventListener === 'function') {
    socket.addEventListener(event, listener);
    return () => socket.removeEventListener?.(event, listener);
  }
  if (typeof socket.on === 'function') {
    socket.on(event, listener);
    return () => {
      if (typeof socket.off === 'function') socket.off(event, listener);
      else socket.removeListener?.(event, listener);
    };
  }
  const key = ('on' + event) as 'onopen' | 'onmessage' | 'onclose' | 'onerror';
  const previous = socket[key];
  socket[key] = listener as (event: unknown) => void;
  return () => {
    if (socket[key] === listener) socket[key] = previous ?? null;
  };
}

function sendSocket(socket: CrdtWebSocketLike, data: string | Uint8Array): Promise<void> {
  return new Promise((resolve, reject) => {
    let settled = false;
    try {
      socket.send(data, (error?: Error) => {
        if (settled) return;
        settled = true;
        if (error) reject(error);
        else resolve();
      });
      if (!settled && socket.send.length < 2) {
        settled = true;
        resolve();
      }
    } catch (error) {
      reject(error);
    }
  });
}

async function waitForBackpressure(socket: CrdtWebSocketLike, maxBufferedAmount: number, timeoutMs: number): Promise<void> {
  const start = Date.now();
  while ((socket.bufferedAmount ?? 0) > maxBufferedAmount) {
    if (Date.now() - start > timeoutMs) throw new Error('Frontier CRDT WebSocket backpressure timeout');
    await new Promise((resolve) => setTimeout(resolve, 4));
  }
}

function encodedFrameBytes(encoded: string | Uint8Array): number {
  return typeof encoded === 'string' ? encoded.length : encoded.byteLength;
}

function createNonce(): string {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

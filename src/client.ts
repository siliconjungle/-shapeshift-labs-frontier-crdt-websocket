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
  encodeCrdtWebSocketFrame,
  assertDocumentId,
  assertPeerId
} from './wire.js';
import type {
  CrdtWebSocketClientTransport,
  CrdtWebSocketClientTransportOptions,
  CrdtWebSocketFrame,
  CrdtWebSocketLike,
  CrdtWebSocketProvider,
  CrdtWebSocketProviderOptions
} from './types.js';

const WS_OPEN = 1;

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
  private readonly reconnect: boolean;
  private readonly reconnectDelayMs: number;
  private readonly maxReconnectDelayMs: number;
  private readonly onPeerJoin?: CrdtWebSocketClientTransportOptions['onPeerJoin'];
  private readonly onPeerLeave?: CrdtWebSocketClientTransportOptions['onPeerLeave'];
  private readonly onError?: CrdtWebSocketClientTransportOptions['onError'];
  private readonly peerIds = new Set<string>();
  private readonly pending: string[] = [];
  private receiver: CrdtSyncMessageReceiver | undefined;
  private socket: CrdtWebSocketLike | undefined;
  private connectPromise: Promise<void> | undefined;
  private cleanup: (() => void)[] = [];
  private closedByUser = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private reconnectAttempt = 0;

  constructor(options: CrdtWebSocketClientTransportOptions) {
    assertPeerId(options.peerId);
    assertDocumentId(options.documentId);
    this.peerId = options.peerId;
    this.documentId = options.documentId;
    this.url = String(options.url);
    this.protocols = options.protocols;
    this.WebSocketCtor = options.WebSocket ?? readGlobalWebSocket();
    if (this.WebSocketCtor === undefined) throw new TypeError('WebSocket constructor is required');
    this.reconnect = options.reconnect === true;
    this.reconnectDelayMs = Math.max(1, options.reconnectDelayMs ?? 100);
    this.maxReconnectDelayMs = Math.max(this.reconnectDelayMs, options.maxReconnectDelayMs ?? 2000);
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
          await this.sendFrame({ kind: 'hello', peerId: this.peerId, documentId: this.documentId });
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
    this.cleanupSocketListeners();
    if (socket && socket.readyState === WS_OPEN) socket.close();
  }

  async send(peerId: string, message: CrdtSyncTransportPayload): Promise<void> {
    assertPeerId(peerId);
    const encoded = encodeCrdtWebSocketFrame(createCrdtWebSocketSyncFrame(this.documentId, this.peerId, peerId, message));
    if (!this.isConnected()) {
      this.pending[this.pending.length] = encoded;
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
    const frame = decodeCrdtWebSocketFrame(input);
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
      await this.sendEncoded(encoded);
    }
  }

  private sendFrame(frame: CrdtWebSocketFrame): Promise<void> {
    return this.sendEncoded(encodeCrdtWebSocketFrame(frame));
  }

  private async sendEncoded(encoded: string): Promise<void> {
    const socket = this.socket;
    if (socket === undefined || socket.readyState !== WS_OPEN) {
      this.pending[this.pending.length] = encoded;
      return;
    }
    await sendSocket(socket, encoded);
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer !== undefined) return;
    const delay = Math.min(this.maxReconnectDelayMs, this.reconnectDelayMs * Math.max(1, 2 ** this.reconnectAttempt++));
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.connect().catch((error) => this.onError?.(error));
    }, delay);
  }

  private cleanupSocketListeners(): void {
    for (let i = 0; i < this.cleanup.length; i++) this.cleanup[i]();
    this.cleanup = [];
  }
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

function sendSocket(socket: CrdtWebSocketLike, data: string): Promise<void> {
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

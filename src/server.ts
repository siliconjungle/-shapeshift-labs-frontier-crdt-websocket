import { WebSocketServer } from 'ws';
import type { WebSocket, WebSocketServer as WsServer } from 'ws';
import {
  cloneCrdtWebSocketFrame,
  CrdtWebSocketFrameTooLargeError,
  decodeCrdtWebSocketFrame,
  encodeCrdtWebSocketTransportFrame
} from './wire.js';
import {
  CRDT_WEBSOCKET_CLOSE_BACKPRESSURE,
  CRDT_WEBSOCKET_CLOSE_HEARTBEAT_TIMEOUT,
  CRDT_WEBSOCKET_CLOSE_POLICY,
  CRDT_WEBSOCKET_CLOSE_TOO_LARGE,
  CRDT_WEBSOCKET_CLOSE_UNSUPPORTED,
  CRDT_WEBSOCKET_DEFAULT_MAX_FRAME_BYTES
} from './constants.js';
import type {
  CrdtWebSocketAuthResult,
  CrdtWebSocketFrame,
  CrdtWebSocketFrameEncoding,
  CrdtWebSocketHelloFrame,
  CrdtWebSocketRoomAdmissionResult,
  CrdtWebSocketServer,
  CrdtWebSocketServerOptions,
  CrdtWebSocketSyncFrame
} from './types.js';

interface PeerSocket {
  peerId: string;
  documentId: string;
  socket: WebSocket;
  principal?: unknown;
  pendingHeartbeatNonce?: string;
  heartbeatSentAt?: number;
}

interface PendingSocketAuth {
  request?: unknown;
  principal?: unknown;
  authReady?: Promise<void>;
  authRejected?: boolean;
}

const DEFAULT_MAX_BUFFERED_AMOUNT = 16 * 1024 * 1024;

export function createCrdtWebSocketServer(options?: CrdtWebSocketServerOptions): CrdtWebSocketServer {
  return new FrontierCrdtWebSocketServer(options);
}

class FrontierCrdtWebSocketServer implements CrdtWebSocketServer {
  readonly ready: Promise<void>;
  private readonly wss: WsServer;
  private readonly rooms = new Map<string, Map<string, WebSocket>>();
  private readonly sockets = new Map<WebSocket, PeerSocket>();
  private readonly pendingAuth = new Map<WebSocket, PendingSocketAuth>();
  private readonly frameEncoding: CrdtWebSocketFrameEncoding;
  private readonly maxFrameBytes: number;
  private readonly maxBufferedAmount: number;
  private readonly heartbeatIntervalMs: number;
  private readonly heartbeatTimeoutMs: number;
  private readonly authenticate?: CrdtWebSocketServerOptions['authenticate'];
  private readonly authorizeRoom?: CrdtWebSocketServerOptions['authorizeRoom'];
  private heartbeatTimer: ReturnType<typeof setInterval> | undefined;

  constructor(options?: CrdtWebSocketServerOptions) {
    this.frameEncoding = options?.frameEncoding ?? 'binary';
    this.maxFrameBytes = Math.max(1, Math.floor(options?.maxFrameBytes ?? options?.maxPayload ?? CRDT_WEBSOCKET_DEFAULT_MAX_FRAME_BYTES));
    this.maxBufferedAmount = Math.max(0, Math.floor(options?.maxBufferedAmount ?? DEFAULT_MAX_BUFFERED_AMOUNT));
    this.heartbeatIntervalMs = Math.max(0, Math.floor(options?.heartbeatIntervalMs ?? 30000));
    this.heartbeatTimeoutMs = Math.max(1, Math.floor(options?.heartbeatTimeoutMs ?? 10000));
    this.authenticate = options?.authenticate;
    this.authorizeRoom = options?.authorizeRoom;
    this.wss = new WebSocketServer({
      port: options?.port,
      host: options?.host,
      path: options?.path,
      server: options?.server as never,
      perMessageDeflate: options?.perMessageDeflate ?? false,
      maxPayload: this.maxFrameBytes
    });
    this.ready = new Promise((resolve, reject) => {
      const address = this.wss.address();
      if (address !== null) {
        resolve();
        return;
      }
      this.wss.once('listening', resolve);
      this.wss.once('error', reject);
    });
    this.wss.on('connection', (socket, request) => this.handleConnection(socket, request));
    this.startHeartbeat();
  }

  address(): unknown {
    return this.wss.address();
  }

  getDocumentIds(): string[] {
    return Array.from(this.rooms.keys()).sort();
  }

  getPeerIds(documentId?: string): string[] {
    if (documentId !== undefined) return Array.from(this.rooms.get(documentId)?.keys() ?? []).sort();
    const ids = new Set<string>();
    this.rooms.forEach((room) => room.forEach((_socket, peerId) => ids.add(peerId)));
    return Array.from(ids).sort();
  }

  close(): Promise<void> {
    if (this.heartbeatTimer !== undefined) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
    for (const socket of this.sockets.keys()) socket.close();
    for (const socket of this.pendingAuth.keys()) socket.close();
    this.rooms.clear();
    this.sockets.clear();
    this.pendingAuth.clear();
    return new Promise((resolve, reject) => {
      this.wss.close((error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  private handleConnection(socket: WebSocket, request?: unknown): void {
    const pending: PendingSocketAuth = { request };
    this.pendingAuth.set(socket, pending);
    pending.authReady = this.authenticateConnection(socket, request).catch(() => {
      pending.authRejected = true;
      socket.close(CRDT_WEBSOCKET_CLOSE_POLICY, 'authentication rejected');
    });
    socket.on('message', (data) => {
      try {
        const frame = decodeCrdtWebSocketFrame(data, { maxFrameBytes: this.maxFrameBytes });
        this.handleFrame(socket, frame).catch(() => socket.close(1011, 'Frontier CRDT WebSocket server error'));
      } catch (error) {
        const code = error instanceof CrdtWebSocketFrameTooLargeError
          ? CRDT_WEBSOCKET_CLOSE_TOO_LARGE
          : CRDT_WEBSOCKET_CLOSE_UNSUPPORTED;
        socket.close(code, 'invalid Frontier CRDT WebSocket frame');
      }
    });
    socket.on('error', () => this.removeSocket(socket));
    socket.on('close', () => this.removeSocket(socket));
  }

  private async handleFrame(socket: WebSocket, frame: CrdtWebSocketFrame): Promise<void> {
    switch (frame.kind) {
      case 'hello':
        await this.register(socket, frame);
        break;
      case 'sync':
        this.routeSync(socket, frame);
        break;
      case 'ping':
        this.send(socket, { kind: 'pong', documentId: frame.documentId, peerId: frame.peerId, nonce: frame.nonce, time: frame.time });
        break;
      case 'pong':
        this.recordPong(socket, frame.nonce);
        break;
      default:
        break;
    }
  }

  private async register(socket: WebSocket, frame: CrdtWebSocketHelloFrame): Promise<void> {
    const pending = this.pendingAuth.get(socket);
    await pending?.authReady;
    if (pending?.authRejected || socket.readyState !== socket.OPEN) return;
    const admission = normalizeAdmissionResult(await this.authorizeRoom?.({
      request: pending?.request,
      peerId: frame.peerId,
      documentId: frame.documentId,
      auth: frame.auth,
      principal: pending?.principal
    }));
    if (!admission.ok) {
      socket.close(admission.closeCode, admission.reason);
      return;
    }
    const previous = this.sockets.get(socket);
    if (previous !== undefined) this.removeSocket(socket);
    let room = this.rooms.get(frame.documentId);
    if (room === undefined) {
      room = new Map();
      this.rooms.set(frame.documentId, room);
    }
    const existing = room.get(frame.peerId);
    if (existing !== undefined && existing !== socket) {
      existing.close(1000, 'peer replaced');
      this.removeSocket(existing);
    }
    const peers = Array.from(room.keys()).sort();
    room.set(frame.peerId, socket);
    this.pendingAuth.delete(socket);
    this.sockets.set(socket, { peerId: frame.peerId, documentId: frame.documentId, socket, principal: pending?.principal });
    this.send(socket, { kind: 'welcome', peerId: frame.peerId, documentId: frame.documentId, peers });
    this.broadcast(frame.documentId, { kind: 'peer-join', peerId: frame.peerId, documentId: frame.documentId }, frame.peerId);
  }

  private routeSync(socket: WebSocket, frame: CrdtWebSocketSyncFrame): void {
    const peer = this.sockets.get(socket);
    if (peer === undefined || peer.documentId !== frame.documentId || peer.peerId !== frame.from) return;
    const target = this.rooms.get(frame.documentId)?.get(frame.to);
    if (target === undefined || target.readyState !== target.OPEN) return;
    this.send(target, cloneCrdtWebSocketFrame(frame));
  }

  private removeSocket(socket: WebSocket): void {
    this.pendingAuth.delete(socket);
    const peer = this.sockets.get(socket);
    if (peer === undefined) return;
    this.sockets.delete(socket);
    const room = this.rooms.get(peer.documentId);
    if (room?.get(peer.peerId) === socket) {
      room.delete(peer.peerId);
      if (room.size === 0) this.rooms.delete(peer.documentId);
      else this.broadcast(peer.documentId, { kind: 'peer-leave', peerId: peer.peerId, documentId: peer.documentId }, peer.peerId);
    }
  }

  private broadcast(documentId: string, frame: CrdtWebSocketFrame, exceptPeerId?: string): void {
    const room = this.rooms.get(documentId);
    if (room === undefined) return;
    room.forEach((socket, peerId) => {
      if (peerId !== exceptPeerId && socket.readyState === socket.OPEN) this.send(socket, frame);
    });
  }

  private send(socket: WebSocket, frame: CrdtWebSocketFrame): void {
    if (socket.bufferedAmount >= this.maxBufferedAmount) {
      socket.close(CRDT_WEBSOCKET_CLOSE_BACKPRESSURE, 'backpressure limit');
      return;
    }
    try {
      socket.send(encodeCrdtWebSocketTransportFrame(frame, { encoding: this.frameEncoding }), (error) => {
        if (error) socket.close(1011, 'Frontier CRDT WebSocket send failed');
      });
    } catch {
      socket.close(1011, 'Frontier CRDT WebSocket send failed');
    }
  }

  private async authenticateConnection(socket: WebSocket, request?: unknown): Promise<void> {
    if (this.authenticate === undefined) return;
    const result = normalizeAuthResult(await this.authenticate({ request }));
    if (!result.ok) {
      const pending = this.pendingAuth.get(socket);
      if (pending !== undefined) pending.authRejected = true;
      socket.close(result.closeCode, result.reason);
      return;
    }
    const pending = this.pendingAuth.get(socket);
    if (pending !== undefined) pending.principal = result.principal;
  }

  private startHeartbeat(): void {
    if (this.heartbeatIntervalMs === 0) return;
    this.heartbeatTimer = setInterval(() => {
      const now = Date.now();
      for (const peer of this.sockets.values()) {
        if (
          peer.pendingHeartbeatNonce !== undefined &&
          peer.heartbeatSentAt !== undefined &&
          now - peer.heartbeatSentAt > this.heartbeatTimeoutMs
        ) {
          peer.socket.close(CRDT_WEBSOCKET_CLOSE_HEARTBEAT_TIMEOUT, 'heartbeat timeout');
          continue;
        }
        if (peer.pendingHeartbeatNonce !== undefined) continue;
        peer.pendingHeartbeatNonce = createNonce();
        peer.heartbeatSentAt = now;
        this.send(peer.socket, {
          kind: 'ping',
          documentId: peer.documentId,
          peerId: peer.peerId,
          nonce: peer.pendingHeartbeatNonce,
          time: now
        });
      }
    }, this.heartbeatIntervalMs);
  }

  private recordPong(socket: WebSocket, nonce?: string): void {
    const peer = this.sockets.get(socket);
    if (peer === undefined) return;
    if (nonce === peer.pendingHeartbeatNonce) {
      peer.pendingHeartbeatNonce = undefined;
      peer.heartbeatSentAt = undefined;
    }
  }
}

function normalizeAuthResult(value: boolean | CrdtWebSocketAuthResult | void): Required<Pick<CrdtWebSocketAuthResult, 'ok' | 'closeCode' | 'reason'>> & CrdtWebSocketAuthResult {
  if (value === false) return { ok: false, closeCode: CRDT_WEBSOCKET_CLOSE_POLICY, reason: 'authentication rejected' };
  if (value === true || value === undefined) return { ok: true, closeCode: 1000, reason: '' };
  const result = value as CrdtWebSocketAuthResult;
  return {
    ...result,
    ok: result.ok !== false,
    closeCode: result.closeCode ?? CRDT_WEBSOCKET_CLOSE_POLICY,
    reason: result.reason ?? 'authentication rejected'
  };
}

function normalizeAdmissionResult(value: boolean | CrdtWebSocketRoomAdmissionResult | void): Required<CrdtWebSocketRoomAdmissionResult> {
  if (value === false) return { ok: false, closeCode: CRDT_WEBSOCKET_CLOSE_POLICY, reason: 'room admission rejected' };
  if (value === true || value === undefined) return { ok: true, closeCode: 1000, reason: '' };
  const result = value as CrdtWebSocketRoomAdmissionResult;
  return {
    ok: result.ok !== false,
    closeCode: result.closeCode ?? CRDT_WEBSOCKET_CLOSE_POLICY,
    reason: result.reason ?? 'room admission rejected'
  };
}

function createNonce(): string {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

import { WebSocketServer } from 'ws';
import type { WebSocket, WebSocketServer as WsServer } from 'ws';
import {
  cloneCrdtWebSocketFrame,
  decodeCrdtWebSocketFrame,
  encodeCrdtWebSocketFrame
} from './wire.js';
import type {
  CrdtWebSocketFrame,
  CrdtWebSocketHelloFrame,
  CrdtWebSocketServer,
  CrdtWebSocketServerOptions,
  CrdtWebSocketSyncFrame
} from './types.js';

interface PeerSocket {
  peerId: string;
  documentId: string;
  socket: WebSocket;
}

export function createCrdtWebSocketServer(options?: CrdtWebSocketServerOptions): CrdtWebSocketServer {
  return new FrontierCrdtWebSocketServer(options);
}

class FrontierCrdtWebSocketServer implements CrdtWebSocketServer {
  readonly ready: Promise<void>;
  private readonly wss: WsServer;
  private readonly rooms = new Map<string, Map<string, WebSocket>>();
  private readonly sockets = new Map<WebSocket, PeerSocket>();

  constructor(options?: CrdtWebSocketServerOptions) {
    this.wss = new WebSocketServer({
      port: options?.port,
      host: options?.host,
      path: options?.path,
      server: options?.server as never,
      perMessageDeflate: options?.perMessageDeflate ?? false,
      maxPayload: options?.maxPayload
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
    this.wss.on('connection', (socket) => this.handleConnection(socket));
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
    for (const socket of this.sockets.keys()) socket.close();
    this.rooms.clear();
    this.sockets.clear();
    return new Promise((resolve, reject) => {
      this.wss.close((error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  private handleConnection(socket: WebSocket): void {
    socket.on('message', (data) => {
      try {
        this.handleFrame(socket, decodeCrdtWebSocketFrame(data));
      } catch {
        socket.close(1003, 'invalid Frontier CRDT WebSocket frame');
      }
    });
    socket.on('close', () => this.removeSocket(socket));
  }

  private handleFrame(socket: WebSocket, frame: CrdtWebSocketFrame): void {
    switch (frame.kind) {
      case 'hello':
        this.register(socket, frame);
        break;
      case 'sync':
        this.routeSync(socket, frame);
        break;
      case 'ping':
        this.send(socket, { kind: 'pong', documentId: frame.documentId, peerId: frame.peerId, nonce: frame.nonce, time: frame.time });
        break;
      default:
        break;
    }
  }

  private register(socket: WebSocket, frame: CrdtWebSocketHelloFrame): void {
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
    this.sockets.set(socket, { peerId: frame.peerId, documentId: frame.documentId, socket });
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
    socket.send(encodeCrdtWebSocketFrame(frame));
  }
}

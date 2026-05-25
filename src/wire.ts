import {
  decodeCrdtSyncMessage,
  encodeCrdtSyncMessage
} from '@shapeshift-labs/frontier-crdt-sync/sync';
import type { CrdtSyncTransportPayload } from '@shapeshift-labs/frontier-crdt-sync/provider';
import type {
  CrdtWebSocketFrame,
  CrdtWebSocketHelloFrame,
  CrdtWebSocketPeerFrame,
  CrdtWebSocketPingFrame,
  CrdtWebSocketSyncFrame,
  CrdtWebSocketWelcomeFrame
} from './types.js';

const textDecoder = new TextDecoder();

export function encodeCrdtWebSocketFrame(frame: CrdtWebSocketFrame): string {
  return JSON.stringify(cloneCrdtWebSocketFrame(frame));
}

export function decodeCrdtWebSocketFrame(input: unknown): CrdtWebSocketFrame {
  if (typeof input === 'string') return parseFrame(input);
  if (input instanceof ArrayBuffer) return parseFrame(textDecoder.decode(new Uint8Array(input)));
  if (ArrayBuffer.isView(input)) {
    return parseFrame(textDecoder.decode(new Uint8Array(input.buffer, input.byteOffset, input.byteLength)));
  }
  if (input !== null && typeof input === 'object' && 'data' in input) {
    return decodeCrdtWebSocketFrame((input as { data: unknown }).data);
  }
  throw new TypeError('invalid Frontier CRDT WebSocket frame');
}

export function cloneCrdtWebSocketFrame(frame: CrdtWebSocketFrame): CrdtWebSocketFrame {
  switch (frame.kind) {
    case 'hello':
      assertPeerId(frame.peerId);
      assertDocumentId(frame.documentId);
      return { kind: 'hello', peerId: frame.peerId, documentId: frame.documentId };
    case 'welcome':
      assertPeerId(frame.peerId);
      assertDocumentId(frame.documentId);
      if (!Array.isArray(frame.peers)) throw new TypeError('invalid Frontier CRDT WebSocket peers');
      return { kind: 'welcome', peerId: frame.peerId, documentId: frame.documentId, peers: normalizePeers(frame.peers) };
    case 'peer-join':
    case 'peer-leave':
      assertPeerId(frame.peerId);
      assertDocumentId(frame.documentId);
      return { kind: frame.kind, peerId: frame.peerId, documentId: frame.documentId };
    case 'sync':
      assertPeerId(frame.from);
      assertPeerId(frame.to);
      assertDocumentId(frame.documentId);
      if (typeof frame.payload !== 'string') throw new TypeError('invalid Frontier CRDT WebSocket sync payload');
      return { kind: 'sync', documentId: frame.documentId, from: frame.from, to: frame.to, payload: frame.payload };
    case 'ping':
    case 'pong': {
      const out: CrdtWebSocketPingFrame = { kind: frame.kind };
      if (frame.documentId !== undefined) {
        assertDocumentId(frame.documentId);
        out.documentId = frame.documentId;
      }
      if (frame.peerId !== undefined) {
        assertPeerId(frame.peerId);
        out.peerId = frame.peerId;
      }
      if (frame.nonce !== undefined) {
        if (typeof frame.nonce !== 'string') throw new TypeError('invalid Frontier CRDT WebSocket ping nonce');
        out.nonce = frame.nonce;
      }
      if (frame.time !== undefined) {
        if (!Number.isFinite(frame.time)) throw new TypeError('invalid Frontier CRDT WebSocket ping time');
        out.time = frame.time;
      }
      return out;
    }
    default:
      throw new TypeError('invalid Frontier CRDT WebSocket frame kind');
  }
}

export function createCrdtWebSocketSyncFrame(
  documentId: string,
  from: string,
  to: string,
  payload: CrdtSyncTransportPayload
): CrdtWebSocketSyncFrame {
  return {
    kind: 'sync',
    documentId,
    from,
    to,
    payload: bytesToBase64(normalizeSyncPayload(payload))
  };
}

export function decodeCrdtWebSocketSyncPayload(frame: CrdtWebSocketSyncFrame): Uint8Array {
  return base64ToBytes((cloneCrdtWebSocketFrame(frame) as CrdtWebSocketSyncFrame).payload);
}

export function assertPeerId(peerId: string): void {
  if (typeof peerId !== 'string' || peerId.length === 0 || peerId.includes('/') || peerId.includes('?')) {
    throw new TypeError('Frontier CRDT WebSocket peer id must be a non-empty path-safe string');
  }
}

export function assertDocumentId(documentId: string): void {
  if (typeof documentId !== 'string' || documentId.length === 0) {
    throw new TypeError('Frontier CRDT WebSocket document id must be a non-empty string');
  }
}

function parseFrame(text: string): CrdtWebSocketFrame {
  const value = JSON.parse(text) as unknown;
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError('invalid Frontier CRDT WebSocket frame');
  }
  const frame = value as CrdtWebSocketFrame;
  if (frame.kind === 'hello') return cloneCrdtWebSocketFrame(frame as CrdtWebSocketHelloFrame);
  if (frame.kind === 'welcome') return cloneCrdtWebSocketFrame(frame as CrdtWebSocketWelcomeFrame);
  if (frame.kind === 'peer-join' || frame.kind === 'peer-leave') return cloneCrdtWebSocketFrame(frame as CrdtWebSocketPeerFrame);
  if (frame.kind === 'sync') return cloneCrdtWebSocketFrame(frame as CrdtWebSocketSyncFrame);
  if (frame.kind === 'ping' || frame.kind === 'pong') return cloneCrdtWebSocketFrame(frame as CrdtWebSocketPingFrame);
  throw new TypeError('invalid Frontier CRDT WebSocket frame kind');
}

function normalizePeers(peers: readonly string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (let i = 0; i < peers.length; i++) {
    const peerId = peers[i];
    assertPeerId(peerId);
    if (!seen.has(peerId)) {
      seen.add(peerId);
      out[out.length] = peerId;
    }
  }
  out.sort();
  return out;
}

function normalizeSyncPayload(payload: CrdtSyncTransportPayload): Uint8Array {
  if (payload instanceof Uint8Array) return payload;
  return encodeCrdtSyncMessage(decodeCrdtSyncMessage(payload));
}

function bytesToBase64(bytes: Uint8Array): string {
  const maybeBuffer = (globalThis as { Buffer?: { from(input: Uint8Array): { toString(encoding: string): string } } }).Buffer;
  if (maybeBuffer !== undefined) return maybeBuffer.from(bytes).toString('base64');
  let binary = '';
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary);
}

function base64ToBytes(base64: string): Uint8Array {
  const maybeBuffer = (globalThis as { Buffer?: { from(input: string, encoding: string): Uint8Array } }).Buffer;
  if (maybeBuffer !== undefined) return new Uint8Array(maybeBuffer.from(base64, 'base64'));
  const binary = atob(base64);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

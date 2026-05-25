import {
  decodeCrdtSyncMessage,
  encodeCrdtSyncMessage
} from '@shapeshift-labs/frontier-crdt-sync/sync';
import type { CrdtSyncTransportPayload } from '@shapeshift-labs/frontier-crdt-sync/provider';
import { CRDT_WEBSOCKET_DEFAULT_MAX_FRAME_BYTES } from './constants.js';
import type {
  CrdtWebSocketFrame,
  CrdtWebSocketFrameEncoding,
  CrdtWebSocketHelloFrame,
  CrdtWebSocketPeerFrame,
  CrdtWebSocketPingFrame,
  CrdtWebSocketSyncFrame,
  CrdtWebSocketWelcomeFrame
} from './types.js';

export interface CrdtWebSocketDecodeOptions {
  maxFrameBytes?: number;
}

export interface CrdtWebSocketEncodeOptions {
  encoding?: CrdtWebSocketFrameEncoding;
}

const textDecoder = new TextDecoder();
const textEncoder = new TextEncoder();
const binaryMagic = new Uint8Array([70, 67, 87, 83]);
const binaryVersion = 1;
const base64ByBytes = new WeakMap<Uint8Array, string>();
const kindToCode: Record<CrdtWebSocketFrame['kind'], number> = {
  hello: 1,
  welcome: 2,
  'peer-join': 3,
  'peer-leave': 4,
  sync: 5,
  ping: 6,
  pong: 7
};

export function encodeCrdtWebSocketFrame(frame: CrdtWebSocketFrame): string {
  return JSON.stringify(toJsonFrame(frame));
}

export function encodeCrdtWebSocketBinaryFrame(frame: CrdtWebSocketFrame): Uint8Array {
  return new BinaryFrameWriter(cloneCrdtWebSocketFrame(frame)).finish();
}

export function encodeCrdtWebSocketTransportFrame(
  frame: CrdtWebSocketFrame,
  options?: CrdtWebSocketEncodeOptions
): string | Uint8Array {
  return options?.encoding === 'json'
    ? encodeCrdtWebSocketFrame(frame)
    : encodeCrdtWebSocketBinaryFrame(frame);
}

export function decodeCrdtWebSocketFrame(input: unknown, options?: CrdtWebSocketDecodeOptions): CrdtWebSocketFrame {
  const maxFrameBytes = options?.maxFrameBytes ?? CRDT_WEBSOCKET_DEFAULT_MAX_FRAME_BYTES;
  if (typeof input === 'string') {
    if (textEncoder.encode(input).byteLength > maxFrameBytes) {
      throw new CrdtWebSocketFrameTooLargeError('Frontier CRDT WebSocket frame exceeds maxFrameBytes');
    }
    return parseFrame(input);
  }
  if (input instanceof ArrayBuffer) return decodeBytes(new Uint8Array(input), maxFrameBytes);
  if (ArrayBuffer.isView(input)) {
    return decodeBytes(new Uint8Array(input.buffer, input.byteOffset, input.byteLength), maxFrameBytes);
  }
  if (input !== null && typeof input === 'object' && 'data' in input) {
    return decodeCrdtWebSocketFrame((input as { data: unknown }).data, options);
  }
  throw new TypeError('invalid Frontier CRDT WebSocket frame');
}

export function cloneCrdtWebSocketFrame(frame: CrdtWebSocketFrame): CrdtWebSocketFrame {
  switch (frame.kind) {
    case 'hello':
      assertPeerId(frame.peerId);
      assertDocumentId(frame.documentId);
      return frame.auth === undefined
        ? { kind: 'hello', peerId: frame.peerId, documentId: frame.documentId }
        : { kind: 'hello', peerId: frame.peerId, documentId: frame.documentId, auth: cloneAuth(frame.auth) };
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
      return { kind: 'sync', documentId: frame.documentId, from: frame.from, to: frame.to, payload: cloneSyncPayload(frame.payload) };
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
    payload: normalizeSyncPayload(payload)
  };
}

export function decodeCrdtWebSocketSyncPayload(frame: CrdtWebSocketSyncFrame): Uint8Array {
  return syncPayloadToBytes((cloneCrdtWebSocketFrame(frame) as CrdtWebSocketSyncFrame).payload);
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

function decodeBytes(bytes: Uint8Array, maxFrameBytes: number): CrdtWebSocketFrame {
  if (bytes.byteLength > maxFrameBytes) {
    throw new CrdtWebSocketFrameTooLargeError('Frontier CRDT WebSocket frame exceeds maxFrameBytes');
  }
  if (isBinaryFrame(bytes)) return new BinaryFrameReader(bytes).read();
  return parseFrame(textDecoder.decode(bytes));
}

export class CrdtWebSocketFrameTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CrdtWebSocketFrameTooLargeError';
  }
}

class BinaryFrameWriter {
  private readonly parts: Uint8Array[] = [];

  constructor(frame: CrdtWebSocketFrame) {
    this.writeBytes(binaryMagic);
    this.writeByte(binaryVersion);
    this.writeByte(kindToCode[frame.kind]);
    switch (frame.kind) {
      case 'hello':
        this.writeString(frame.peerId);
        this.writeString(frame.documentId);
        this.writeOptionalJson(frame.auth);
        break;
      case 'welcome':
        this.writeString(frame.peerId);
        this.writeString(frame.documentId);
        this.writeUint32(frame.peers.length);
        for (let i = 0; i < frame.peers.length; i++) this.writeString(frame.peers[i]);
        break;
      case 'peer-join':
      case 'peer-leave':
        this.writeString(frame.peerId);
        this.writeString(frame.documentId);
        break;
      case 'sync':
        this.writeString(frame.documentId);
        this.writeString(frame.from);
        this.writeString(frame.to);
        this.writeBytesWithLength(syncPayloadToBytes(frame.payload));
        break;
      case 'ping':
      case 'pong': {
        let flags = 0;
        if (frame.documentId !== undefined) flags |= 1;
        if (frame.peerId !== undefined) flags |= 2;
        if (frame.nonce !== undefined) flags |= 4;
        if (frame.time !== undefined) flags |= 8;
        this.writeByte(flags);
        if (frame.documentId !== undefined) this.writeString(frame.documentId);
        if (frame.peerId !== undefined) this.writeString(frame.peerId);
        if (frame.nonce !== undefined) this.writeString(frame.nonce);
        if (frame.time !== undefined) this.writeFloat64(frame.time);
        break;
      }
    }
  }

  finish(): Uint8Array {
    let length = 0;
    for (let i = 0; i < this.parts.length; i++) length += this.parts[i].byteLength;
    const out = new Uint8Array(length);
    let offset = 0;
    for (let i = 0; i < this.parts.length; i++) {
      out.set(this.parts[i], offset);
      offset += this.parts[i].byteLength;
    }
    return out;
  }

  private writeByte(value: number): void {
    this.parts.push(new Uint8Array([value & 255]));
  }

  private writeUint32(value: number): void {
    if (!Number.isSafeInteger(value) || value < 0 || value > 0xffffffff) throw new TypeError('invalid Frontier CRDT WebSocket binary length');
    const bytes = new Uint8Array(4);
    new DataView(bytes.buffer).setUint32(0, value);
    this.parts.push(bytes);
  }

  private writeFloat64(value: number): void {
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setFloat64(0, value);
    this.parts.push(bytes);
  }

  private writeString(value: string): void {
    this.writeBytesWithLength(textEncoder.encode(value));
  }

  private writeOptionalJson(value: unknown): void {
    if (value === undefined) {
      this.writeByte(0);
      return;
    }
    this.writeByte(1);
    this.writeString(JSON.stringify(value));
  }

  private writeBytesWithLength(bytes: Uint8Array): void {
    this.writeUint32(bytes.byteLength);
    this.writeBytes(bytes);
  }

  private writeBytes(bytes: Uint8Array): void {
    this.parts.push(bytes);
  }
}

class BinaryFrameReader {
  private offset = 0;

  constructor(private readonly bytes: Uint8Array) {}

  read(): CrdtWebSocketFrame {
    this.expectMagic();
    const version = this.readByte();
    if (version !== binaryVersion) throw new TypeError('invalid Frontier CRDT WebSocket binary version');
    const kind = this.readByte();
    let frame: CrdtWebSocketFrame;
    switch (kind) {
      case 1: {
        const peerId = this.readString();
        const documentId = this.readString();
        const auth = this.readOptionalJson();
        frame = auth === undefined ? { kind: 'hello', peerId, documentId } : { kind: 'hello', peerId, documentId, auth };
        break;
      }
      case 2: {
        const peerId = this.readString();
        const documentId = this.readString();
        const count = this.readUint32();
        const peers = new Array<string>(count);
        for (let i = 0; i < count; i++) peers[i] = this.readString();
        frame = { kind: 'welcome', peerId, documentId, peers };
        break;
      }
      case 3:
      case 4:
        frame = { kind: kind === 3 ? 'peer-join' : 'peer-leave', peerId: this.readString(), documentId: this.readString() };
        break;
      case 5:
        frame = {
          kind: 'sync',
          documentId: this.readString(),
          from: this.readString(),
          to: this.readString(),
          payload: this.readBytesWithLength()
        };
        break;
      case 6:
      case 7: {
        const flags = this.readByte();
        const out: CrdtWebSocketPingFrame = { kind: kind === 6 ? 'ping' : 'pong' };
        if (flags & 1) out.documentId = this.readString();
        if (flags & 2) out.peerId = this.readString();
        if (flags & 4) out.nonce = this.readString();
        if (flags & 8) out.time = this.readFloat64();
        frame = out;
        break;
      }
      default:
        throw new TypeError('invalid Frontier CRDT WebSocket binary frame kind');
    }
    if (this.offset !== this.bytes.byteLength) throw new TypeError('invalid Frontier CRDT WebSocket binary frame');
    return cloneCrdtWebSocketFrame(frame);
  }

  private expectMagic(): void {
    for (let i = 0; i < binaryMagic.length; i++) {
      if (this.readByte() !== binaryMagic[i]) throw new TypeError('invalid Frontier CRDT WebSocket binary magic');
    }
  }

  private readByte(): number {
    if (this.offset >= this.bytes.byteLength) throw new TypeError('truncated Frontier CRDT WebSocket binary frame');
    return this.bytes[this.offset++];
  }

  private readUint32(): number {
    if (this.offset + 4 > this.bytes.byteLength) throw new TypeError('truncated Frontier CRDT WebSocket binary frame');
    const value = new DataView(this.bytes.buffer, this.bytes.byteOffset + this.offset, 4).getUint32(0);
    this.offset += 4;
    return value;
  }

  private readFloat64(): number {
    if (this.offset + 8 > this.bytes.byteLength) throw new TypeError('truncated Frontier CRDT WebSocket binary frame');
    const value = new DataView(this.bytes.buffer, this.bytes.byteOffset + this.offset, 8).getFloat64(0);
    this.offset += 8;
    return value;
  }

  private readString(): string {
    return textDecoder.decode(this.readBytesWithLength());
  }

  private readOptionalJson(): unknown {
    const present = this.readByte();
    if (present === 0) return undefined;
    if (present !== 1) throw new TypeError('invalid Frontier CRDT WebSocket binary optional value');
    return JSON.parse(this.readString()) as unknown;
  }

  private readBytesWithLength(): Uint8Array {
    const length = this.readUint32();
    if (this.offset + length > this.bytes.byteLength) throw new TypeError('truncated Frontier CRDT WebSocket binary frame');
    const out = this.bytes.slice(this.offset, this.offset + length);
    this.offset += length;
    return out;
  }
}

function isBinaryFrame(bytes: Uint8Array): boolean {
  if (bytes.byteLength < binaryMagic.length + 2) return false;
  for (let i = 0; i < binaryMagic.length; i++) {
    if (bytes[i] !== binaryMagic[i]) return false;
  }
  return true;
}

function cloneAuth(value: unknown): unknown {
  if (value === null || typeof value !== 'object') return value;
  return JSON.parse(JSON.stringify(value)) as unknown;
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

function cloneSyncPayload(payload: string | Uint8Array): string | Uint8Array {
  if (typeof payload === 'string') return payload;
  if (payload instanceof Uint8Array) return payload;
  throw new TypeError('invalid Frontier CRDT WebSocket sync payload');
}

function syncPayloadToBytes(payload: string | Uint8Array): Uint8Array {
  return typeof payload === 'string' ? base64ToBytes(payload) : payload;
}

function toJsonFrame(frame: CrdtWebSocketFrame): CrdtWebSocketFrame {
  const cloned = cloneCrdtWebSocketFrame(frame);
  return cloned.kind === 'sync' && cloned.payload instanceof Uint8Array
    ? { ...cloned, payload: bytesToBase64(cloned.payload) }
    : cloned;
}

function bytesToBase64(bytes: Uint8Array): string {
  const cached = base64ByBytes.get(bytes);
  if (cached !== undefined) return cached;
  const maybeBuffer = (globalThis as { Buffer?: { from(input: Uint8Array): { toString(encoding: string): string } } }).Buffer;
  let base64: string;
  if (maybeBuffer !== undefined) base64 = maybeBuffer.from(bytes).toString('base64');
  else {
    let binary = '';
    for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
    base64 = btoa(binary);
  }
  base64ByBytes.set(bytes, base64);
  return base64;
}

function base64ToBytes(base64: string): Uint8Array {
  validateBase64Payload(base64);
  const maybeBuffer = (globalThis as { Buffer?: { from(input: string, encoding: string): Uint8Array } }).Buffer;
  if (maybeBuffer !== undefined) return new Uint8Array(maybeBuffer.from(base64, 'base64'));
  const binary = atob(base64);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

function validateBase64Payload(base64: string): void {
  if (
    base64.length % 4 !== 0 ||
    /[^A-Za-z0-9+/=]/.test(base64) ||
    /=[^=]/.test(base64)
  ) {
    throw new TypeError('invalid Frontier CRDT WebSocket base64 payload');
  }
}

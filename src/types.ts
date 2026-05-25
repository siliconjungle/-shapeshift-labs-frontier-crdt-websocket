import type { CrdtSyncEndpoint } from '@shapeshift-labs/frontier-crdt-sync/sync';
import type {
  CrdtSyncMessageReceiver,
  CrdtSyncProvider,
  CrdtSyncProviderOptions,
  CrdtSyncTransport
} from '@shapeshift-labs/frontier-crdt-sync/provider';

export type CrdtWebSocketFrameKind =
  | 'hello'
  | 'welcome'
  | 'peer-join'
  | 'peer-leave'
  | 'sync'
  | 'ping'
  | 'pong';

export interface CrdtWebSocketHelloFrame {
  kind: 'hello';
  peerId: string;
  documentId: string;
}

export interface CrdtWebSocketWelcomeFrame {
  kind: 'welcome';
  peerId: string;
  documentId: string;
  peers: string[];
}

export interface CrdtWebSocketPeerFrame {
  kind: 'peer-join' | 'peer-leave';
  peerId: string;
  documentId: string;
}

export interface CrdtWebSocketSyncFrame {
  kind: 'sync';
  documentId: string;
  from: string;
  to: string;
  payload: string;
}

export interface CrdtWebSocketPingFrame {
  kind: 'ping' | 'pong';
  documentId?: string;
  peerId?: string;
  nonce?: string;
  time?: number;
}

export type CrdtWebSocketFrame =
  | CrdtWebSocketHelloFrame
  | CrdtWebSocketWelcomeFrame
  | CrdtWebSocketPeerFrame
  | CrdtWebSocketSyncFrame
  | CrdtWebSocketPingFrame;

export interface CrdtWebSocketLike {
  readyState: number;
  binaryType?: string;
  send(data: string | ArrayBufferLike | ArrayBufferView, callback?: (error?: Error) => void): void;
  close(code?: number, reason?: string): void;
  addEventListener?(type: string, listener: (...args: unknown[]) => void): void;
  removeEventListener?(type: string, listener: (...args: unknown[]) => void): void;
  on?(type: string, listener: (...args: unknown[]) => void): void;
  off?(type: string, listener: (...args: unknown[]) => void): void;
  once?(type: string, listener: (...args: unknown[]) => void): void;
  removeListener?(type: string, listener: (...args: unknown[]) => void): void;
  onopen?: ((event: unknown) => void) | null;
  onmessage?: ((event: unknown) => void) | null;
  onclose?: ((event: unknown) => void) | null;
  onerror?: ((event: unknown) => void) | null;
}

export interface CrdtWebSocketConstructor {
  new(url: string, protocols?: string | string[]): CrdtWebSocketLike;
}

export type CrdtWebSocketPeerListener = (peerId: string) => void | Promise<void>;
export type CrdtWebSocketErrorListener = (error: unknown) => void;

export interface CrdtWebSocketClientTransportOptions {
  url: string | URL;
  peerId: string;
  documentId: string;
  protocols?: string | string[];
  WebSocket?: CrdtWebSocketConstructor;
  reconnect?: boolean;
  reconnectDelayMs?: number;
  maxReconnectDelayMs?: number;
  onPeerJoin?: CrdtWebSocketPeerListener;
  onPeerLeave?: CrdtWebSocketPeerListener;
  onError?: CrdtWebSocketErrorListener;
}

export interface CrdtWebSocketProviderOptions extends Omit<CrdtSyncProviderOptions, 'transport'>, CrdtWebSocketClientTransportOptions {
  autoSyncOnPeerJoin?: boolean;
}

export interface CrdtWebSocketClientTransport extends CrdtSyncTransport {
  readonly peerId: string;
  readonly documentId: string;
  readonly url: string;
  isConnected(): boolean;
  getPeerIds(): string[];
}

export interface CrdtWebSocketProvider extends CrdtSyncProvider {
  readonly transport: CrdtWebSocketClientTransport;
}

export interface CrdtWebSocketServerOptions {
  port?: number;
  host?: string;
  path?: string;
  server?: unknown;
  perMessageDeflate?: boolean;
  maxPayload?: number;
}

export interface CrdtWebSocketServer {
  readonly ready: Promise<void>;
  address(): unknown;
  close(): Promise<void>;
  getDocumentIds(): string[];
  getPeerIds(documentId?: string): string[];
}

export type {
  CrdtSyncEndpoint,
  CrdtSyncMessageReceiver
};

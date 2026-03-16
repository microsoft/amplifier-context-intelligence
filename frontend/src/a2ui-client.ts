import { isA2UIMessage, isBridgeMessage } from './types/a2ui.js';
import type { A2UIServerMessage, BridgeMessage, UserActionPayload } from './types/a2ui.js';

// ── Connection state ──────────────────────────────────────────────────────────

export enum ConnectionState {
  DISCONNECTED = 'DISCONNECTED',
  CONNECTING = 'CONNECTING',
  CONNECTED = 'CONNECTED',
}

// ── Typed event map ───────────────────────────────────────────────────────────

export type EventMap = {
  connected: () => void;
  disconnected: () => void;
  'bridge-message': (message: BridgeMessage) => void;
  'a2ui-message': (message: A2UIServerMessage) => void;
  error: (event: Event) => void;
};

// ── Minimal WebSocket-like interface (allows injection of MockWebSocket) ──────

type WSLike = {
  onopen: ((event: Event) => void) | null;
  onmessage: ((event: MessageEvent) => void) | null;
  onclose: ((event: CloseEvent) => void) | null;
  onerror: ((event: Event) => void) | null;
  send(data: string): void;
  close(): void;
};

type WSFactory = (url: string) => WSLike;

// ── A2UIClient ────────────────────────────────────────────────────────────────

export class A2UIClient {
  private _state: ConnectionState = ConnectionState.DISCONNECTED;
  private _ws: WSLike | null = null;
  private readonly _url: string;
  private readonly _wsFactory: WSFactory;
  private readonly _listeners: Map<string, Set<(...args: unknown[]) => void>> = new Map();

  constructor(url: string, wsFactory?: WSFactory) {
    this._url = url;
    this._wsFactory = wsFactory ?? ((u) => new WebSocket(u));
  }

  get state(): ConnectionState {
    return this._state;
  }

  // ── Event emitter ───────────────────────────────────────────────────────────

  on<K extends keyof EventMap>(event: K, handler: EventMap[K]): void {
    if (!this._listeners.has(event)) {
      this._listeners.set(event, new Set());
    }
    this._listeners.get(event)!.add(handler as (...args: unknown[]) => void);
  }

  off<K extends keyof EventMap>(event: K, handler: EventMap[K]): void {
    this._listeners.get(event)?.delete(handler as (...args: unknown[]) => void);
  }

  emit<K extends keyof EventMap>(event: K, ...args: Parameters<EventMap[K]>): void {
    this._listeners.get(event)?.forEach((handler) => handler(...(args as unknown[])));
  }

  // ── Connection lifecycle ────────────────────────────────────────────────────

  connect(): void {
    if (this._state !== ConnectionState.DISCONNECTED) return;
    this._state = ConnectionState.CONNECTING;
    this._ws = this._wsFactory(this._url);

    this._ws.onopen = () => {
      this._state = ConnectionState.CONNECTED;
      this.emit('connected');
    };

    this._ws.onmessage = (event: MessageEvent) => {
      try {
        const data: unknown = JSON.parse(event.data as string);
        if (isA2UIMessage(data)) {
          this.emit('a2ui-message', data);
        } else if (isBridgeMessage(data)) {
          this.emit('bridge-message', data);
        }
      } catch {
        // Ignore malformed JSON from the server
      }
    };

    this._ws.onclose = () => {
      this._state = ConnectionState.DISCONNECTED;
      this.emit('disconnected');
    };

    this._ws.onerror = (event: Event) => {
      this.emit('error', event);
    };
  }

  disconnect(): void {
    this._ws?.close();
  }

  // ── Send methods ────────────────────────────────────────────────────────────

  /** Send a plain text message to the Intelligence Service. */
  sendMessage(text: string): void {
    this._send({ type: 'message', text });
  }

  /** Send a component action event. */
  sendAction(componentId: string, actionType: string, payload: Record<string, unknown>): void {
    this._send({ type: 'action', componentId, actionType, payload });
  }

  /** Request a new session from the bridge. */
  sendNewSession(): void {
    this._send({ type: 'new_session' });
  }

  /** Send an A2UI user-action message (wraps payload as {userAction: ...}). */
  sendUserAction(payload: UserActionPayload): void {
    this._send({ userAction: payload });
  }

  // ── Private helpers ─────────────────────────────────────────────────────────

  private _send(data: unknown): void {
    if (this._state !== ConnectionState.CONNECTED) return;
    this._ws!.send(JSON.stringify(data));
  }
}

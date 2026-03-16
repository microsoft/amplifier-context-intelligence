import { describe, it, expect, vi } from 'vitest';
import { MockWebSocket } from './setup.js';
import { A2UIClient, ConnectionState } from '../src/a2ui-client.js';
import type { UserActionPayload } from '../src/types/a2ui.js';

/**
 * Factory helper: creates an A2UIClient with an injected MockWebSocket via wsFactory,
 * then immediately calls connect() so the ws is captured.
 */
function createClient(url = 'ws://test') {
  let ws!: MockWebSocket;
  const client = new A2UIClient(url, (u: string) => {
    ws = new MockWebSocket(u);
    return ws;
  });
  client.connect();
  return { client, ws };
}

describe('A2UIClient', () => {
  it('starts in DISCONNECTED state', () => {
    const client = new A2UIClient('ws://test');
    expect(client.state).toBe(ConnectionState.DISCONNECTED);
  });

  it('transitions to CONNECTING then CONNECTED on connect', () => {
    const { client, ws } = createClient();
    expect(client.state).toBe(ConnectionState.CONNECTING);
    ws.simulateOpen();
    expect(client.state).toBe(ConnectionState.CONNECTED);
  });

  it('emits connected event on open', () => {
    const { client, ws } = createClient();
    const handler = vi.fn();
    client.on('connected', handler);
    ws.simulateOpen();
    expect(handler).toHaveBeenCalledOnce();
  });

  it('emits bridge-message for bridge envelope messages (type: session_created)', () => {
    const { client, ws } = createClient();
    ws.simulateOpen();
    const handler = vi.fn();
    client.on('bridge-message', handler);
    const msg = { type: 'session_created', sessionId: 'abc-123' };
    ws.simulateMessage(JSON.stringify(msg));
    expect(handler).toHaveBeenCalledWith(msg);
  });

  it('emits a2ui-message for A2UI protocol messages (beginRendering)', () => {
    const { client, ws } = createClient();
    ws.simulateOpen();
    const handler = vi.fn();
    client.on('a2ui-message', handler);
    const msg = {
      beginRendering: {
        surfaceId: 'surface-1',
        root: { id: 'root', component: {} },
      },
    };
    ws.simulateMessage(JSON.stringify(msg));
    expect(handler).toHaveBeenCalledWith(msg);
  });

  it('sends message type over WebSocket (JSON with type:message, text field)', () => {
    const { client, ws } = createClient();
    ws.simulateOpen();
    client.sendMessage('hello world');
    expect(ws.sent).toHaveLength(1);
    expect(JSON.parse(ws.sent[0])).toEqual({ type: 'message', text: 'hello world' });
  });

  it('sends action type (type:action, componentId, actionType, payload)', () => {
    const { client, ws } = createClient();
    ws.simulateOpen();
    client.sendAction('btn-1', 'click', { value: 42 });
    expect(ws.sent).toHaveLength(1);
    expect(JSON.parse(ws.sent[0])).toEqual({
      type: 'action',
      componentId: 'btn-1',
      actionType: 'click',
      payload: { value: 42 },
    });
  });

  it('sends new_session type', () => {
    const { client, ws } = createClient();
    ws.simulateOpen();
    client.sendNewSession();
    expect(ws.sent).toHaveLength(1);
    expect(JSON.parse(ws.sent[0])).toEqual({ type: 'new_session' });
  });

  it('sends userAction A2UI message (wraps UserActionPayload in {userAction: ...})', () => {
    const { client, ws } = createClient();
    ws.simulateOpen();
    const payload: UserActionPayload = {
      name: 'buttonClick',
      surfaceId: 'surface-1',
      sourceComponentId: 'btn-1',
      timestamp: 1000,
      context: { key: 'value' },
    };
    client.sendUserAction(payload);
    expect(ws.sent).toHaveLength(1);
    expect(JSON.parse(ws.sent[0])).toEqual({ userAction: payload });
  });

  it('transitions to DISCONNECTED on close and emits disconnected', () => {
    const { client, ws } = createClient();
    ws.simulateOpen();
    const handler = vi.fn();
    client.on('disconnected', handler);
    ws.simulateClose();
    expect(client.state).toBe(ConnectionState.DISCONNECTED);
    expect(handler).toHaveBeenCalledOnce();
  });

  it('does not send when not in CONNECTED state', () => {
    const { client, ws } = createClient();
    // State is CONNECTING (not CONNECTED) — message should be dropped
    client.sendMessage('should not be sent');
    expect(ws.sent).toHaveLength(0);
  });

  it('disconnect() closes the underlying WebSocket', () => {
    const { client, ws } = createClient();
    client.disconnect();
    expect(ws.readyState).toBe(MockWebSocket.CLOSING);
  });

  it('connect() is idempotent - does not create a new WebSocket when already connecting', () => {
    let wsCount = 0;
    const client = new A2UIClient('ws://test', (u: string) => {
      wsCount++;
      return new MockWebSocket(u);
    });
    client.connect(); // first call → CONNECTING
    client.connect(); // second call → should be no-op
    expect(wsCount).toBe(1);
  });
});

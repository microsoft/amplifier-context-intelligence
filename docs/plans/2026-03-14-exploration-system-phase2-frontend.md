# Exploration System Phase 2: Frontend + A2UI Catalog

> **Execution:** Use the subagent-driven-development workflow to implement this plan.

**Goal:** Build the standalone A2UI frontend SPA that connects to the Intelligence Service via WebSocket, renders A2UI v0.8 messages using Lit web components, and displays 6 custom visualization components (NetworkGraph, TimeseriesChart, StatChart, DotDiagram, DataTable, MetricCard).

**Architecture:** The frontend is a Vite + TypeScript + Lit SPA served by nginx on port 3000. An `A2UIClient` class manages the WebSocket connection to the Intelligence Service (port 8100) with automatic reconnection. An `A2UIRenderer` Lit element processes incoming A2UI v0.8 protocol messages (`beginRendering`, `surfaceUpdate`, `dataModelUpdate`, `deleteSurface`), manages per-surface state and data models, resolves BoundValue paths via JSON Pointer, and instantiates catalog components from a registry. Six custom catalog components wrap visualization libraries (Cytoscape.js, Plotly.js, @hpcc-js/wasm Graphviz) or are Lit-native (DataTable, MetricCard). User interactions in components emit `ci-action` custom events that the client translates to `userAction` A2UI messages and sends back over WebSocket.

**Tech Stack:** Vite 6.x, TypeScript 5.x, Lit 3.x, Cytoscape.js, Plotly.js, @hpcc-js/wasm-graphviz, Vitest + happy-dom

---

## Working Directory

All file paths in this plan are relative to the `amplifier-context-intelligence` submodule:

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
```

Verify you are on the feature branch:

```bash
git branch --show-current
# Expected: feat/exploration-system
```

## Prerequisites

Phase 1 must be complete. Verify the placeholder frontend exists:

```bash
test -f frontend/index.html && echo "OK" || echo "MISSING"
test -f Dockerfile.frontend && echo "OK" || echo "MISSING"
```

Phase 2 replaces the Phase 1 placeholder `frontend/index.html` with a Vite SPA and replaces `Dockerfile.frontend` with a multi-stage build.

## A2UI v0.8 Protocol Note

The design document references `createSurface` and `updateComponents`, but those are **v0.9 names**. The actual A2UI v0.8 stable message types are:

| Design doc name | Actual v0.8 name | Direction |
|---|---|---|
| `createSurface` | `beginRendering` | Server → Client |
| `updateComponents` | `surfaceUpdate` | Server → Client |
| `updateDataModel` | `dataModelUpdate` | Server → Client |
| `deleteSurface` | `deleteSurface` | Server → Client |
| `action` | `userAction` | Client → Server |
| `error` | `error` | Client → Server |

This plan uses the correct v0.8 names throughout.

---

### Task 1: Project Scaffolding

**Files:**
- Create: `frontend/package.json`
- Create: `frontend/tsconfig.json`
- Create: `frontend/vite.config.ts`
- Replace: `frontend/index.html` (was Phase 1 placeholder)
- Create: `frontend/src/main.ts`
- Create: `frontend/test/setup.ts`

No TDD for boilerplate scaffolding.

**Step 1: Create `frontend/package.json`**

```json
{
  "name": "context-intelligence-explorer",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc --noEmit && vite build",
    "preview": "vite preview",
    "test": "vitest run",
    "test:watch": "vitest"
  },
  "dependencies": {
    "lit": "^3.2.0",
    "cytoscape": "^3.30.0",
    "plotly.js-dist-min": "^2.35.0",
    "@hpcc-js/wasm-graphviz": "^1.6.0"
  },
  "devDependencies": {
    "vite": "^6.0.0",
    "typescript": "^5.6.0",
    "vitest": "^2.1.0",
    "happy-dom": "^15.0.0",
    "@types/cytoscape": "^3.21.0"
  }
}
```

**Step 2: Create `frontend/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "lib": ["ES2022", "DOM", "DOM.Iterable"],
    "strict": true,
    "noEmit": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "useDefineForClassFields": false,
    "experimentalDecorators": true,
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true
  },
  "include": ["src/**/*.ts", "test/**/*.ts"],
  "exclude": ["node_modules", "dist"]
}
```

**Step 3: Create `frontend/vite.config.ts`**

```typescript
/// <reference types="vitest" />
import { defineConfig } from 'vite';

export default defineConfig({
  build: {
    target: 'es2022',
    outDir: 'dist',
  },
  server: {
    port: 3000,
    proxy: {
      '/ws': {
        target: 'ws://localhost:8100',
        ws: true,
      },
    },
  },
  test: {
    environment: 'happy-dom',
    setupFiles: ['./test/setup.ts'],
  },
});
```

**Step 4: Replace `frontend/index.html` with Vite SPA entry**

Replace the entire contents of `frontend/index.html`:

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Context Intelligence Explorer</title>
  <link rel="icon" href="https://avatars.githubusercontent.com/u/240397093" />
  <link rel="stylesheet" href="/src/theme/tokens.css" />
</head>
<body>
  <ci-app-shell></ci-app-shell>
  <script type="module" src="/src/main.ts"></script>
</body>
</html>
```

**Step 5: Create `frontend/src/main.ts`**

```typescript
/**
 * Entry point for the Context Intelligence Explorer SPA.
 * Imports all components to register custom elements, then wires
 * the A2UI client to the renderer and app shell.
 */

// Import catalog components (registers custom elements as side effect)
import './catalog/metric-card.js';
import './catalog/data-table.js';
import './catalog/dot-diagram.js';
import './catalog/stat-chart.js';
import './catalog/timeseries-chart.js';
import './catalog/network-graph.js';

// Import app components
import './a2ui-renderer.js';
import './session-controls.js';
import './app-shell.js';
```

**Step 6: Create `frontend/test/setup.ts`**

```typescript
/**
 * Vitest setup file for happy-dom environment.
 * Provides WebSocket mock and DOM cleanup.
 */

import { afterEach } from 'vitest';

// Clean up DOM after each test
afterEach(() => {
  document.body.innerHTML = '';
});

/**
 * Mock WebSocket for testing A2UI client.
 * Tests create instances directly — not installed globally.
 */
export class MockWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readyState = MockWebSocket.CONNECTING;
  url: string;
  onopen: ((ev: Event) => void) | null = null;
  onclose: ((ev: CloseEvent) => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;

  sent: string[] = [];

  constructor(url: string) {
    this.url = url;
  }

  /** Call from test to simulate connection opening. */
  simulateOpen(): void {
    this.readyState = MockWebSocket.OPEN;
    this.onopen?.(new Event('open'));
  }

  /** Call from test to simulate receiving a JSON message. */
  simulateMessage(data: unknown): void {
    const json = JSON.stringify(data);
    this.onmessage?.(new MessageEvent('message', { data: json }));
  }

  /** Call from test to simulate connection closing. */
  simulateClose(code = 1000, reason = ''): void {
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.(new CloseEvent('close', { code, reason }));
  }

  /** Call from test to simulate an error. */
  simulateError(): void {
    this.onerror?.(new Event('error'));
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(_code?: number, _reason?: string): void {
    this.readyState = MockWebSocket.CLOSED;
  }
}
```

**Step 7: Install dependencies**

Run:

```bash
cd frontend && npm install
```

Expected: `node_modules/` created, no errors.

**Step 8: Verify TypeScript compiles**

Run:

```bash
cd frontend && npx tsc --noEmit
```

Expected: No errors (empty project compiles cleanly).

**Step 9: Commit**

```bash
git add frontend/package.json frontend/tsconfig.json frontend/vite.config.ts frontend/index.html frontend/src/main.ts frontend/test/setup.ts && git commit -m "feat(frontend): scaffold Vite + TypeScript + Lit project"
```

Note: `frontend/node_modules/` should already be in `.gitignore`. If not, add it:

```bash
echo "frontend/node_modules/" >> .gitignore
echo "frontend/dist/" >> .gitignore
git add .gitignore && git commit --amend --no-edit
```

---

### Task 2: A2UI Type Definitions + Design Tokens

**Files:**
- Create: `frontend/src/types/a2ui.ts`
- Create: `frontend/src/types/plotly.d.ts`
- Create: `frontend/src/theme/tokens.css`

No TDD for type definitions and CSS.

**Step 1: Create `frontend/src/types/a2ui.ts`**

These are the TypeScript interfaces for A2UI v0.8 protocol messages plus bridge-specific messages from Phase 1.

```typescript
/**
 * A2UI v0.8 protocol type definitions.
 *
 * Server→Client: beginRendering, surfaceUpdate, dataModelUpdate, deleteSurface
 * Client→Server: userAction, error
 *
 * Plus bridge-specific envelope messages from the Intelligence Service.
 */

// ── Bound Values ──

/** A value that is either a literal or bound to a data model path. */
export interface BoundValue {
  literalString?: string;
  literalNumber?: number;
  literalBoolean?: boolean;
  /** JSON Pointer path into the surface data model. */
  path?: string;
}

// ── Server → Client Messages (A2UI v0.8) ──

export interface BeginRenderingPayload {
  surfaceId: string;
  root: string;
  catalogId?: string;
  styles?: Record<string, string>;
}

export interface ComponentNode {
  id: string;
  weight?: number;
  /**
   * Wrapper object with exactly one key: the component type name.
   * Value is the component's properties (may contain BoundValues).
   * Example: { "MetricCard": { "label": { "literalString": "Sessions" } } }
   */
  component: Record<string, Record<string, unknown>>;
}

export interface SurfaceUpdatePayload {
  surfaceId: string;
  components: ComponentNode[];
}

export interface DataEntry {
  key: string;
  valueString?: string;
  valueNumber?: number;
  valueBoolean?: boolean;
  valueMap?: DataEntry[];
  valueList?: DataEntry[];
}

export interface DataModelUpdatePayload {
  surfaceId: string;
  path?: string;
  contents: DataEntry[];
}

export interface DeleteSurfacePayload {
  surfaceId: string;
}

/** Union of all A2UI v0.8 server→client messages. */
export type A2UIServerMessage =
  | { beginRendering: BeginRenderingPayload }
  | { surfaceUpdate: SurfaceUpdatePayload }
  | { dataModelUpdate: DataModelUpdatePayload }
  | { deleteSurface: DeleteSurfacePayload };

// ── Client → Server Messages (A2UI v0.8) ──

export interface UserActionPayload {
  name: string;
  surfaceId: string;
  sourceComponentId: string;
  timestamp: string;
  context: Record<string, unknown>;
}

export interface A2UIErrorPayload {
  code?: string;
  surfaceId?: string;
  message: string;
}

export type A2UIClientMessage =
  | { userAction: UserActionPayload }
  | { error: A2UIErrorPayload };

// ── Bridge-Specific Messages (from Phase 1 WebSocket bridge) ──

export interface SessionCreatedMessage {
  type: 'session_created';
  session_id: string;
  message: string;
}

export interface BridgeResponseMessage {
  type: 'response';
  session_id: string;
  content: string;
}

export interface BridgeErrorMessage {
  type: 'error';
  session_id: string;
  message: string;
}

export interface ActionAckMessage {
  type: 'action_ack';
  session_id: string;
  component_id: string;
}

export type BridgeMessage =
  | SessionCreatedMessage
  | BridgeResponseMessage
  | BridgeErrorMessage
  | ActionAckMessage;

// ── Message Classification ──

/** Returns true if the parsed JSON is an A2UI protocol message. */
export function isA2UIMessage(msg: Record<string, unknown>): msg is Record<string, unknown> & A2UIServerMessage {
  return (
    'beginRendering' in msg ||
    'surfaceUpdate' in msg ||
    'dataModelUpdate' in msg ||
    'deleteSurface' in msg
  );
}

/** Returns true if the parsed JSON is a bridge envelope message. */
export function isBridgeMessage(msg: Record<string, unknown>): msg is BridgeMessage {
  return typeof msg.type === 'string';
}

// ── Surface State ──

export interface SurfaceState {
  surfaceId: string;
  catalogId: string;
  rootId: string;
  components: ComponentNode[];
  dataModel: Record<string, unknown>;
  styles: Record<string, string>;
}

// ── Resolved Component (after BoundValue resolution) ──

export interface ResolvedComponent {
  id: string;
  type: string;
  resolvedProps: Record<string, unknown>;
}
```

**Step 2: Create `frontend/src/types/plotly.d.ts`**

Plotly.js-dist-min has no bundled TypeScript declarations. Provide minimal types:

```typescript
/** Minimal type declarations for plotly.js-dist-min. */
declare module 'plotly.js-dist-min' {
  export function newPlot(
    root: HTMLElement,
    data: Array<Record<string, unknown>>,
    layout?: Record<string, unknown>,
    config?: Record<string, unknown>,
  ): Promise<HTMLElement>;

  export function react(
    root: HTMLElement,
    data: Array<Record<string, unknown>>,
    layout?: Record<string, unknown>,
    config?: Record<string, unknown>,
  ): Promise<HTMLElement>;

  export function purge(root: HTMLElement): void;

  export function relayout(
    root: HTMLElement,
    update: Record<string, unknown>,
  ): Promise<HTMLElement>;
}
```

**Step 3: Create `frontend/src/theme/tokens.css`**

Mirror the operational dashboard's OKLCH design tokens so both UIs feel visually consistent:

```css
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

/* ── Light mode ── */
:root {
  color-scheme: light;
  --font-sans: 'Outfit', system-ui, sans-serif;
  --font-mono: 'JetBrains Mono', monospace;
  --radius: 1.15rem;

  --background: oklch(0.984 0.006 95);
  --foreground: oklch(0.215 0.015 255);
  --card: oklch(0.996 0.004 95 / 88%);
  --card-foreground: oklch(0.215 0.015 255);
  --primary: oklch(0.59 0.12 161);
  --primary-foreground: oklch(0.988 0.004 95);
  --secondary: oklch(0.968 0.01 205);
  --secondary-foreground: oklch(0.29 0.02 255);
  --muted: oklch(0.95 0.008 220);
  --muted-foreground: oklch(0.53 0.018 255);
  --accent: oklch(0.95 0.014 165);
  --accent-foreground: oklch(0.29 0.04 165);
  --destructive: oklch(0.63 0.2 25);
  --destructive-foreground: oklch(0.988 0.004 95);
  --border: oklch(0.89 0.01 235 / 72%);
  --input: oklch(0.89 0.01 235 / 72%);
  --ring: oklch(0.68 0.08 161);

  --success: oklch(0.59 0.12 161);
  --warning: oklch(0.78 0.14 75);
  --error: oklch(0.63 0.2 25);
}

/* ── Dark mode (default for explorer) ── */
.dark, :root {
  color-scheme: dark;

  --background: oklch(0.145 0.005 255);
  --foreground: oklch(0.985 0.002 255);
  --card: oklch(0.18 0.007 255 / 88%);
  --card-foreground: oklch(0.985 0.002 255);
  --primary: oklch(0.696 0.17 162);
  --primary-foreground: oklch(0.145 0.005 255);
  --secondary: oklch(0.245 0.008 255);
  --secondary-foreground: oklch(0.985 0.002 255);
  --muted: oklch(0.245 0.008 255);
  --muted-foreground: oklch(0.62 0.013 255);
  --accent: oklch(0.27 0.028 165);
  --accent-foreground: oklch(0.92 0.01 150);
  --destructive: oklch(0.5 0.2 25);
  --destructive-foreground: oklch(0.985 0.002 255);
  --border: oklch(0.3 0.006 255);
  --input: oklch(0.3 0.006 255);
  --ring: oklch(0.696 0.17 162);

  --success: oklch(0.696 0.17 162);
  --warning: oklch(0.78 0.14 75);
  --error: oklch(0.65 0.22 25);
}

/* ── Reset ── */
*, *::before, *::after { box-sizing: border-box; }
html { min-height: 100%; font-family: var(--font-sans); }
body {
  margin: 0;
  min-height: 100dvh;
  background: var(--background);
  color: var(--foreground);
  font-feature-settings: "ss01" 1, "cv02" 1, "cv03" 1;
  text-rendering: optimizeLegibility;
}
```

**Step 4: Verify TypeScript compiles**

Run:

```bash
cd frontend && npx tsc --noEmit
```

Expected: No errors.

**Step 5: Commit**

```bash
git add frontend/src/types/ frontend/src/theme/ && git commit -m "feat(frontend): add A2UI v0.8 type definitions and design tokens"
```

---

### Task 3: A2UI Client

**Files:**
- Create: `frontend/src/a2ui-client.ts`
- Create: `frontend/test/a2ui-client.test.ts`

**Step 1: Write the failing tests**

Create `frontend/test/a2ui-client.test.ts`:

```typescript
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { MockWebSocket } from './setup.js';
import { A2UIClient, ConnectionState } from '../src/a2ui-client.js';

/** Helper: create client with mock WebSocket factory. */
function createClient(): { client: A2UIClient; ws: MockWebSocket } {
  let captured: MockWebSocket | null = null;
  const client = new A2UIClient('ws://test/ws', (url: string) => {
    captured = new MockWebSocket(url);
    return captured as unknown as WebSocket;
  });
  return { client, get ws() { return captured!; } };
}

describe('A2UIClient', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('starts in DISCONNECTED state', () => {
    const { client } = createClient();
    expect(client.state).toBe(ConnectionState.DISCONNECTED);
  });

  it('transitions to CONNECTING then CONNECTED on connect', () => {
    const { client, ws } = createClient();
    client.connect();
    expect(client.state).toBe(ConnectionState.CONNECTING);
    ws.simulateOpen();
    expect(client.state).toBe(ConnectionState.CONNECTED);
  });

  it('emits "connected" event on open', () => {
    const { client, ws } = createClient();
    const handler = vi.fn();
    client.on('connected', handler);
    client.connect();
    ws.simulateOpen();
    expect(handler).toHaveBeenCalledOnce();
  });

  it('emits "bridge-message" for bridge envelope messages', () => {
    const { client, ws } = createClient();
    const handler = vi.fn();
    client.on('bridge-message', handler);
    client.connect();
    ws.simulateOpen();
    ws.simulateMessage({ type: 'session_created', session_id: 's1', message: 'ok' });
    expect(handler).toHaveBeenCalledWith({
      type: 'session_created',
      session_id: 's1',
      message: 'ok',
    });
  });

  it('emits "a2ui-message" for A2UI protocol messages', () => {
    const { client, ws } = createClient();
    const handler = vi.fn();
    client.on('a2ui-message', handler);
    client.connect();
    ws.simulateOpen();
    const msg = { beginRendering: { surfaceId: 'sf1', root: 'r1' } };
    ws.simulateMessage(msg);
    expect(handler).toHaveBeenCalledWith(msg);
  });

  it('sends message type over WebSocket', () => {
    const { client, ws } = createClient();
    client.connect();
    ws.simulateOpen();
    client.sendMessage('show me all sessions');
    const sent = JSON.parse(ws.sent[0]);
    expect(sent).toEqual({ type: 'message', text: 'show me all sessions' });
  });

  it('sends action type over WebSocket', () => {
    const { client, ws } = createClient();
    client.connect();
    ws.simulateOpen();
    client.sendAction('graph-1', 'node-click', { nodeId: 'n42' });
    const sent = JSON.parse(ws.sent[0]);
    expect(sent).toEqual({
      type: 'action',
      componentId: 'graph-1',
      actionType: 'node-click',
      payload: { nodeId: 'n42' },
    });
  });

  it('sends new_session type over WebSocket', () => {
    const { client, ws } = createClient();
    client.connect();
    ws.simulateOpen();
    client.sendNewSession();
    const sent = JSON.parse(ws.sent[0]);
    expect(sent).toEqual({ type: 'new_session' });
  });

  it('sends userAction A2UI message over WebSocket', () => {
    const { client, ws } = createClient();
    client.connect();
    ws.simulateOpen();
    client.sendUserAction({
      name: 'node-click',
      surfaceId: 'sf1',
      sourceComponentId: 'graph-1',
      timestamp: '2026-01-01T00:00:00Z',
      context: { nodeId: 'n42' },
    });
    const sent = JSON.parse(ws.sent[0]);
    expect(sent).toEqual({
      userAction: {
        name: 'node-click',
        surfaceId: 'sf1',
        sourceComponentId: 'graph-1',
        timestamp: '2026-01-01T00:00:00Z',
        context: { nodeId: 'n42' },
      },
    });
  });

  it('transitions to DISCONNECTED on close', () => {
    const { client, ws } = createClient();
    const handler = vi.fn();
    client.on('disconnected', handler);
    client.connect();
    ws.simulateOpen();
    ws.simulateClose(1000, 'normal');
    expect(client.state).toBe(ConnectionState.DISCONNECTED);
    expect(handler).toHaveBeenCalledOnce();
  });

  it('does not send when not connected', () => {
    const { client } = createClient();
    // Should not throw, but should not send
    client.sendMessage('hello');
    // No ws.sent to check because ws is not created until connect()
  });

  it('disconnect closes the WebSocket', () => {
    const { client, ws } = createClient();
    client.connect();
    ws.simulateOpen();
    const closeSpy = vi.spyOn(ws, 'close');
    client.disconnect();
    expect(closeSpy).toHaveBeenCalled();
  });
});
```

**Step 2: Run test to verify it fails**

Run:

```bash
cd frontend && npx vitest run test/a2ui-client.test.ts
```

Expected: FAIL (cannot resolve `../src/a2ui-client.js`)

**Step 3: Write minimal implementation**

Create `frontend/src/a2ui-client.ts`:

```typescript
/**
 * A2UI WebSocket client for the Context Intelligence Explorer.
 *
 * Manages the WebSocket connection to the Intelligence Service,
 * classifies incoming messages as bridge-envelope or A2UI protocol,
 * and emits typed events for the renderer and app shell.
 */

import {
  isA2UIMessage,
  isBridgeMessage,
  type A2UIServerMessage,
  type BridgeMessage,
  type UserActionPayload,
} from './types/a2ui.js';

export enum ConnectionState {
  DISCONNECTED = 'disconnected',
  CONNECTING = 'connecting',
  CONNECTED = 'connected',
}

type EventMap = {
  'connected': () => void;
  'disconnected': () => void;
  'a2ui-message': (msg: A2UIServerMessage) => void;
  'bridge-message': (msg: BridgeMessage) => void;
  'error': (error: string) => void;
};

type WebSocketFactory = (url: string) => WebSocket;

export class A2UIClient {
  private ws: WebSocket | null = null;
  private _state: ConnectionState = ConnectionState.DISCONNECTED;
  private listeners = new Map<string, Set<Function>>();
  private wsFactory: WebSocketFactory;
  private url: string;

  constructor(url: string, wsFactory?: WebSocketFactory) {
    this.url = url;
    this.wsFactory = wsFactory ?? ((u: string) => new WebSocket(u));
  }

  get state(): ConnectionState {
    return this._state;
  }

  on<K extends keyof EventMap>(event: K, handler: EventMap[K]): void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(handler);
  }

  off<K extends keyof EventMap>(event: K, handler: EventMap[K]): void {
    this.listeners.get(event)?.delete(handler);
  }

  private emit<K extends keyof EventMap>(event: K, ...args: Parameters<EventMap[K]>): void {
    const handlers = this.listeners.get(event);
    if (handlers) {
      for (const handler of handlers) {
        (handler as Function)(...args);
      }
    }
  }

  connect(): void {
    if (this._state !== ConnectionState.DISCONNECTED) return;

    this._state = ConnectionState.CONNECTING;
    this.ws = this.wsFactory(this.url);

    this.ws.onopen = () => {
      this._state = ConnectionState.CONNECTED;
      this.emit('connected');
    };

    this.ws.onmessage = (event: MessageEvent) => {
      try {
        const data = JSON.parse(event.data);
        if (isA2UIMessage(data)) {
          this.emit('a2ui-message', data as A2UIServerMessage);
        } else if (isBridgeMessage(data)) {
          this.emit('bridge-message', data as BridgeMessage);
        }
      } catch {
        this.emit('error', 'Failed to parse WebSocket message');
      }
    };

    this.ws.onclose = () => {
      this._state = ConnectionState.DISCONNECTED;
      this.ws = null;
      this.emit('disconnected');
    };

    this.ws.onerror = () => {
      this.emit('error', 'WebSocket error');
    };
  }

  disconnect(): void {
    if (this.ws) {
      this.ws.close(1000, 'Client disconnect');
    }
  }

  /** Send a user question to the bridge. */
  sendMessage(text: string): void {
    this.send({ type: 'message', text });
  }

  /** Send a UI action to the bridge (Phase 1 bridge format). */
  sendAction(componentId: string, actionType: string, payload: Record<string, unknown>): void {
    this.send({ type: 'action', componentId, actionType, payload });
  }

  /** Send a new session request to the bridge. */
  sendNewSession(): void {
    this.send({ type: 'new_session' });
  }

  /** Send an A2UI userAction message to the bridge. */
  sendUserAction(payload: UserActionPayload): void {
    this.send({ userAction: payload });
  }

  private send(data: unknown): void {
    if (this.ws && this._state === ConnectionState.CONNECTED) {
      this.ws.send(JSON.stringify(data));
    }
  }
}
```

**Step 4: Run test to verify it passes**

Run:

```bash
cd frontend && npx vitest run test/a2ui-client.test.ts
```

Expected: 11 passed

**Step 5: Commit**

```bash
git add frontend/src/a2ui-client.ts frontend/test/a2ui-client.test.ts && git commit -m "feat(frontend): add A2UI WebSocket client with message classification"
```

---

### Task 4: A2UI Renderer

**Files:**
- Create: `frontend/src/a2ui-renderer.ts`
- Create: `frontend/test/a2ui-renderer.test.ts`

The renderer manages surface state, data models, resolves BoundValues, and creates catalog component elements.

**Step 1: Write the failing tests**

Create `frontend/test/a2ui-renderer.test.ts`:

```typescript
import { describe, it, expect, beforeEach } from 'vitest';
import {
  resolveJsonPointer,
  dataEntriesToObject,
  resolveBoundValue,
  parseComponentNode,
} from '../src/a2ui-renderer.js';

describe('resolveJsonPointer', () => {
  const model = {
    user: { name: 'Alice', scores: [10, 20, 30] },
    metrics: { totalSessions: 42 },
  };

  it('resolves root-level path', () => {
    expect(resolveJsonPointer(model, '/user')).toEqual({ name: 'Alice', scores: [10, 20, 30] });
  });

  it('resolves nested path', () => {
    expect(resolveJsonPointer(model, '/user/name')).toBe('Alice');
  });

  it('resolves numeric index in path', () => {
    expect(resolveJsonPointer(model, '/user/scores/1')).toBe(20);
  });

  it('returns undefined for missing path', () => {
    expect(resolveJsonPointer(model, '/nonexistent')).toBeUndefined();
  });

  it('returns entire model for empty path', () => {
    expect(resolveJsonPointer(model, '/')).toEqual(model);
  });
});

describe('dataEntriesToObject', () => {
  it('converts flat entries to object', () => {
    const entries = [
      { key: 'name', valueString: 'Alice' },
      { key: 'age', valueNumber: 30 },
      { key: 'active', valueBoolean: true },
    ];
    expect(dataEntriesToObject(entries)).toEqual({
      name: 'Alice',
      age: 30,
      active: true,
    });
  });

  it('converts nested valueMap entries', () => {
    const entries = [
      {
        key: 'address',
        valueMap: [
          { key: 'city', valueString: 'NYC' },
          { key: 'zip', valueString: '10001' },
        ],
      },
    ];
    expect(dataEntriesToObject(entries)).toEqual({
      address: { city: 'NYC', zip: '10001' },
    });
  });
});

describe('resolveBoundValue', () => {
  const model = { metrics: { total: 42 }, label: 'Sessions' };

  it('resolves literalString', () => {
    expect(resolveBoundValue({ literalString: 'hello' }, model)).toBe('hello');
  });

  it('resolves literalNumber', () => {
    expect(resolveBoundValue({ literalNumber: 99 }, model)).toBe(99);
  });

  it('resolves literalBoolean', () => {
    expect(resolveBoundValue({ literalBoolean: true }, model)).toBe(true);
  });

  it('resolves path against data model', () => {
    expect(resolveBoundValue({ path: '/metrics/total' }, model)).toBe(42);
  });

  it('prefers path over literal when both present', () => {
    expect(resolveBoundValue({ literalString: 'fallback', path: '/label' }, model)).toBe('Sessions');
  });

  it('returns non-BoundValue objects as-is', () => {
    const raw = { nodes: [{ id: 'n1' }] };
    expect(resolveBoundValue(raw, model)).toEqual(raw);
  });

  it('returns primitives as-is', () => {
    expect(resolveBoundValue('plain string', model)).toBe('plain string');
    expect(resolveBoundValue(42, model)).toBe(42);
  });
});

describe('parseComponentNode', () => {
  it('extracts type and properties from component wrapper', () => {
    const node = {
      id: 'card-1',
      component: {
        MetricCard: {
          label: { literalString: 'Sessions' },
          value: { path: '/metrics/total' },
        },
      },
    };
    const model = { metrics: { total: 42 } };
    const resolved = parseComponentNode(node, model);

    expect(resolved.id).toBe('card-1');
    expect(resolved.type).toBe('MetricCard');
    expect(resolved.resolvedProps.label).toBe('Sessions');
    expect(resolved.resolvedProps.value).toBe(42);
  });

  it('handles component with no bound values', () => {
    const node = {
      id: 'dot-1',
      component: { DotDiagram: { source: { literalString: 'digraph { a -> b }' } } },
    };
    const resolved = parseComponentNode(node, {});
    expect(resolved.type).toBe('DotDiagram');
    expect(resolved.resolvedProps.source).toBe('digraph { a -> b }');
  });
});
```

**Step 2: Run test to verify it fails**

Run:

```bash
cd frontend && npx vitest run test/a2ui-renderer.test.ts
```

Expected: FAIL (cannot resolve `../src/a2ui-renderer.js`)

**Step 3: Write minimal implementation**

Create `frontend/src/a2ui-renderer.ts`:

```typescript
/**
 * A2UI Renderer — processes A2UI v0.8 messages and renders catalog components.
 *
 * Manages per-surface state (component tree + data model), resolves BoundValues
 * via JSON Pointer, and instantiates Lit components from the catalog registry.
 */

import { LitElement, html, css, nothing, type TemplateResult } from 'lit';
import { customElement, state } from 'lit/decorators.js';
import type {
  A2UIServerMessage,
  BeginRenderingPayload,
  SurfaceUpdatePayload,
  DataModelUpdatePayload,
  ComponentNode,
  DataEntry,
  ResolvedComponent,
  SurfaceState,
} from './types/a2ui.js';
import { getCatalogFactory } from './catalog/index.js';

// ── Pure functions (exported for unit testing) ──

/**
 * Resolve a JSON Pointer path against an object.
 * RFC 6901: paths start with `/`, segments separated by `/`.
 */
export function resolveJsonPointer(obj: Record<string, unknown>, pointer: string): unknown {
  if (pointer === '/') return obj;
  if (!pointer.startsWith('/')) return undefined;

  const segments = pointer
    .slice(1)
    .split('/')
    .map((s) => s.replace(/~1/g, '/').replace(/~0/g, '~'));

  let current: unknown = obj;
  for (const segment of segments) {
    if (current === null || current === undefined) return undefined;
    if (Array.isArray(current)) {
      const index = Number(segment);
      if (Number.isNaN(index)) return undefined;
      current = current[index];
    } else if (typeof current === 'object') {
      current = (current as Record<string, unknown>)[segment];
    } else {
      return undefined;
    }
  }
  return current;
}

/** Convert A2UI DataEntry array into a plain JS object. */
export function dataEntriesToObject(entries: DataEntry[]): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const entry of entries) {
    if (entry.valueString !== undefined) {
      result[entry.key] = entry.valueString;
    } else if (entry.valueNumber !== undefined) {
      result[entry.key] = entry.valueNumber;
    } else if (entry.valueBoolean !== undefined) {
      result[entry.key] = entry.valueBoolean;
    } else if (entry.valueMap !== undefined) {
      result[entry.key] = dataEntriesToObject(entry.valueMap);
    } else if (entry.valueList !== undefined) {
      result[entry.key] = entry.valueList.map((e) => {
        if (e.valueString !== undefined) return e.valueString;
        if (e.valueNumber !== undefined) return e.valueNumber;
        if (e.valueBoolean !== undefined) return e.valueBoolean;
        if (e.valueMap !== undefined) return dataEntriesToObject(e.valueMap);
        return null;
      });
    }
  }
  return result;
}

/** Resolve a BoundValue (or pass-through non-BoundValue). */
export function resolveBoundValue(value: unknown, dataModel: Record<string, unknown>): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value !== 'object') return value;

  const bv = value as Record<string, unknown>;

  // Check if this looks like a BoundValue (has path or literal* keys)
  const hasBoundKeys =
    'path' in bv ||
    'literalString' in bv ||
    'literalNumber' in bv ||
    'literalBoolean' in bv;

  if (!hasBoundKeys) return value;

  // Path takes priority (A2UI spec: path binds to data model)
  if (typeof bv.path === 'string') {
    return resolveJsonPointer(dataModel, bv.path);
  }
  if (bv.literalString !== undefined) return bv.literalString;
  if (bv.literalNumber !== undefined) return bv.literalNumber;
  if (bv.literalBoolean !== undefined) return bv.literalBoolean;

  return value;
}

/**
 * Parse a ComponentNode: extract the type name, resolve all BoundValues.
 * The component wrapper has one key (the type name) mapping to properties.
 */
export function parseComponentNode(
  node: ComponentNode,
  dataModel: Record<string, unknown>,
): ResolvedComponent {
  const entries = Object.entries(node.component);
  const [type, rawProps] = entries[0] ?? ['Unknown', {}];

  const resolvedProps: Record<string, unknown> = {};
  for (const [key, val] of Object.entries(rawProps)) {
    resolvedProps[key] = resolveBoundValue(val, dataModel);
  }

  return { id: node.id, type, resolvedProps };
}

// ── Lit Element ──

@customElement('ci-a2ui-renderer')
export class A2UIRenderer extends LitElement {
  static override styles = css`
    :host { display: block; }
    .surface { margin-bottom: 1rem; }
    .surface-components {
      display: flex;
      flex-wrap: wrap;
      gap: 1rem;
    }
    .unknown-component {
      padding: 1rem;
      border: 1px dashed var(--border, #444);
      border-radius: 0.5rem;
      color: var(--muted-foreground, #888);
      font-size: 0.85rem;
    }
  `;

  @state() private surfaces = new Map<string, SurfaceState>();

  /** Process an incoming A2UI server message. */
  processMessage(msg: A2UIServerMessage): void {
    if ('beginRendering' in msg) {
      this.handleBeginRendering(msg.beginRendering);
    } else if ('surfaceUpdate' in msg) {
      this.handleSurfaceUpdate(msg.surfaceUpdate);
    } else if ('dataModelUpdate' in msg) {
      this.handleDataModelUpdate(msg.dataModelUpdate);
    } else if ('deleteSurface' in msg) {
      this.surfaces.delete(msg.deleteSurface.surfaceId);
    }
    this.requestUpdate();
  }

  private handleBeginRendering(payload: BeginRenderingPayload): void {
    this.surfaces.set(payload.surfaceId, {
      surfaceId: payload.surfaceId,
      catalogId: payload.catalogId ?? 'context-intelligence',
      rootId: payload.root,
      components: [],
      dataModel: {},
      styles: payload.styles ?? {},
    });
  }

  private handleSurfaceUpdate(payload: SurfaceUpdatePayload): void {
    const surface = this.surfaces.get(payload.surfaceId);
    if (!surface) return;
    surface.components = payload.components;
  }

  private handleDataModelUpdate(payload: DataModelUpdatePayload): void {
    const surface = this.surfaces.get(payload.surfaceId);
    if (!surface) return;

    const newData = dataEntriesToObject(payload.contents);

    if (payload.path) {
      // Merge at the specified path
      const segments = payload.path.split('/').filter(Boolean);
      let target: Record<string, unknown> = surface.dataModel;
      for (let i = 0; i < segments.length - 1; i++) {
        if (!(segments[i] in target) || typeof target[segments[i]] !== 'object') {
          target[segments[i]] = {};
        }
        target = target[segments[i]] as Record<string, unknown>;
      }
      const lastKey = segments[segments.length - 1];
      if (lastKey) {
        target[lastKey] = newData;
      } else {
        Object.assign(surface.dataModel, newData);
      }
    } else {
      Object.assign(surface.dataModel, newData);
    }
  }

  private handleAction(surfaceId: string, componentId: string, detail: Record<string, unknown>): void {
    this.dispatchEvent(
      new CustomEvent('a2ui-action', {
        bubbles: true,
        composed: true,
        detail: { surfaceId, componentId, ...detail },
      }),
    );
  }

  override render(): TemplateResult {
    if (this.surfaces.size === 0) {
      return html`<slot></slot>`;
    }

    return html`
      ${[...this.surfaces.values()].map((surface) => this.renderSurface(surface))}
    `;
  }

  private renderSurface(surface: SurfaceState): TemplateResult {
    const resolved = surface.components.map((node) =>
      parseComponentNode(node, surface.dataModel),
    );

    return html`
      <div class="surface" data-surface-id=${surface.surfaceId}>
        <div class="surface-components">
          ${resolved.map((comp) => this.renderCatalogComponent(surface.surfaceId, comp))}
        </div>
      </div>
    `;
  }

  private renderCatalogComponent(surfaceId: string, comp: ResolvedComponent): TemplateResult | typeof nothing {
    const factory = getCatalogFactory(comp.type);
    if (!factory) {
      return html`<div class="unknown-component">Unsupported component: ${comp.type}</div>`;
    }
    return factory(comp.id, comp.resolvedProps, (detail: Record<string, unknown>) => {
      this.handleAction(surfaceId, comp.id, detail);
    });
  }
}
```

**Step 4: Run test to verify it passes**

Run:

```bash
cd frontend && npx vitest run test/a2ui-renderer.test.ts
```

Expected: All tests pass (the test imports only the pure functions, not the Lit element).

Note: This will fail until the `catalog/index.ts` exists (imported by the Lit element). Create a temporary stub:

Create `frontend/src/catalog/index.ts`:

```typescript
/** Catalog component registry — stub until components are added. */
import type { TemplateResult } from 'lit';

type ActionHandler = (detail: Record<string, unknown>) => void;
type ComponentFactory = (id: string, props: Record<string, unknown>, onAction: ActionHandler) => TemplateResult;

const registry = new Map<string, ComponentFactory>();

export function registerCatalogComponent(name: string, factory: ComponentFactory): void {
  registry.set(name, factory);
}

export function getCatalogFactory(name: string): ComponentFactory | undefined {
  return registry.get(name);
}
```

Now re-run:

```bash
cd frontend && npx vitest run test/a2ui-renderer.test.ts
```

Expected: All tests pass.

**Step 5: Commit**

```bash
git add frontend/src/a2ui-renderer.ts frontend/src/catalog/index.ts frontend/test/a2ui-renderer.test.ts && git commit -m "feat(frontend): add A2UI renderer with BoundValue resolution and surface state"
```

---

### Task 5: MetricCard Component

**Files:**
- Create: `frontend/src/catalog/metric-card.ts`
- Create: `frontend/test/catalog/metric-card.test.ts`

**Step 1: Write the failing test**

Create `frontend/test/catalog/metric-card.test.ts`:

```typescript
import { describe, it, expect, afterEach } from 'vitest';
import '../src/catalog/metric-card.js';

afterEach(() => { document.body.innerHTML = ''; });

function createElement(props: Record<string, unknown>): HTMLElement & Record<string, unknown> {
  const el = document.createElement('ci-metric-card') as HTMLElement & Record<string, unknown>;
  Object.assign(el, props);
  document.body.appendChild(el);
  return el;
}

describe('ci-metric-card', () => {
  it('is defined as a custom element', () => {
    expect(customElements.get('ci-metric-card')).toBeDefined();
  });

  it('renders label and value in shadow DOM', async () => {
    const el = createElement({ label: 'Sessions', value: '42' });
    await (el as any).updateComplete;
    const shadow = el.shadowRoot!;
    expect(shadow.querySelector('.label')?.textContent).toContain('Sessions');
    expect(shadow.querySelector('.value')?.textContent).toContain('42');
  });

  it('renders unit when provided', async () => {
    const el = createElement({ label: 'Duration', value: '3.2', unit: 'sec' });
    await (el as any).updateComplete;
    expect(el.shadowRoot!.querySelector('.unit')?.textContent).toContain('sec');
  });

  it('renders trend indicator when provided', async () => {
    const el = createElement({ label: 'Errors', value: '5', trend: 'up' });
    await (el as any).updateComplete;
    const trend = el.shadowRoot!.querySelector('.trend');
    expect(trend).toBeTruthy();
    expect(trend!.classList.contains('trend-up')).toBe(true);
  });

  it('dispatches ci-action event on click', async () => {
    const el = createElement({ label: 'Click me', value: '0', componentId: 'mc1' });
    await (el as any).updateComplete;
    let fired = false;
    el.addEventListener('ci-action', () => { fired = true; });
    el.shadowRoot!.querySelector('.metric-card')?.dispatchEvent(new Event('click', { bubbles: true }));
    expect(fired).toBe(true);
  });
});
```

**Step 2: Run test to verify it fails**

Run:

```bash
cd frontend && npx vitest run test/catalog/metric-card.test.ts
```

Expected: FAIL

**Step 3: Write minimal implementation**

Create `frontend/src/catalog/metric-card.ts`:

```typescript
/**
 * MetricCard — A2UI custom catalog component.
 * Displays a single KPI metric with label, value, optional unit and trend.
 * Pure Lit — no external visualization library.
 */

import { LitElement, html, css } from 'lit';
import { customElement, property } from 'lit/decorators.js';

@customElement('ci-metric-card')
export class MetricCard extends LitElement {
  static override styles = css`
    :host { display: block; }
    .metric-card {
      background: var(--card, #1a1a2e);
      border: 1px solid var(--border, #333);
      border-radius: var(--radius, 1.15rem);
      padding: 1.25rem 1.5rem;
      cursor: pointer;
      transition: border-color 0.2s, box-shadow 0.2s;
      backdrop-filter: blur(16px);
      min-width: 160px;
    }
    .metric-card:hover {
      border-color: var(--primary, #4ade80);
      box-shadow: 0 0 0 1px var(--primary, #4ade80);
    }
    .label {
      font-size: 0.68rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.22em;
      color: var(--muted-foreground, #888);
      margin-bottom: 0.5rem;
    }
    .value {
      font-size: 2rem;
      font-weight: 700;
      letter-spacing: -0.03em;
      color: var(--foreground, #fff);
      line-height: 1;
    }
    .unit {
      font-size: 0.85rem;
      color: var(--muted-foreground, #888);
      margin-left: 0.25rem;
    }
    .trend {
      display: inline-flex;
      align-items: center;
      gap: 0.25rem;
      font-size: 0.75rem;
      font-weight: 600;
      margin-top: 0.5rem;
      padding: 0.15rem 0.5rem;
      border-radius: 999px;
    }
    .trend-up { color: var(--error, #f87171); background: oklch(0.25 0.06 25); }
    .trend-down { color: var(--success, #4ade80); background: oklch(0.2 0.04 160); }
    .trend-flat { color: var(--muted-foreground, #888); background: var(--muted, #2a2a3e); }
    .trend-label { margin-left: 0.25rem; }
  `;

  @property({ type: String }) componentId = '';
  @property({ type: String }) label = '';
  @property({ type: String }) value = '';
  @property({ type: String }) unit = '';
  @property({ type: String }) trend: 'up' | 'down' | 'flat' | '' = '';
  @property({ type: String }) trendLabel = '';

  override render() {
    const trendArrow = this.trend === 'up' ? '\u2191' : this.trend === 'down' ? '\u2193' : '\u2192';

    return html`
      <div class="metric-card" @click=${this.handleClick}>
        <div class="label">${this.label}</div>
        <div>
          <span class="value">${this.value}</span>
          ${this.unit ? html`<span class="unit">${this.unit}</span>` : ''}
        </div>
        ${this.trend
          ? html`
              <div class="trend trend-${this.trend}">
                ${trendArrow}
                ${this.trendLabel ? html`<span class="trend-label">${this.trendLabel}</span>` : ''}
              </div>
            `
          : ''}
      </div>
    `;
  }

  private handleClick(): void {
    this.dispatchEvent(
      new CustomEvent('ci-action', {
        bubbles: true,
        composed: true,
        detail: { name: 'metric-click', componentId: this.componentId },
      }),
    );
  }
}
```

**Step 4: Run test to verify it passes**

Run:

```bash
cd frontend && npx vitest run test/catalog/metric-card.test.ts
```

Expected: 5 passed

**Step 5: Commit**

```bash
git add frontend/src/catalog/metric-card.ts frontend/test/catalog/metric-card.test.ts && git commit -m "feat(frontend): add MetricCard catalog component"
```

---

### Task 6: DataTable Component

**Files:**
- Create: `frontend/src/catalog/data-table.ts`
- Create: `frontend/test/catalog/data-table.test.ts`

**Step 1: Write the failing test**

Create `frontend/test/catalog/data-table.test.ts`:

```typescript
import { describe, it, expect, afterEach } from 'vitest';
import '../src/catalog/data-table.js';

afterEach(() => { document.body.innerHTML = ''; });

function createElement(props: Record<string, unknown>): HTMLElement & Record<string, unknown> {
  const el = document.createElement('ci-data-table') as HTMLElement & Record<string, unknown>;
  Object.assign(el, props);
  document.body.appendChild(el);
  return el;
}

describe('ci-data-table', () => {
  it('is defined as a custom element', () => {
    expect(customElements.get('ci-data-table')).toBeDefined();
  });

  it('renders column headers', async () => {
    const el = createElement({
      columns: [
        { key: 'name', label: 'Name' },
        { key: 'status', label: 'Status' },
      ],
      rows: [],
    });
    await (el as any).updateComplete;
    const ths = el.shadowRoot!.querySelectorAll('th');
    expect(ths.length).toBe(2);
    expect(ths[0].textContent).toContain('Name');
    expect(ths[1].textContent).toContain('Status');
  });

  it('renders row data', async () => {
    const el = createElement({
      columns: [{ key: 'name', label: 'Name' }],
      rows: [{ name: 'Session A' }, { name: 'Session B' }],
    });
    await (el as any).updateComplete;
    const tds = el.shadowRoot!.querySelectorAll('td');
    expect(tds.length).toBe(2);
    expect(tds[0].textContent).toContain('Session A');
    expect(tds[1].textContent).toContain('Session B');
  });

  it('dispatches ci-action on row click', async () => {
    const el = createElement({
      columns: [{ key: 'id', label: 'ID' }],
      rows: [{ id: 'r1' }],
      componentId: 'tbl1',
    });
    await (el as any).updateComplete;

    let detail: Record<string, unknown> | null = null;
    el.addEventListener('ci-action', ((e: CustomEvent) => { detail = e.detail; }) as EventListener);

    const row = el.shadowRoot!.querySelector('tbody tr') as HTMLElement;
    row?.click();
    expect(detail).toBeTruthy();
    expect((detail as Record<string, unknown>).name).toBe('row-click');
  });

  it('sorts by column when header is clicked', async () => {
    const el = createElement({
      columns: [{ key: 'name', label: 'Name', sortable: true }],
      rows: [{ name: 'Bravo' }, { name: 'Alpha' }, { name: 'Charlie' }],
    });
    await (el as any).updateComplete;

    // Click header to sort ascending
    const th = el.shadowRoot!.querySelector('th') as HTMLElement;
    th?.click();
    await (el as any).updateComplete;

    const tds = el.shadowRoot!.querySelectorAll('td');
    expect(tds[0].textContent).toContain('Alpha');
    expect(tds[1].textContent).toContain('Bravo');
    expect(tds[2].textContent).toContain('Charlie');
  });
});
```

**Step 2: Run test to verify it fails**

Run:

```bash
cd frontend && npx vitest run test/catalog/data-table.test.ts
```

Expected: FAIL

**Step 3: Write minimal implementation**

Create `frontend/src/catalog/data-table.ts`:

```typescript
/**
 * DataTable — A2UI custom catalog component.
 * Sortable data table with row-click actions. Pure Lit, no external library.
 */

import { LitElement, html, css } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';

export interface TableColumn {
  key: string;
  label: string;
  sortable?: boolean;
}

@customElement('ci-data-table')
export class DataTable extends LitElement {
  static override styles = css`
    :host { display: block; overflow-x: auto; }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.85rem;
    }
    th {
      text-align: left;
      padding: 0.5rem 0.75rem;
      font-size: 0.68rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.15em;
      color: var(--muted-foreground, #888);
      border-bottom: 1px solid var(--border, #333);
      user-select: none;
    }
    th.sortable { cursor: pointer; }
    th.sortable:hover { color: var(--foreground, #fff); }
    .sort-indicator { margin-left: 0.25rem; font-size: 0.7rem; }
    td {
      padding: 0.6rem 0.75rem;
      border-bottom: 1px solid color-mix(in oklch, var(--border, #333) 50%, transparent);
      color: var(--card-foreground, #eee);
    }
    tr.clickable { cursor: pointer; }
    tr.clickable:hover td {
      background: color-mix(in oklch, var(--muted, #2a2a3e) 50%, transparent);
    }
    .empty {
      padding: 2rem;
      text-align: center;
      color: var(--muted-foreground, #888);
    }
  `;

  @property({ type: String }) componentId = '';
  @property({ type: Array }) columns: TableColumn[] = [];
  @property({ type: Array }) rows: Record<string, unknown>[] = [];
  @property({ type: Number }) pageSize = 50;

  @state() private sortKey = '';
  @state() private sortAsc = true;

  private get sortedRows(): Record<string, unknown>[] {
    if (!this.sortKey) return this.rows;
    const key = this.sortKey;
    const dir = this.sortAsc ? 1 : -1;
    return [...this.rows].sort((a, b) => {
      const av = String(a[key] ?? '');
      const bv = String(b[key] ?? '');
      return av.localeCompare(bv) * dir;
    });
  }

  override render() {
    if (!this.columns.length) {
      return html`<div class="empty">No data</div>`;
    }

    const rows = this.sortedRows.slice(0, this.pageSize);

    return html`
      <table>
        <thead>
          <tr>
            ${this.columns.map(
              (col) => html`
                <th
                  class=${col.sortable ? 'sortable' : ''}
                  @click=${col.sortable ? () => this.handleSort(col.key) : undefined}
                >
                  ${col.label}
                  ${this.sortKey === col.key
                    ? html`<span class="sort-indicator">${this.sortAsc ? '\u25B2' : '\u25BC'}</span>`
                    : ''}
                </th>
              `,
            )}
          </tr>
        </thead>
        <tbody>
          ${rows.map(
            (row, idx) => html`
              <tr class="clickable" @click=${() => this.handleRowClick(row, idx)}>
                ${this.columns.map((col) => html`<td>${String(row[col.key] ?? '')}</td>`)}
              </tr>
            `,
          )}
        </tbody>
      </table>
    `;
  }

  private handleSort(key: string): void {
    if (this.sortKey === key) {
      this.sortAsc = !this.sortAsc;
    } else {
      this.sortKey = key;
      this.sortAsc = true;
    }
  }

  private handleRowClick(row: Record<string, unknown>, index: number): void {
    this.dispatchEvent(
      new CustomEvent('ci-action', {
        bubbles: true,
        composed: true,
        detail: { name: 'row-click', componentId: this.componentId, row, index },
      }),
    );
  }
}
```

**Step 4: Run test to verify it passes**

Run:

```bash
cd frontend && npx vitest run test/catalog/data-table.test.ts
```

Expected: 5 passed

**Step 5: Commit**

```bash
git add frontend/src/catalog/data-table.ts frontend/test/catalog/data-table.test.ts && git commit -m "feat(frontend): add DataTable catalog component with sorting"
```

---

### Task 7: DotDiagram Component

**Files:**
- Create: `frontend/src/catalog/dot-diagram.ts`
- Create: `frontend/test/catalog/dot-diagram.test.ts`

Note: @hpcc-js/wasm-graphviz uses WASM which cannot load in happy-dom. Tests mock the Graphviz module.

**Step 1: Write the failing test**

Create `frontend/test/catalog/dot-diagram.test.ts`:

```typescript
import { describe, it, expect, afterEach, vi } from 'vitest';

// Mock @hpcc-js/wasm-graphviz before importing the component
vi.mock('@hpcc-js/wasm-graphviz', () => ({
  Graphviz: {
    load: vi.fn().mockResolvedValue({
      dot: vi.fn().mockReturnValue('<svg xmlns="http://www.w3.org/2000/svg"><text>mock</text></svg>'),
    }),
  },
}));

import '../src/catalog/dot-diagram.js';

afterEach(() => { document.body.innerHTML = ''; });

describe('ci-dot-diagram', () => {
  it('is defined as a custom element', () => {
    expect(customElements.get('ci-dot-diagram')).toBeDefined();
  });

  it('has a source property', () => {
    const el = document.createElement('ci-dot-diagram') as any;
    el.source = 'digraph { a -> b }';
    expect(el.source).toBe('digraph { a -> b }');
  });

  it('has an engine property defaulting to dot', () => {
    const el = document.createElement('ci-dot-diagram') as any;
    expect(el.engine).toBe('dot');
  });

  it('creates a container div in shadow DOM', async () => {
    const el = document.createElement('ci-dot-diagram') as any;
    el.source = 'digraph { a -> b }';
    document.body.appendChild(el);
    await el.updateComplete;
    const container = el.shadowRoot!.querySelector('.diagram-container');
    expect(container).toBeTruthy();
  });
});
```

**Step 2: Run test to verify it fails**

Run:

```bash
cd frontend && npx vitest run test/catalog/dot-diagram.test.ts
```

Expected: FAIL (cannot resolve `../src/catalog/dot-diagram.js`)

**Step 3: Write minimal implementation**

Create `frontend/src/catalog/dot-diagram.ts`:

```typescript
/**
 * DotDiagram — A2UI custom catalog component.
 * Renders Graphviz DOT source to SVG using @hpcc-js/wasm-graphviz.
 * The agent sends DOT source as a string; this component renders it client-side.
 */

import { LitElement, html, css } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';

// Dynamic import to avoid loading WASM until needed
type GraphvizInstance = { dot: (source: string, format?: string, engine?: string) => string };
let graphvizInstance: GraphvizInstance | null = null;

async function getGraphviz(): Promise<GraphvizInstance> {
  if (!graphvizInstance) {
    const { Graphviz } = await import('@hpcc-js/wasm-graphviz');
    graphvizInstance = await Graphviz.load();
  }
  return graphvizInstance;
}

@customElement('ci-dot-diagram')
export class DotDiagram extends LitElement {
  static override styles = css`
    :host { display: block; }
    .diagram-container {
      width: 100%;
      overflow: auto;
      background: var(--card, #1a1a2e);
      border: 1px solid var(--border, #333);
      border-radius: var(--radius, 1.15rem);
      padding: 1rem;
    }
    .diagram-container svg {
      max-width: 100%;
      height: auto;
    }
    .loading {
      padding: 2rem;
      text-align: center;
      color: var(--muted-foreground, #888);
    }
    .error {
      padding: 1rem;
      color: var(--error, #f87171);
      font-size: 0.85rem;
    }
  `;

  @property({ type: String }) componentId = '';
  @property({ type: String }) source = '';
  @property({ type: String }) engine = 'dot';

  @state() private svgContent = '';
  @state() private loading = false;
  @state() private errorMsg = '';

  override updated(changed: Map<string, unknown>): void {
    if (changed.has('source') || changed.has('engine')) {
      this.renderDot();
    }
  }

  private async renderDot(): Promise<void> {
    if (!this.source) {
      this.svgContent = '';
      return;
    }

    this.loading = true;
    this.errorMsg = '';

    try {
      const gv = await getGraphviz();
      this.svgContent = gv.dot(this.source, 'svg', this.engine);
    } catch (err) {
      this.errorMsg = `Graphviz error: ${err instanceof Error ? err.message : String(err)}`;
      this.svgContent = '';
    } finally {
      this.loading = false;
    }
  }

  override render() {
    if (this.loading) {
      return html`<div class="loading">Rendering diagram...</div>`;
    }
    if (this.errorMsg) {
      return html`<div class="error">${this.errorMsg}</div>`;
    }
    return html`
      <div class="diagram-container" .innerHTML=${this.svgContent}></div>
    `;
  }
}
```

**Step 4: Run test to verify it passes**

Run:

```bash
cd frontend && npx vitest run test/catalog/dot-diagram.test.ts
```

Expected: 4 passed

**Step 5: Commit**

```bash
git add frontend/src/catalog/dot-diagram.ts frontend/test/catalog/dot-diagram.test.ts && git commit -m "feat(frontend): add DotDiagram catalog component (Graphviz WASM)"
```

---

### Task 8: StatChart Component

**Files:**
- Create: `frontend/src/catalog/stat-chart.ts`
- Create: `frontend/test/catalog/stat-chart.test.ts`

Note: Plotly.js cannot render in happy-dom. Tests verify property binding and container creation.

**Step 1: Write the failing test**

Create `frontend/test/catalog/stat-chart.test.ts`:

```typescript
import { describe, it, expect, afterEach, vi } from 'vitest';

// Mock Plotly before importing the component
vi.mock('plotly.js-dist-min', () => ({
  newPlot: vi.fn().mockResolvedValue(undefined),
  react: vi.fn().mockResolvedValue(undefined),
  purge: vi.fn(),
}));

import '../src/catalog/stat-chart.js';

afterEach(() => { document.body.innerHTML = ''; });

describe('ci-stat-chart', () => {
  it('is defined as a custom element', () => {
    expect(customElements.get('ci-stat-chart')).toBeDefined();
  });

  it('accepts traces property', () => {
    const el = document.createElement('ci-stat-chart') as any;
    const traces = [{ type: 'bar', x: ['A', 'B'], y: [10, 20] }];
    el.traces = traces;
    expect(el.traces).toEqual(traces);
  });

  it('accepts layout property', () => {
    const el = document.createElement('ci-stat-chart') as any;
    el.layout = { title: 'Test' };
    expect(el.layout.title).toBe('Test');
  });

  it('creates a chart container in shadow DOM', async () => {
    const el = document.createElement('ci-stat-chart') as any;
    el.traces = [{ type: 'bar', x: ['A'], y: [10] }];
    document.body.appendChild(el);
    await el.updateComplete;
    const container = el.shadowRoot!.querySelector('.chart-container');
    expect(container).toBeTruthy();
  });
});
```

**Step 2: Run test to verify it fails**

Run:

```bash
cd frontend && npx vitest run test/catalog/stat-chart.test.ts
```

Expected: FAIL

**Step 3: Write minimal implementation**

Create `frontend/src/catalog/stat-chart.ts`:

```typescript
/**
 * StatChart — A2UI custom catalog component.
 * Renders bar charts, pie charts, and histograms using Plotly.js.
 */

import { LitElement, html, css } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';

@customElement('ci-stat-chart')
export class StatChart extends LitElement {
  static override styles = css`
    :host { display: block; }
    .chart-container {
      width: 100%;
      min-height: 300px;
      background: var(--card, #1a1a2e);
      border: 1px solid var(--border, #333);
      border-radius: var(--radius, 1.15rem);
      padding: 0.5rem;
    }
    .error {
      padding: 1rem;
      color: var(--error, #f87171);
      font-size: 0.85rem;
    }
  `;

  @property({ type: String }) componentId = '';
  @property({ type: Array }) traces: Array<Record<string, unknown>> = [];
  @property({ type: Object }) layout: Record<string, unknown> = {};

  @state() private errorMsg = '';
  private chartEl: HTMLDivElement | null = null;
  private plotlyLoaded = false;

  override render() {
    if (this.errorMsg) {
      return html`<div class="error">${this.errorMsg}</div>`;
    }
    return html`<div class="chart-container"></div>`;
  }

  override async updated(changed: Map<string, unknown>): Promise<void> {
    if (changed.has('traces') || changed.has('layout')) {
      await this.renderChart();
    }
  }

  private async renderChart(): Promise<void> {
    if (!this.traces.length) return;

    try {
      const Plotly = await import('plotly.js-dist-min');
      this.chartEl = this.shadowRoot!.querySelector('.chart-container') as HTMLDivElement;
      if (!this.chartEl) return;

      const darkLayout = {
        paper_bgcolor: 'transparent',
        plot_bgcolor: 'transparent',
        font: { color: '#e2e8f0', family: 'Outfit, system-ui, sans-serif' },
        margin: { t: 40, r: 20, b: 40, l: 50 },
        ...this.layout,
      };

      const config = { responsive: true, displayModeBar: false };

      if (this.plotlyLoaded) {
        await Plotly.react(this.chartEl, this.traces, darkLayout, config);
      } else {
        await Plotly.newPlot(this.chartEl, this.traces, darkLayout, config);
        this.plotlyLoaded = true;

        // Listen for Plotly click events
        (this.chartEl as any).on?.('plotly_click', (data: any) => {
          this.dispatchEvent(
            new CustomEvent('ci-action', {
              bubbles: true,
              composed: true,
              detail: {
                name: 'chart-click',
                componentId: this.componentId,
                point: data?.points?.[0],
              },
            }),
          );
        });
      }
    } catch (err) {
      this.errorMsg = `Chart error: ${err instanceof Error ? err.message : String(err)}`;
    }
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    if (this.chartEl) {
      import('plotly.js-dist-min').then((Plotly) => {
        if (this.chartEl) Plotly.purge(this.chartEl);
      }).catch(() => {});
    }
  }
}
```

**Step 4: Run test to verify it passes**

Run:

```bash
cd frontend && npx vitest run test/catalog/stat-chart.test.ts
```

Expected: 4 passed

**Step 5: Commit**

```bash
git add frontend/src/catalog/stat-chart.ts frontend/test/catalog/stat-chart.test.ts && git commit -m "feat(frontend): add StatChart catalog component (Plotly.js)"
```

---

### Task 9: TimeseriesChart Component

**Files:**
- Create: `frontend/src/catalog/timeseries-chart.ts`
- Create: `frontend/test/catalog/timeseries-chart.test.ts`

**Step 1: Write the failing test**

Create `frontend/test/catalog/timeseries-chart.test.ts`:

```typescript
import { describe, it, expect, afterEach, vi } from 'vitest';

vi.mock('plotly.js-dist-min', () => ({
  newPlot: vi.fn().mockResolvedValue(undefined),
  react: vi.fn().mockResolvedValue(undefined),
  purge: vi.fn(),
}));

import '../src/catalog/timeseries-chart.js';

afterEach(() => { document.body.innerHTML = ''; });

describe('ci-timeseries-chart', () => {
  it('is defined as a custom element', () => {
    expect(customElements.get('ci-timeseries-chart')).toBeDefined();
  });

  it('accepts traces with time-based x values', () => {
    const el = document.createElement('ci-timeseries-chart') as any;
    const traces = [{
      x: ['2026-01-01T00:00:00Z', '2026-01-01T01:00:00Z'],
      y: [10, 20],
      name: 'Events',
    }];
    el.traces = traces;
    expect(el.traces).toEqual(traces);
  });

  it('creates a chart container in shadow DOM', async () => {
    const el = document.createElement('ci-timeseries-chart') as any;
    el.traces = [{ x: ['2026-01-01'], y: [10] }];
    document.body.appendChild(el);
    await el.updateComplete;
    const container = el.shadowRoot!.querySelector('.chart-container');
    expect(container).toBeTruthy();
  });

  it('defaults xaxis type to date', () => {
    const el = document.createElement('ci-timeseries-chart') as any;
    expect(el.layout.xaxis?.type).toBe('date');
  });
});
```

**Step 2: Run test to verify it fails**

Run:

```bash
cd frontend && npx vitest run test/catalog/timeseries-chart.test.ts
```

Expected: FAIL

**Step 3: Write minimal implementation**

Create `frontend/src/catalog/timeseries-chart.ts`:

```typescript
/**
 * TimeseriesChart — A2UI custom catalog component.
 * Renders time-based line/scatter charts using Plotly.js with WebGL traces.
 */

import { LitElement, html, css } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';

@customElement('ci-timeseries-chart')
export class TimeseriesChart extends LitElement {
  static override styles = css`
    :host { display: block; }
    .chart-container {
      width: 100%;
      min-height: 350px;
      background: var(--card, #1a1a2e);
      border: 1px solid var(--border, #333);
      border-radius: var(--radius, 1.15rem);
      padding: 0.5rem;
    }
    .error {
      padding: 1rem;
      color: var(--error, #f87171);
      font-size: 0.85rem;
    }
  `;

  @property({ type: String }) componentId = '';
  @property({ type: Array }) traces: Array<Record<string, unknown>> = [];
  @property({ type: Object }) layout: Record<string, unknown> = {
    xaxis: { type: 'date' },
  };

  @state() private errorMsg = '';
  private chartEl: HTMLDivElement | null = null;
  private plotlyLoaded = false;

  override render() {
    if (this.errorMsg) {
      return html`<div class="error">${this.errorMsg}</div>`;
    }
    return html`<div class="chart-container"></div>`;
  }

  override async updated(changed: Map<string, unknown>): Promise<void> {
    if (changed.has('traces') || changed.has('layout')) {
      await this.renderChart();
    }
  }

  private async renderChart(): Promise<void> {
    if (!this.traces.length) return;

    try {
      const Plotly = await import('plotly.js-dist-min');
      this.chartEl = this.shadowRoot!.querySelector('.chart-container') as HTMLDivElement;
      if (!this.chartEl) return;

      // Add WebGL rendering for large datasets, default to scattergl
      const webglTraces = this.traces.map((trace) => {
        const t = { ...trace };
        if (!t.type || t.type === 'scatter') {
          t.type = 'scattergl';
        }
        if (!t.mode) {
          t.mode = 'lines+markers';
        }
        return t;
      });

      const mergedLayout = {
        paper_bgcolor: 'transparent',
        plot_bgcolor: 'transparent',
        font: { color: '#e2e8f0', family: 'Outfit, system-ui, sans-serif' },
        margin: { t: 40, r: 20, b: 50, l: 60 },
        xaxis: { type: 'date', gridcolor: '#333' },
        yaxis: { gridcolor: '#333' },
        ...this.layout,
      };

      const config = { responsive: true, displayModeBar: false };

      if (this.plotlyLoaded) {
        await Plotly.react(this.chartEl, webglTraces, mergedLayout, config);
      } else {
        await Plotly.newPlot(this.chartEl, webglTraces, mergedLayout, config);
        this.plotlyLoaded = true;

        (this.chartEl as any).on?.('plotly_click', (data: any) => {
          this.dispatchEvent(
            new CustomEvent('ci-action', {
              bubbles: true,
              composed: true,
              detail: {
                name: 'timeseries-click',
                componentId: this.componentId,
                point: data?.points?.[0],
              },
            }),
          );
        });
      }
    } catch (err) {
      this.errorMsg = `Chart error: ${err instanceof Error ? err.message : String(err)}`;
    }
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    if (this.chartEl) {
      import('plotly.js-dist-min').then((Plotly) => {
        if (this.chartEl) Plotly.purge(this.chartEl);
      }).catch(() => {});
    }
  }
}
```

**Step 4: Run test to verify it passes**

Run:

```bash
cd frontend && npx vitest run test/catalog/timeseries-chart.test.ts
```

Expected: 4 passed

**Step 5: Commit**

```bash
git add frontend/src/catalog/timeseries-chart.ts frontend/test/catalog/timeseries-chart.test.ts && git commit -m "feat(frontend): add TimeseriesChart catalog component (Plotly.js WebGL)"
```

---

### Task 10: NetworkGraph Component

**Files:**
- Create: `frontend/src/catalog/network-graph.ts`
- Create: `frontend/test/catalog/network-graph.test.ts`

This is the most complex component. Cytoscape.js cannot initialize in happy-dom, so tests verify property handling and event dispatch.

**Step 1: Write the failing test**

Create `frontend/test/catalog/network-graph.test.ts`:

```typescript
import { describe, it, expect, afterEach, vi } from 'vitest';

// Mock cytoscape before importing the component
vi.mock('cytoscape', () => {
  const mockCy = {
    on: vi.fn(),
    layout: vi.fn().mockReturnValue({ run: vi.fn() }),
    json: vi.fn().mockReturnValue({ elements: [] }),
    add: vi.fn(),
    remove: vi.fn(),
    destroy: vi.fn(),
    elements: vi.fn().mockReturnValue({ remove: vi.fn() }),
    resize: vi.fn(),
  };
  return { default: vi.fn().mockReturnValue(mockCy) };
});

import '../src/catalog/network-graph.js';

afterEach(() => { document.body.innerHTML = ''; });

describe('ci-network-graph', () => {
  it('is defined as a custom element', () => {
    expect(customElements.get('ci-network-graph')).toBeDefined();
  });

  it('accepts elements property', () => {
    const el = document.createElement('ci-network-graph') as any;
    const elements = {
      nodes: [{ data: { id: 'n1', label: 'Node 1' } }],
      edges: [{ data: { source: 'n1', target: 'n2' } }],
    };
    el.elements = elements;
    expect(el.elements).toEqual(elements);
  });

  it('accepts layout property', () => {
    const el = document.createElement('ci-network-graph') as any;
    el.layoutName = 'breadthfirst';
    expect(el.layoutName).toBe('breadthfirst');
  });

  it('defaults layout to cose', () => {
    const el = document.createElement('ci-network-graph') as any;
    expect(el.layoutName).toBe('cose');
  });

  it('creates a graph container in shadow DOM', async () => {
    const el = document.createElement('ci-network-graph') as any;
    document.body.appendChild(el);
    await el.updateComplete;
    const container = el.shadowRoot!.querySelector('.graph-container');
    expect(container).toBeTruthy();
  });

  it('dispatches ci-action on synthetic node click', () => {
    const el = document.createElement('ci-network-graph') as any;
    let detail: Record<string, unknown> | null = null;
    el.addEventListener('ci-action', ((e: CustomEvent) => { detail = e.detail; }) as EventListener);

    // Directly call the internal handler to test event dispatch
    el.emitNodeAction('n42', { type: 'Session', label: 'Session 42' });
    expect(detail).toBeTruthy();
    expect((detail as any).name).toBe('node-click');
    expect((detail as any).nodeId).toBe('n42');
  });
});
```

**Step 2: Run test to verify it fails**

Run:

```bash
cd frontend && npx vitest run test/catalog/network-graph.test.ts
```

Expected: FAIL

**Step 3: Write minimal implementation**

Create `frontend/src/catalog/network-graph.ts`:

```typescript
/**
 * NetworkGraph — A2UI custom catalog component.
 * Renders interactive network graphs using Cytoscape.js.
 *
 * Features:
 * - Configurable layout algorithms (cose, breadthfirst, grid, circle)
 * - Node click events → A2UI action messages
 * - Incremental updates (add/remove elements without full re-render)
 * - Dark theme styling consistent with operational dashboard
 */

import { LitElement, html, css } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';
import type cytoscape from 'cytoscape';

export interface GraphElements {
  nodes: Array<{ data: Record<string, unknown>; classes?: string }>;
  edges: Array<{ data: Record<string, unknown>; classes?: string }>;
}

@customElement('ci-network-graph')
export class NetworkGraph extends LitElement {
  static override styles = css`
    :host { display: block; }
    .graph-container {
      width: 100%;
      min-height: 400px;
      background: var(--card, #1a1a2e);
      border: 1px solid var(--border, #333);
      border-radius: var(--radius, 1.15rem);
      overflow: hidden;
    }
    .error {
      padding: 1rem;
      color: var(--error, #f87171);
      font-size: 0.85rem;
    }
  `;

  @property({ type: String }) componentId = '';
  @property({ type: Object }) elements: GraphElements = { nodes: [], edges: [] };
  @property({ type: String }) layoutName = 'cose';

  @state() private errorMsg = '';
  private cy: cytoscape.Core | null = null;

  override render() {
    if (this.errorMsg) {
      return html`<div class="error">${this.errorMsg}</div>`;
    }
    return html`<div class="graph-container"></div>`;
  }

  override async updated(changed: Map<string, unknown>): Promise<void> {
    if (changed.has('elements') || changed.has('layoutName')) {
      await this.renderGraph();
    }
  }

  private async renderGraph(): Promise<void> {
    if (!this.elements.nodes.length && !this.elements.edges.length) return;

    try {
      const cytoscapeModule = await import('cytoscape');
      const cytoscape = cytoscapeModule.default;

      const container = this.shadowRoot!.querySelector('.graph-container') as HTMLElement;
      if (!container) return;

      if (this.cy) {
        // Incremental update: replace elements, re-run layout
        this.cy.elements().remove();
        this.cy.add([
          ...this.elements.nodes.map((n) => ({ group: 'nodes' as const, data: n.data, classes: n.classes })),
          ...this.elements.edges.map((e) => ({ group: 'edges' as const, data: e.data, classes: e.classes })),
        ]);
        this.cy.layout({ name: this.layoutName, animate: true }).run();
      } else {
        this.cy = cytoscape({
          container,
          elements: [
            ...this.elements.nodes.map((n) => ({ group: 'nodes' as const, data: n.data, classes: n.classes })),
            ...this.elements.edges.map((e) => ({ group: 'edges' as const, data: e.data, classes: e.classes })),
          ],
          layout: { name: this.layoutName },
          style: [
            {
              selector: 'node',
              style: {
                'background-color': '#4ade80',
                'label': 'data(label)',
                'color': '#e2e8f0',
                'text-valign': 'bottom',
                'text-margin-y': 6,
                'font-size': '11px',
                'font-family': 'Outfit, system-ui, sans-serif',
                'width': 28,
                'height': 28,
              },
            },
            {
              selector: 'edge',
              style: {
                'width': 2,
                'line-color': '#555',
                'target-arrow-color': '#555',
                'target-arrow-shape': 'triangle',
                'curve-style': 'bezier',
              },
            },
            {
              selector: 'node:selected',
              style: { 'border-width': 3, 'border-color': '#60a5fa' },
            },
          ],
        });

        // Node click → A2UI action
        this.cy.on('tap', 'node', (evt) => {
          const node = evt.target;
          this.emitNodeAction(node.id(), node.data());
        });
      }
    } catch (err) {
      this.errorMsg = `Graph error: ${err instanceof Error ? err.message : String(err)}`;
    }
  }

  /** Emit a node-click action. Public for testing. */
  emitNodeAction(nodeId: string, nodeData: Record<string, unknown>): void {
    this.dispatchEvent(
      new CustomEvent('ci-action', {
        bubbles: true,
        composed: true,
        detail: {
          name: 'node-click',
          componentId: this.componentId,
          nodeId,
          nodeData,
        },
      }),
    );
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    this.cy?.destroy();
    this.cy = null;
  }
}
```

**Step 4: Run test to verify it passes**

Run:

```bash
cd frontend && npx vitest run test/catalog/network-graph.test.ts
```

Expected: 6 passed

**Step 5: Commit**

```bash
git add frontend/src/catalog/network-graph.ts frontend/test/catalog/network-graph.test.ts && git commit -m "feat(frontend): add NetworkGraph catalog component (Cytoscape.js)"
```

---

### Task 11: Catalog Registry + catalog.json

**Files:**
- Modify: `frontend/src/catalog/index.ts` (replace stub with full registry)
- Create: `frontend/catalog.json`
- Create: `frontend/test/catalog/registry.test.ts`

**Step 1: Write the failing test**

Create `frontend/test/catalog/registry.test.ts`:

```typescript
import { describe, it, expect, vi } from 'vitest';

// Mock all visualization libraries for component registration side effects
vi.mock('cytoscape', () => ({ default: vi.fn().mockReturnValue({ on: vi.fn(), layout: vi.fn().mockReturnValue({ run: vi.fn() }), destroy: vi.fn(), elements: vi.fn().mockReturnValue({ remove: vi.fn() }), add: vi.fn(), resize: vi.fn() }) }));
vi.mock('plotly.js-dist-min', () => ({ newPlot: vi.fn(), react: vi.fn(), purge: vi.fn() }));
vi.mock('@hpcc-js/wasm-graphviz', () => ({ Graphviz: { load: vi.fn().mockResolvedValue({ dot: vi.fn().mockReturnValue('') }) } }));

// Import components to register them
import '../../src/catalog/metric-card.js';
import '../../src/catalog/data-table.js';
import '../../src/catalog/dot-diagram.js';
import '../../src/catalog/stat-chart.js';
import '../../src/catalog/timeseries-chart.js';
import '../../src/catalog/network-graph.js';

// Import the registry after components are registered
import { getCatalogFactory, CATALOG_COMPONENTS } from '../../src/catalog/index.js';

describe('catalog registry', () => {
  it('has all 6 components registered', () => {
    expect(CATALOG_COMPONENTS).toHaveLength(6);
  });

  it('contains MetricCard', () => {
    expect(CATALOG_COMPONENTS).toContain('MetricCard');
    expect(getCatalogFactory('MetricCard')).toBeDefined();
  });

  it('contains DataTable', () => {
    expect(CATALOG_COMPONENTS).toContain('DataTable');
    expect(getCatalogFactory('DataTable')).toBeDefined();
  });

  it('contains DotDiagram', () => {
    expect(CATALOG_COMPONENTS).toContain('DotDiagram');
    expect(getCatalogFactory('DotDiagram')).toBeDefined();
  });

  it('contains StatChart', () => {
    expect(CATALOG_COMPONENTS).toContain('StatChart');
    expect(getCatalogFactory('StatChart')).toBeDefined();
  });

  it('contains TimeseriesChart', () => {
    expect(CATALOG_COMPONENTS).toContain('TimeseriesChart');
    expect(getCatalogFactory('TimeseriesChart')).toBeDefined();
  });

  it('contains NetworkGraph', () => {
    expect(CATALOG_COMPONENTS).toContain('NetworkGraph');
    expect(getCatalogFactory('NetworkGraph')).toBeDefined();
  });

  it('returns undefined for unknown component', () => {
    expect(getCatalogFactory('NonExistent')).toBeUndefined();
  });
});
```

**Step 2: Run test to verify it fails**

Run:

```bash
cd frontend && npx vitest run test/catalog/registry.test.ts
```

Expected: FAIL (`CATALOG_COMPONENTS` not exported from stub)

**Step 3: Replace `frontend/src/catalog/index.ts` with full registry**

Replace the entire contents of `frontend/src/catalog/index.ts`:

```typescript
/**
 * Catalog component registry — maps A2UI component type names to Lit template factories.
 *
 * The renderer calls getCatalogFactory(typeName) to get a factory function that
 * returns a Lit TemplateResult for the component with bound properties.
 */

import { html, type TemplateResult } from 'lit';

type ActionHandler = (detail: Record<string, unknown>) => void;
type ComponentFactory = (id: string, props: Record<string, unknown>, onAction: ActionHandler) => TemplateResult;

const registry = new Map<string, ComponentFactory>();

export function registerCatalogComponent(name: string, factory: ComponentFactory): void {
  registry.set(name, factory);
}

export function getCatalogFactory(name: string): ComponentFactory | undefined {
  return registry.get(name);
}

/** List of all registered component type names. */
export const CATALOG_COMPONENTS: string[] = [];

// ── Register all 6 catalog components ──

function register(name: string, factory: ComponentFactory): void {
  registerCatalogComponent(name, factory);
  CATALOG_COMPONENTS.push(name);
}

register('MetricCard', (id, props, onAction) => html`
  <ci-metric-card
    .componentId=${id}
    .label=${props.label ?? ''}
    .value=${String(props.value ?? '')}
    .unit=${props.unit ?? ''}
    .trend=${props.trend ?? ''}
    .trendLabel=${props.trendLabel ?? ''}
    @ci-action=${(e: Event) => onAction((e as CustomEvent).detail)}
  ></ci-metric-card>
`);

register('DataTable', (id, props, onAction) => html`
  <ci-data-table
    .componentId=${id}
    .columns=${props.columns ?? []}
    .rows=${props.rows ?? []}
    .pageSize=${props.pageSize ?? 50}
    @ci-action=${(e: Event) => onAction((e as CustomEvent).detail)}
  ></ci-data-table>
`);

register('DotDiagram', (id, props) => html`
  <ci-dot-diagram
    .componentId=${id}
    .source=${props.source ?? ''}
    .engine=${props.engine ?? 'dot'}
  ></ci-dot-diagram>
`);

register('StatChart', (id, props, onAction) => html`
  <ci-stat-chart
    .componentId=${id}
    .traces=${props.traces ?? []}
    .layout=${props.layout ?? {}}
    @ci-action=${(e: Event) => onAction((e as CustomEvent).detail)}
  ></ci-stat-chart>
`);

register('TimeseriesChart', (id, props, onAction) => html`
  <ci-timeseries-chart
    .componentId=${id}
    .traces=${props.traces ?? []}
    .layout=${props.layout ?? { xaxis: { type: 'date' } }}
    @ci-action=${(e: Event) => onAction((e as CustomEvent).detail)}
  ></ci-timeseries-chart>
`);

register('NetworkGraph', (id, props, onAction) => html`
  <ci-network-graph
    .componentId=${id}
    .elements=${props.elements ?? { nodes: [], edges: [] }}
    .layoutName=${props.layout ?? 'cose'}
    @ci-action=${(e: Event) => onAction((e as CustomEvent).detail)}
  ></ci-network-graph>
`);
```

**Step 4: Create `frontend/catalog.json`**

This is the A2UI custom catalog definition that the agent includes in its system prompt to know what components are available:

```json
{
  "catalogId": "context-intelligence",
  "components": {
    "MetricCard": {
      "type": "object",
      "description": "KPI metric card with label, value, optional unit and trend indicator.",
      "properties": {
        "label": { "type": "BoundValue", "description": "Metric name (e.g. 'Total Sessions')" },
        "value": { "type": "BoundValue", "description": "Metric value (string or number)" },
        "unit": { "type": "BoundValue", "description": "Optional unit label (e.g. 'sec', 'req/s')" },
        "trend": { "type": "BoundValue", "description": "Trend direction: 'up', 'down', or 'flat'" },
        "trendLabel": { "type": "BoundValue", "description": "Trend description (e.g. '+12% vs last hour')" }
      }
    },
    "DataTable": {
      "type": "object",
      "description": "Sortable data table with row-click actions.",
      "properties": {
        "columns": { "type": "BoundValue", "description": "Array of {key, label, sortable?} column definitions" },
        "rows": { "type": "BoundValue", "description": "Array of row objects keyed by column.key" },
        "pageSize": { "type": "BoundValue", "description": "Max rows to display (default 50)" }
      }
    },
    "DotDiagram": {
      "type": "object",
      "description": "Graphviz DOT diagram rendered to SVG. Agent sends DOT source, component renders client-side.",
      "properties": {
        "source": { "type": "BoundValue", "description": "DOT language source string" },
        "engine": { "type": "BoundValue", "description": "Graphviz engine: 'dot', 'neato', 'fdp', 'circo', 'twopi'" }
      }
    },
    "StatChart": {
      "type": "object",
      "description": "Statistical chart: bar, pie, or histogram via Plotly.js.",
      "properties": {
        "traces": { "type": "BoundValue", "description": "Array of Plotly trace objects ({type, x, y, values, labels, name})" },
        "layout": { "type": "BoundValue", "description": "Plotly layout object (title, axis labels, etc.)" }
      }
    },
    "TimeseriesChart": {
      "type": "object",
      "description": "Time-series line chart with WebGL rendering for large datasets.",
      "properties": {
        "traces": { "type": "BoundValue", "description": "Array of trace objects ({x: ISO timestamps[], y: number[], name})" },
        "layout": { "type": "BoundValue", "description": "Plotly layout (xaxis defaults to {type:'date'})" }
      }
    },
    "NetworkGraph": {
      "type": "object",
      "description": "Interactive network graph via Cytoscape.js. Supports node-click actions for drill-down.",
      "properties": {
        "elements": { "type": "BoundValue", "description": "{nodes: [{data: {id, label, ...}}], edges: [{data: {source, target, ...}}]}" },
        "layout": { "type": "BoundValue", "description": "Layout algorithm: 'cose', 'breadthfirst', 'grid', 'circle'" }
      }
    }
  },
  "styles": {}
}
```

**Step 5: Run test to verify it passes**

Run:

```bash
cd frontend && npx vitest run test/catalog/registry.test.ts
```

Expected: 8 passed

**Step 6: Commit**

```bash
git add frontend/src/catalog/index.ts frontend/catalog.json frontend/test/catalog/registry.test.ts && git commit -m "feat(frontend): add catalog registry with all 6 components + catalog.json"
```

---

### Task 12: Session Controls + App Shell

**Files:**
- Create: `frontend/src/session-controls.ts`
- Create: `frontend/src/app-shell.ts`

No TDD — these are layout/wiring components that are best verified visually. The A2UI client and renderer are already tested.

**Step 1: Create `frontend/src/session-controls.ts`**

```typescript
/**
 * Session controls — new session button and connection status indicator.
 */

import { LitElement, html, css } from 'lit';
import { customElement, property } from 'lit/decorators.js';
import { ConnectionState } from './a2ui-client.js';

@customElement('ci-session-controls')
export class SessionControls extends LitElement {
  static override styles = css`
    :host { display: flex; align-items: center; gap: 0.75rem; }
    .status {
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      font-size: 0.75rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      color: var(--muted-foreground, #888);
    }
    .status-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
    }
    .status-dot.connected { background: var(--success, #4ade80); }
    .status-dot.connecting { background: var(--warning, #fbbf24); animation: pulse 1s ease-in-out infinite; }
    .status-dot.disconnected { background: var(--error, #f87171); }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
    .new-session-btn {
      background: color-mix(in oklch, var(--primary, #4ade80) 12%, transparent);
      color: var(--primary, #4ade80);
      border: 1px solid color-mix(in oklch, var(--primary, #4ade80) 30%, transparent);
      border-radius: 0.6rem;
      padding: 0.4rem 0.85rem;
      cursor: pointer;
      font-family: var(--font-sans, system-ui);
      font-size: 0.8rem;
      font-weight: 600;
      transition: all 0.15s;
    }
    .new-session-btn:hover {
      background: color-mix(in oklch, var(--primary, #4ade80) 20%, transparent);
      border-color: var(--primary, #4ade80);
    }
    .new-session-btn:disabled { opacity: 0.5; cursor: not-allowed; }
  `;

  @property({ type: String }) connectionState: ConnectionState = ConnectionState.DISCONNECTED;
  @property({ type: String }) sessionId = '';

  override render() {
    const stateLabel = this.connectionState;
    const dotClass = this.connectionState === ConnectionState.CONNECTED
      ? 'connected'
      : this.connectionState === ConnectionState.CONNECTING
        ? 'connecting'
        : 'disconnected';

    return html`
      <div class="status">
        <span class="status-dot ${dotClass}"></span>
        ${stateLabel}
      </div>
      <button
        class="new-session-btn"
        ?disabled=${this.connectionState !== ConnectionState.CONNECTED}
        @click=${this.handleNewSession}
      >
        New Session
      </button>
    `;
  }

  private handleNewSession(): void {
    this.dispatchEvent(
      new CustomEvent('new-session', { bubbles: true, composed: true }),
    );
  }
}
```

**Step 2: Create `frontend/src/app-shell.ts`**

```typescript
/**
 * App shell — top-level layout for the Context Intelligence Explorer.
 * Contains header, query input, A2UI renderer surface, and session controls.
 */

import { LitElement, html, css } from 'lit';
import { customElement, state, query } from 'lit/decorators.js';
import { A2UIClient, ConnectionState } from './a2ui-client.js';
import type { A2UIServerMessage, BridgeMessage } from './types/a2ui.js';
import type { A2UIRenderer } from './a2ui-renderer.js';

@customElement('ci-app-shell')
export class AppShell extends LitElement {
  static override styles = css`
    :host {
      display: flex;
      flex-direction: column;
      min-height: 100dvh;
      max-width: 1400px;
      margin: 0 auto;
      padding: 1.5rem;
    }

    /* ── Header ── */
    .header {
      display: flex;
      align-items: center;
      gap: 0.75rem;
      margin-bottom: 1.5rem;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 0.6rem;
      margin-right: auto;
    }
    .brand img {
      width: 28px;
      height: 28px;
      border-radius: 50%;
    }
    .brand-name {
      font-size: 0.95rem;
      font-weight: 600;
      color: var(--foreground, #fff);
    }
    .brand-sub {
      font-size: 0.75rem;
      color: var(--muted-foreground, #888);
      margin-left: 0.5rem;
    }
    .dashboard-link {
      font-size: 0.8rem;
      color: var(--muted-foreground, #888);
      text-decoration: none;
      padding: 0.35rem 0.75rem;
      border-radius: 0.6rem;
      border: 1px solid transparent;
      transition: all 0.15s;
    }
    .dashboard-link:hover {
      color: var(--foreground, #fff);
      border-color: var(--border, #333);
      background: var(--muted, #2a2a3e);
    }

    /* ── Surface area ── */
    .surface-area {
      flex: 1;
      margin-bottom: 1rem;
    }
    .welcome {
      text-align: center;
      padding: 4rem 2rem;
      color: var(--muted-foreground, #888);
    }
    .welcome h2 {
      font-size: 1.25rem;
      color: var(--foreground, #fff);
      margin-bottom: 0.5rem;
    }
    .welcome p { font-size: 0.9rem; line-height: 1.6; }

    /* ── Input bar ── */
    .input-bar {
      display: flex;
      gap: 0.75rem;
      align-items: center;
      padding: 0.75rem;
      background: var(--card, #1a1a2e);
      border: 1px solid var(--border, #333);
      border-radius: var(--radius, 1.15rem);
      backdrop-filter: blur(16px);
    }
    .query-input {
      flex: 1;
      background: transparent;
      border: none;
      outline: none;
      color: var(--foreground, #fff);
      font-family: var(--font-sans, system-ui);
      font-size: 0.9rem;
      padding: 0.5rem;
    }
    .query-input::placeholder { color: var(--muted-foreground, #888); }
    .send-btn {
      background: var(--primary, #4ade80);
      color: var(--primary-foreground, #000);
      border: none;
      border-radius: 0.6rem;
      padding: 0.5rem 1.25rem;
      cursor: pointer;
      font-family: var(--font-sans, system-ui);
      font-size: 0.85rem;
      font-weight: 600;
      transition: opacity 0.15s;
    }
    .send-btn:hover { opacity: 0.85; }
    .send-btn:disabled { opacity: 0.4; cursor: not-allowed; }

    /* ── Footer controls ── */
    .footer {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-top: 0.75rem;
      padding-top: 0.75rem;
    }

    /* ── Response text (bridge stub) ── */
    .bridge-response {
      padding: 1rem 1.25rem;
      background: var(--card, #1a1a2e);
      border: 1px solid var(--border, #333);
      border-radius: var(--radius, 1.15rem);
      color: var(--card-foreground, #eee);
      font-size: 0.9rem;
      line-height: 1.6;
      margin-bottom: 1rem;
    }
  `;

  @state() private connectionState = ConnectionState.DISCONNECTED;
  @state() private sessionId = '';
  @state() private bridgeResponse = '';
  @state() private hasSurfaces = false;

  @query('ci-a2ui-renderer') private renderer!: A2UIRenderer;

  private client: A2UIClient;

  constructor() {
    super();
    // Determine WebSocket URL: use proxy in dev, direct in production
    const wsUrl = location.protocol === 'https:'
      ? `wss://${location.host}/ws`
      : `ws://${location.host}/ws`;
    this.client = new A2UIClient(wsUrl);
    this.setupClientHandlers();
  }

  private setupClientHandlers(): void {
    this.client.on('connected', () => {
      this.connectionState = ConnectionState.CONNECTED;
    });

    this.client.on('disconnected', () => {
      this.connectionState = ConnectionState.DISCONNECTED;
      this.sessionId = '';
    });

    this.client.on('bridge-message', (msg: BridgeMessage) => {
      if (msg.type === 'session_created') {
        this.sessionId = msg.session_id;
        this.bridgeResponse = '';
      } else if (msg.type === 'response') {
        this.bridgeResponse = msg.content;
      } else if (msg.type === 'error') {
        this.bridgeResponse = `Error: ${msg.message}`;
      }
    });

    this.client.on('a2ui-message', (msg: A2UIServerMessage) => {
      this.renderer?.processMessage(msg);
      this.hasSurfaces = true;
    });

    this.client.on('error', (err: string) => {
      console.error('[A2UIClient]', err);
    });
  }

  override connectedCallback(): void {
    super.connectedCallback();
    this.client.connect();
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    this.client.disconnect();
  }

  override render() {
    const isConnected = this.connectionState === ConnectionState.CONNECTED;

    return html`
      <!-- Header -->
      <nav class="header">
        <div class="brand">
          <img src="https://avatars.githubusercontent.com/u/240397093" alt="Amplifier" />
          <span class="brand-name">Context Intelligence</span>
          <span class="brand-sub">Explorer</span>
        </div>
        <a class="dashboard-link" href="http://localhost:8000" target="_blank">Dashboard</a>
        <a class="dashboard-link" href="http://localhost:8000/docs" target="_blank">API</a>
      </nav>

      <!-- A2UI Surface Area -->
      <div class="surface-area">
        ${this.bridgeResponse
          ? html`<div class="bridge-response">${this.bridgeResponse}</div>`
          : ''}

        <ci-a2ui-renderer
          @a2ui-action=${this.handleA2UIAction}
        >
          ${!this.hasSurfaces && !this.bridgeResponse
            ? html`
                <div class="welcome">
                  <h2>Ask a question about the telemetry graph</h2>
                  <p>
                    Try: "Show me all sessions from the last hour" or
                    "What are the most common tool call patterns?"
                  </p>
                </div>
              `
            : ''}
        </ci-a2ui-renderer>
      </div>

      <!-- Input bar -->
      <div class="input-bar">
        <input
          class="query-input"
          type="text"
          placeholder=${isConnected ? 'Ask a question...' : 'Connecting...'}
          ?disabled=${!isConnected}
          @keydown=${this.handleKeydown}
        />
        <button
          class="send-btn"
          ?disabled=${!isConnected}
          @click=${this.handleSend}
        >
          Send
        </button>
      </div>

      <!-- Footer controls -->
      <div class="footer">
        <ci-session-controls
          .connectionState=${this.connectionState}
          .sessionId=${this.sessionId}
          @new-session=${this.handleNewSession}
        ></ci-session-controls>
      </div>
    `;
  }

  private handleKeydown(e: KeyboardEvent): void {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      this.handleSend();
    }
  }

  private handleSend(): void {
    const input = this.shadowRoot!.querySelector('.query-input') as HTMLInputElement;
    const text = input?.value.trim();
    if (!text) return;

    this.client.sendMessage(text);
    this.bridgeResponse = '';
    input.value = '';
  }

  private handleNewSession(): void {
    this.client.sendNewSession();
    this.hasSurfaces = false;
    this.bridgeResponse = '';
  }

  private handleA2UIAction(e: CustomEvent): void {
    const detail = e.detail;
    this.client.sendAction(detail.componentId, detail.name, detail);
  }
}
```

**Step 3: Verify TypeScript compiles**

Run:

```bash
cd frontend && npx tsc --noEmit
```

Expected: No errors.

**Step 4: Commit**

```bash
git add frontend/src/session-controls.ts frontend/src/app-shell.ts && git commit -m "feat(frontend): add session controls and app shell"
```

---

### Task 13: Dockerfile.frontend Update

**Files:**
- Replace: `Dockerfile.frontend` (real multi-stage build)
- Create: `frontend/nginx.conf`

No TDD for Docker/nginx configuration.

**Step 1: Create `frontend/nginx.conf`**

This nginx config serves the Vite build output and proxies `/ws` to the Intelligence Service:

```nginx
server {
    listen 80;
    server_name _;
    root /usr/share/nginx/html;
    index index.html;

    # SPA fallback — all routes serve index.html
    location / {
        try_files $uri $uri/ /index.html;
    }

    # WebSocket proxy to Intelligence Service
    location /ws {
        proxy_pass http://intelligence-service:8100/ws;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 86400;
    }

    # Cache static assets
    location /assets {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}
```

**Step 2: Replace `Dockerfile.frontend` with multi-stage build**

Replace the entire contents of `Dockerfile.frontend`:

```dockerfile
# Stage 1: Build the frontend with Vite
FROM node:22-alpine AS build

WORKDIR /app

# Copy package files first for layer caching
COPY frontend/package.json frontend/package-lock.json* ./

RUN npm ci

# Copy source files
COPY frontend/ .

# Build for production
RUN npm run build

# Stage 2: Serve with nginx
FROM nginx:alpine

# Copy custom nginx config
COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf

# Copy built assets from build stage
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
```

**Step 3: Verify Dockerfile syntax**

Run:

```bash
head -3 Dockerfile.frontend
```

Expected: `# Stage 1: Build the frontend with Vite`

**Step 4: Commit**

```bash
git add Dockerfile.frontend frontend/nginx.conf && git commit -m "feat(docker): update Dockerfile.frontend with Vite build + nginx proxy"
```

---

### Task 14: Full Verification

**Files:** None (verification only)

**Step 1: Run the full frontend test suite**

Run:

```bash
cd frontend && npx vitest run
```

Expected: All tests pass. Approximate counts:
- a2ui-client: 11 tests
- a2ui-renderer: 14 tests
- metric-card: 5 tests
- data-table: 5 tests
- dot-diagram: 4 tests
- stat-chart: 4 tests
- timeseries-chart: 4 tests
- network-graph: 6 tests
- registry: 8 tests
- **Total: ~61 tests**

**Step 2: Verify TypeScript compiles cleanly**

Run:

```bash
cd frontend && npx tsc --noEmit
```

Expected: No errors.

**Step 3: Verify Vite builds successfully**

Run:

```bash
cd frontend && npx vite build
```

Expected: Build completes, `dist/` directory created with `index.html` and `assets/`.

**Step 4: Verify all expected files exist**

Run:

```bash
echo "=== Source ===" && \
ls frontend/src/*.ts && \
echo "=== Types ===" && \
ls frontend/src/types/*.ts && \
echo "=== Theme ===" && \
ls frontend/src/theme/*.css && \
echo "=== Catalog ===" && \
ls frontend/src/catalog/*.ts && \
echo "=== Tests ===" && \
ls frontend/test/*.ts frontend/test/catalog/*.ts && \
echo "=== Config ===" && \
ls frontend/package.json frontend/tsconfig.json frontend/vite.config.ts frontend/catalog.json frontend/nginx.conf && \
echo "=== Docker ===" && \
ls Dockerfile.frontend
```

Expected: All files listed without errors.

**Step 5: Verify Docker Compose config is still valid**

Run:

```bash
docker compose config --quiet 2>&1 && echo "Compose config valid" || echo "Compose config has errors"
```

Expected: `Compose config valid`

**Step 6: Run the existing Python test suite (no regressions)**

Run:

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence && pytest tests/ -v
```

Expected: All existing tests pass.

**Step 7: Verify git status is clean**

Run:

```bash
git status
```

Expected: `nothing to commit, working tree clean` (on branch `feat/exploration-system`)

---

## Summary

After completing all 14 tasks, Phase 2 delivers:

| Component | Files | Tests |
|-----------|-------|-------|
| Project scaffolding | `package.json`, `tsconfig.json`, `vite.config.ts`, `index.html`, `main.ts` | — |
| A2UI types + tokens | `types/a2ui.ts`, `types/plotly.d.ts`, `theme/tokens.css` | — |
| A2UI client | `a2ui-client.ts` | 11 tests |
| A2UI renderer | `a2ui-renderer.ts` | 14 tests |
| MetricCard | `catalog/metric-card.ts` | 5 tests |
| DataTable | `catalog/data-table.ts` | 5 tests |
| DotDiagram | `catalog/dot-diagram.ts` | 4 tests |
| StatChart | `catalog/stat-chart.ts` | 4 tests |
| TimeseriesChart | `catalog/timeseries-chart.ts` | 4 tests |
| NetworkGraph | `catalog/network-graph.ts` | 6 tests |
| Catalog registry | `catalog/index.ts`, `catalog.json` | 8 tests |
| Session controls | `session-controls.ts` | — |
| App shell | `app-shell.ts` | — |
| Dockerfile | `Dockerfile.frontend`, `nginx.conf` | — |

**Total: ~61 new tests, 20 new files, 2 replaced files, 14 commits on `feat/exploration-system`**

## Key Design Decisions

- **Custom A2UI renderer** — Built directly on Lit rather than depending on `@a2ui/lit` package. This gives full control over catalog component integration and avoids depending on a package that may have breaking changes.
- **Correct v0.8 message names** — Uses `beginRendering`/`surfaceUpdate` (v0.8) not `createSurface`/`updateComponents` (v0.9), matching the design's v0.8 target.
- **Dual message handling** — The client classifies incoming WebSocket messages as either bridge-envelope (have `type` field) or A2UI protocol (have `beginRendering`/`surfaceUpdate`/etc. as top-level keys), routing them to the appropriate handler.
- **Dynamic import for visualization libraries** — Cytoscape, Plotly, and Graphviz WASM are loaded via dynamic `import()` to avoid blocking initial page load.
- **BoundValue resolution** — The renderer resolves `path` (JSON Pointer) and `literalString`/`literalNumber`/`literalBoolean` values before passing resolved props to catalog components.
- **Dark-first design** — The explorer defaults to dark mode, matching the operational dashboard's aesthetic with OKLCH color tokens.
- **Vitest mocking for visualization libraries** — Cytoscape, Plotly, and Graphviz WASM are fully mocked in tests since they require real browser APIs.

## What Comes Next

- **Research phase:** Deep dive into the context-intelligence analyst agent, session-analyst patterns, and A2UI Python SDK to design the server bundle agents and tools
- **Phase 3+:** Server bundle implementation (`amplifier-bundle-context-intelligence-server`), self-improvement loop, DOT file architectural documentation, end-to-end integration testing

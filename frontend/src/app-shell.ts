import { LitElement, html, css, nothing } from 'lit';
import { customElement, state, query } from 'lit/decorators.js';
import { A2UIClient, ConnectionState } from './a2ui-client.js';
import { A2UIRenderer } from './a2ui-renderer.js';
import type { BridgeMessage, UserActionPayload } from './types/a2ui.js';

// Side-effect imports so the custom elements are registered
import './a2ui-renderer.js';
import './session-controls.js';

// ── AppShell ──────────────────────────────────────────────────────────────────
//
// Top-level application shell.  It:
//   • Creates and manages an A2UIClient whose WebSocket URL is derived from the
//     current page's protocol and host.
//   • Wires client events to local @state properties and the ci-a2ui-renderer.
//   • Renders the header, A2UI surface area, query input bar, and footer.

@customElement('ci-app-shell')
export class AppShell extends LitElement {
  static styles = css`
    :host {
      display: flex;
      flex-direction: column;
      min-height: 100vh;
      max-width: 1400px;
      margin: 0 auto;
      padding: 0 1rem;
      box-sizing: border-box;
      color: var(--foreground, oklch(0.93 0.010 250));
      background: var(--background, oklch(0.145 0.005 255));
      font-family: 'Outfit', system-ui, sans-serif;
    }

    /* ── Header ─────────────────────────────────────────────────────────── */

    .header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0.75rem 0;
      border-bottom: 1px solid var(--border, oklch(0.28 0.012 260));
      flex-shrink: 0;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 0.625rem;
      text-decoration: none;
      color: inherit;
    }

    .brand-logo {
      width: 28px;
      height: 28px;
      border-radius: 6px;
      background: var(--primary, oklch(0.696 0.17 162));
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 1rem;
      font-weight: 700;
      color: oklch(0.145 0.005 255);
      flex-shrink: 0;
    }

    .brand-text {
      display: flex;
      flex-direction: column;
      gap: 0;
    }

    .brand-name {
      font-size: 0.9rem;
      font-weight: 600;
      line-height: 1.2;
    }

    .brand-sub {
      font-size: 0.65rem;
      opacity: 0.55;
      line-height: 1.2;
    }

    .nav-links {
      display: flex;
      align-items: center;
      gap: 1.25rem;
    }

    .nav-links a {
      font-size: 0.82rem;
      text-decoration: none;
      color: var(--foreground, oklch(0.93 0.010 250));
      opacity: 0.65;
      transition: opacity 0.15s ease;
    }

    .nav-links a:hover {
      opacity: 1;
    }

    /* ── Surface area ────────────────────────────────────────────────────── */

    .surface-area {
      flex: 1;
      display: flex;
      flex-direction: column;
      overflow-y: auto;
      padding: 1rem 0;
      gap: 1rem;
    }

    .bridge-response {
      background: var(--card, oklch(0.17 0.015 260));
      border: 1px solid var(--border, oklch(0.28 0.012 260));
      border-radius: 8px;
      padding: 1rem 1.25rem;
      font-size: 0.85rem;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: 'JetBrains Mono', monospace;
    }

    ci-a2ui-renderer {
      display: contents;
    }

    .welcome {
      flex: 1;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 0.75rem;
      padding: 3rem 1rem;
      opacity: 0.45;
      text-align: center;
    }

    .welcome-icon {
      font-size: 2.5rem;
    }

    .welcome-title {
      font-size: 1rem;
      font-weight: 500;
    }

    .welcome-hint {
      font-size: 0.8rem;
    }

    /* ── Input bar ───────────────────────────────────────────────────────── */

    .input-bar {
      display: flex;
      align-items: center;
      gap: 0.625rem;
      padding: 0.75rem 1rem;
      background: rgba(255, 255, 255, 0.04);
      backdrop-filter: blur(12px);
      -webkit-backdrop-filter: blur(12px);
      border: 1px solid var(--border, oklch(0.28 0.012 260));
      border-radius: 10px;
      margin-bottom: 0.5rem;
      flex-shrink: 0;
    }

    .query-input {
      flex: 1;
      background: transparent;
      border: none;
      outline: none;
      color: var(--foreground, oklch(0.93 0.010 250));
      font-family: 'Outfit', system-ui, sans-serif;
      font-size: 0.9rem;
    }

    .query-input::placeholder {
      color: var(--foreground, oklch(0.93 0.010 250));
      opacity: 0.35;
    }

    .query-input:disabled {
      opacity: 0.4;
      cursor: not-allowed;
    }

    .send-btn {
      padding: 0.375rem 0.875rem;
      border-radius: 6px;
      border: none;
      background: var(--primary, oklch(0.696 0.17 162));
      color: oklch(0.145 0.005 255);
      font-family: 'Outfit', system-ui, sans-serif;
      font-size: 0.82rem;
      font-weight: 600;
      cursor: pointer;
      transition: opacity 0.15s ease;
      flex-shrink: 0;
    }

    .send-btn:hover:not(:disabled) {
      opacity: 0.85;
    }

    .send-btn:disabled {
      opacity: 0.4;
      cursor: not-allowed;
    }

    /* ── Footer ──────────────────────────────────────────────────────────── */

    .footer {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0.5rem 0 0.75rem;
      border-top: 1px solid var(--border, oklch(0.28 0.012 260));
      flex-shrink: 0;
    }

  `;

  // ── Reactive state ──────────────────────────────────────────────────────────

  @state() private connectionState: ConnectionState = ConnectionState.DISCONNECTED;
  @state() private sessionId: string | undefined;
  @state() private bridgeResponse: unknown = null;
  @state() private hasSurfaces: boolean = false;

  // ── DOM references ──────────────────────────────────────────────────────────

  @query('ci-a2ui-renderer') private renderer!: A2UIRenderer;
  @query('.query-input')     private queryInput!: HTMLInputElement;

  // ── A2UI client ─────────────────────────────────────────────────────────────

  private readonly client: A2UIClient;

  constructor() {
    super();
    const wsProtocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${location.host}/ws`;
    this.client = new A2UIClient(wsUrl);
    this.setupClientHandlers();
  }

  private setupClientHandlers(): void {
    // connected → update state
    this.client.on('connected', () => {
      this.connectionState = ConnectionState.CONNECTED;
    });

    // disconnected → clear state + sessionId
    this.client.on('disconnected', () => {
      this.connectionState = ConnectionState.DISCONNECTED;
      this.sessionId = undefined;
    });

    // bridge-message → handle session_created / response / error
    this.client.on('bridge-message', (message: BridgeMessage) => {
      if (message.type === 'sessionCreated') {
        this.sessionId = message.sessionId;
      } else if (message.type === 'response') {
        this.bridgeResponse = message.payload;
      } else if (message.type === 'error') {
        console.error('[Bridge error]', message.code, message.message);
      }
    });

    // a2ui-message → forward to renderer; track surface existence
    this.client.on('a2ui-message', (message) => {
      this.renderer?.processMessage(message);
      if ('beginRendering' in message) {
        // hasSurfaces stays true until explicit new-session; optimistic by design
        this.hasSurfaces = true;
      }
    });

    // error → log to console
    this.client.on('error', (e) => console.error(e));
  }

  // ── Lifecycle ───────────────────────────────────────────────────────────────

  override connectedCallback(): void {
    super.connectedCallback();
    this.client.connect();
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    this.client.disconnect();
  }

  // ── Event handlers ──────────────────────────────────────────────────────────

  private handleSend(): void {
    const text = this.queryInput?.value.trim() ?? '';
    if (!text || this.connectionState !== ConnectionState.CONNECTED) return;
    this.client.sendMessage(text);
    this.queryInput!.value = '';
  }

  private handleKeyDown(e: KeyboardEvent): void {
    if (e.key === 'Enter') this.handleSend();
  }

  private handleNewSession(): void {
    this.client.sendNewSession();
    this.sessionId = undefined;
    this.bridgeResponse = null;
    this.hasSurfaces = false;
  }

  private handleA2UIAction(e: CustomEvent<UserActionPayload>): void {
    const { sourceComponentId, name, context } = e.detail;
    this.client.sendAction(sourceComponentId, name, context);
  }

  // ── Render ──────────────────────────────────────────────────────────────────

  override render() {
    const isConnected = this.connectionState === ConnectionState.CONNECTED;

    return html`
      <nav class="header">
        <a class="brand" href="/">
          <div class="brand-logo">CI</div>
          <div class="brand-text">
            <span class="brand-name">Context Intelligence</span>
            <span class="brand-sub">Explorer</span>
          </div>
        </a>
        <div class="nav-links">
          <a href="/dashboard">Dashboard</a>
          <a href="/api">API</a>
        </div>
      </nav>

      <div class="surface-area">
        ${this.bridgeResponse
          ? html`<div class="bridge-response">${JSON.stringify(this.bridgeResponse, null, 2)}</div>`
          : nothing}

        <ci-a2ui-renderer
          @a2ui-action=${this.handleA2UIAction}
        ></ci-a2ui-renderer>

        ${!this.hasSurfaces && !this.bridgeResponse
          ? html`
              <div class="welcome">
                <span class="welcome-icon">🔭</span>
                <div class="welcome-title">Context Intelligence Explorer</div>
                <div class="welcome-hint">Connect and send a query to begin exploring.</div>
              </div>
            `
          : nothing}
      </div>

      <div class="input-bar">
        <input
          class="query-input"
          type="text"
          placeholder="Ask a question…"
          ?disabled=${!isConnected}
          @keydown=${this.handleKeyDown}
        />
        <button
          class="send-btn"
          ?disabled=${!isConnected}
          @click=${this.handleSend}
        >
          Send
        </button>
      </div>

      <div class="footer">
        <ci-session-controls
          .connectionState=${this.connectionState}
          @new-session=${this.handleNewSession}
        ></ci-session-controls>
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'ci-app-shell': AppShell;
  }
}

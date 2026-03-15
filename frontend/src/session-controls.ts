import { LitElement, html, css } from 'lit';
import { customElement, property } from 'lit/decorators.js';
import { ConnectionState } from './a2ui-client.js';

// ── SessionControls ───────────────────────────────────────────────────────────
//
// Displays the current WebSocket connection state (with a coloured status dot)
// and a "New Session" button. The button is disabled when not connected.
// Dispatches a `new-session` CustomEvent when the button is clicked.

@customElement('ci-session-controls')
export class SessionControls extends LitElement {
  static styles = css`
    :host {
      display: flex;
      align-items: center;
      gap: 1rem;
    }

    .status {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
    }

    .status-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      flex-shrink: 0;
    }

    .status-dot.connected {
      background: var(--success, oklch(0.65 0.18 145));
    }

    .status-dot.connecting {
      background: var(--warning, oklch(0.75 0.18 75));
      animation: pulse 1.5s ease-in-out infinite;
    }

    .status-dot.disconnected {
      background: var(--error, oklch(0.65 0.20 25));
    }

    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50%       { opacity: 0.3; }
    }

    .status-label {
      font-size: 0.8rem;
      color: var(--foreground, oklch(0.93 0.010 250));
      opacity: 0.7;
    }

    .new-session-btn {
      padding: 0.375rem 0.75rem;
      border-radius: 6px;
      border: 1px solid var(--primary, oklch(0.68 0.18 265));
      background: color-mix(in oklch, var(--primary, oklch(0.68 0.18 265)) 12%, transparent);
      color: var(--primary, oklch(0.68 0.18 265));
      font-size: 0.8rem;
      font-family: inherit;
      cursor: pointer;
      transition: background 0.15s ease;
    }

    .new-session-btn:hover:not(:disabled) {
      background: color-mix(in oklch, var(--primary, oklch(0.68 0.18 265)) 22%, transparent);
    }

    .new-session-btn:disabled {
      opacity: 0.4;
      cursor: not-allowed;
    }
  `;

  @property({ type: String }) connectionState: ConnectionState = ConnectionState.DISCONNECTED;

  private handleNewSession(): void {
    this.dispatchEvent(new CustomEvent('new-session', { bubbles: true, composed: true }));
  }

  private stateInfo(): { label: string; dotClass: string } {
    switch (this.connectionState) {
      case ConnectionState.CONNECTED:    return { label: 'Connected',    dotClass: 'connected' };
      case ConnectionState.CONNECTING:   return { label: 'Connecting…',  dotClass: 'connecting' };
      case ConnectionState.DISCONNECTED: return { label: 'Disconnected', dotClass: 'disconnected' };
    }
  }

  override render() {
    const { label, dotClass } = this.stateInfo();
    return html`
      <div class="status">
        <span class="status-dot ${dotClass}"></span>
        <span class="status-label">${label}</span>
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
}

declare global {
  interface HTMLElementTagNameMap {
    'ci-session-controls': SessionControls;
  }
}

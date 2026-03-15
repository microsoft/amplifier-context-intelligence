import { LitElement, html, css } from 'lit';
import { customElement, property } from 'lit/decorators.js';

// ── MetricCard ────────────────────────────────────────────────────────────────
//
// Displays a single KPI metric with label, value, optional unit, and optional
// trend indicator. Dispatches a `ci-action` CustomEvent on click.

@customElement('ci-metric-card')
export class MetricCard extends LitElement {
  static styles = css`
    :host {
      display: block;
    }

    .metric-card {
      background: rgba(255, 255, 255, 0.05);
      backdrop-filter: blur(12px);
      -webkit-backdrop-filter: blur(12px);
      border: 1px solid rgba(255, 255, 255, 0.1);
      border-radius: var(--radius, 12px);
      padding: 1.25rem 1.5rem;
      cursor: pointer;
      transition:
        border-color 0.2s ease,
        box-shadow 0.2s ease,
        transform 0.15s ease;
      user-select: none;
    }

    .metric-card:hover {
      border-color: var(--color-primary, #6366f1);
      box-shadow: 0 0 0 2px var(--color-primary, #6366f1),
        0 8px 32px color-mix(in srgb, var(--color-primary, #6366f1) 20%, transparent);
      transform: translateY(-1px);
    }

    .label {
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 0.7rem;
      font-weight: 600;
      color: var(--color-muted, #94a3b8);
      margin-bottom: 0.5rem;
    }

    .value-row {
      display: flex;
      align-items: baseline;
      gap: 0.375rem;
    }

    .value {
      font-size: 2rem;
      font-weight: 700;
      line-height: 1;
      color: var(--color-text, #f1f5f9);
    }

    .unit {
      font-size: 0.85rem;
      font-weight: 400;
      color: var(--color-muted, #94a3b8);
    }

    .trend {
      display: inline-flex;
      align-items: center;
      gap: 0.25rem;
      margin-top: 0.5rem;
      padding: 0.2rem 0.6rem;
      border-radius: 999px;
      font-size: 0.75rem;
      font-weight: 600;
    }

    .trend-up {
      background: rgba(239, 68, 68, 0.15);
      color: var(--color-error, #ef4444);
    }

    .trend-down {
      background: rgba(34, 197, 94, 0.15);
      color: var(--color-success, #22c55e);
    }

    .trend-flat {
      background: rgba(148, 163, 184, 0.15);
      color: var(--color-muted, #94a3b8);
    }
  `;

  @property({ type: String }) componentId = '';
  @property({ type: String }) label = '';
  @property() value: string | number = '';
  @property({ type: String }) unit = '';
  @property({ type: String }) trend: 'up' | 'down' | 'flat' | '' = '';
  @property({ type: String }) trendLabel = '';

  private handleClick(): void {
    this.dispatchEvent(
      new CustomEvent('ci-action', {
        bubbles: true,
        composed: true,
        detail: {
          name: 'metric-click',
          componentId: this.componentId,
        },
      }),
    );
  }

  private trendArrow(): string {
    if (this.trend === 'up') return '↑';
    if (this.trend === 'down') return '↓';
    if (this.trend === 'flat') return '→';
    return '';
  }

  render() {
    return html`
      <div class="metric-card" @click=${this.handleClick}>
        <div class="label">${this.label}</div>
        <div class="value-row">
          <span class="value">${this.value}</span>
          ${this.unit ? html`<span class="unit">${this.unit}</span>` : ''}
        </div>
        ${this.trend
          ? html`
              <div class="trend trend-${this.trend}">
                ${this.trendArrow()}${this.trendLabel ? html` ${this.trendLabel}` : ''}
              </div>
            `
          : ''}
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'ci-metric-card': MetricCard;
  }
}

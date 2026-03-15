import { LitElement, html, css } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';
import { createRef, ref } from 'lit/directives/ref.js';

// ── StatChart ─────────────────────────────────────────────────────────────────
//
// Renders bar charts, pie charts, and histograms using Plotly.js.
// Plotly is loaded lazily on first render via a dynamic import so the
// large bundle is only fetched when a chart is actually rendered.

// ── Types ─────────────────────────────────────────────────────────────────────

interface PlotlyStatic {
  newPlot(
    el: HTMLElement,
    traces: Array<Record<string, unknown>>,
    layout?: Record<string, unknown>,
    config?: Record<string, unknown>,
  ): Promise<PlotlyHTMLElement>;
  react(
    el: HTMLElement,
    traces: Array<Record<string, unknown>>,
    layout?: Record<string, unknown>,
    config?: Record<string, unknown>,
  ): Promise<PlotlyHTMLElement>;
  purge(el: HTMLElement): void;
}

type PlotlyHTMLElement = HTMLElement & {
  on?: (event: string, handler: (data: unknown) => void) => void;
};

// ── Component ─────────────────────────────────────────────────────────────────

@customElement('ci-stat-chart')
export class StatChart extends LitElement {
  static styles = css`
    :host {
      display: block;
    }

    .chart-container {
      min-height: 300px;
      background: rgba(255, 255, 255, 0.05);
      backdrop-filter: blur(12px);
      -webkit-backdrop-filter: blur(12px);
      border: 1px solid rgba(255, 255, 255, 0.1);
      border-radius: var(--radius, 12px);
      overflow: hidden;
    }

    .error {
      color: var(--color-error, #ef4444);
      font-size: 0.875rem;
      padding: 1rem;
      background: rgba(239, 68, 68, 0.1);
      border-radius: 4px;
      white-space: pre-wrap;
    }
  `;

  @property({ type: String }) componentId = '';
  @property({ type: Array }) traces: Array<Record<string, unknown>> = [];
  @property({ type: Object }) layout: Record<string, unknown> = {};

  @state() private errorMsg = '';

  private chartEl = createRef<HTMLDivElement>();
  private plotlyLoaded = false;

  override updated(changedProperties: Map<string | symbol, unknown>): void {
    if (changedProperties.has('traces') || changedProperties.has('layout')) {
      void this.renderChart();
    }
  }

  private async renderChart(): Promise<void> {
    if (!this.chartEl.value) return;

    this.errorMsg = '';

    try {
      const Plotly = (await import('plotly.js-dist-min')).default as PlotlyStatic;

      const darkLayout: Record<string, unknown> = {
        paper_bgcolor: 'transparent',
        plot_bgcolor: 'transparent',
        font: {
          color: '#e2e8f0',
          family: 'Outfit, sans-serif',
        },
        ...this.layout,
      };

      const config = {
        responsive: true,
        displayModeBar: false,
      };

      if (!this.plotlyLoaded) {
        const plotlyDiv = await Plotly.newPlot(
          this.chartEl.value,
          this.traces,
          darkLayout,
          config,
        ) as PlotlyHTMLElement;

        if (typeof plotlyDiv.on === 'function') {
          plotlyDiv.on('plotly_click', () => {
            this.dispatchEvent(
              new CustomEvent('ci-action', {
                bubbles: true,
                composed: true,
                detail: {
                  name: 'chart-click',
                  componentId: this.componentId,
                },
              }),
            );
          });
        }

        this.plotlyLoaded = true;
      } else {
        await Plotly.react(this.chartEl.value, this.traces, darkLayout, config);
      }
    } catch (err) {
      this.errorMsg = err instanceof Error ? err.message : String(err);
    }
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    if (this.chartEl.value && this.plotlyLoaded) {
      void import('plotly.js-dist-min').then(({ default: Plotly }) => {
        if (this.chartEl.value) {
          (Plotly as PlotlyStatic).purge(this.chartEl.value);
        }
      });
    }
  }

  render() {
    if (this.errorMsg) {
      return html`<div class="error">${this.errorMsg}</div>`;
    }
    return html`<div class="chart-container" ${ref(this.chartEl)}></div>`;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'ci-stat-chart': StatChart;
  }
}

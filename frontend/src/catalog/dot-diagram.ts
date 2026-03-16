import { LitElement, html, css, nothing } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';

// ── DotDiagram ─────────────────────────────────────────────────────────────────
//
// Renders a Graphviz DOT source string to SVG using @hpcc-js/wasm-graphviz.
// WASM is loaded lazily on first render via a module-level singleton so the
// (large) binary is fetched at most once per page load.

// ── Types ─────────────────────────────────────────────────────────────────────

interface GraphvizInstance {
  dot(source: string, format?: string, engine?: string): string;
}

// ── Lazy singleton ─────────────────────────────────────────────────────────────

let graphvizInstance: GraphvizInstance | null = null;

async function getGraphviz(): Promise<GraphvizInstance> {
  if (graphvizInstance) return graphvizInstance;
  const { Graphviz } = await import('@hpcc-js/wasm-graphviz');
  graphvizInstance = (await Graphviz.load()) as unknown as GraphvizInstance;
  return graphvizInstance;
}

// ── Component ──────────────────────────────────────────────────────────────────

@customElement('ci-dot-diagram')
export class DotDiagram extends LitElement {
  static styles = css`
    :host {
      display: block;
    }

    .diagram-container {
      overflow: auto;
    }

    .diagram-container svg {
      max-width: 100%;
      height: auto;
    }

    .loading {
      color: var(--color-muted, #94a3b8);
      font-size: 0.875rem;
      padding: 1rem;
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
  @property({ type: String }) source = '';
  @property({ type: String }) engine = 'dot';

  @state() private svgContent = '';
  @state() private loading = false;
  @state() private errorMsg = '';

  override updated(changedProperties: Map<string | symbol, unknown>): void {
    if (changedProperties.has('source') || changedProperties.has('engine')) {
      void this.renderDot();
    }
  }

  private async renderDot(): Promise<void> {
    if (!this.source) {
      this.svgContent = '';
      this.errorMsg = '';
      return;
    }

    this.loading = true;
    this.errorMsg = '';

    try {
      const gv = await getGraphviz();
      this.svgContent = gv.dot(this.source, 'svg', this.engine);
    } catch (err) {
      this.errorMsg = err instanceof Error ? err.message : String(err);
      this.svgContent = '';
    } finally {
      this.loading = false;
    }
  }

  render() {
    if (this.loading) {
      return html`<div class="loading">Rendering diagram…</div>`;
    }
    if (this.errorMsg) {
      return html`<div class="error">${this.errorMsg}</div>`;
    }
    if (this.svgContent) {
      return html`<div class="diagram-container" .innerHTML=${this.svgContent}></div>`;
    }
    return nothing;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'ci-dot-diagram': DotDiagram;
  }
}

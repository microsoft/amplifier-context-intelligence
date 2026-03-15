import { LitElement, html, css } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';
import { createRef, ref } from 'lit/directives/ref.js';

// ── NetworkGraph ──────────────────────────────────────────────────────────────
//
// Renders an interactive network graph using Cytoscape.js.
// Cytoscape is loaded lazily on first render via a dynamic import so the
// large bundle is only fetched when a graph is actually rendered.

// ── Types ─────────────────────────────────────────────────────────────────────

export interface GraphElements {
  nodes: Array<{ data: Record<string, unknown>; classes?: string }>;
  edges: Array<{ data: Record<string, unknown>; classes?: string }>;
}

// Minimal Cytoscape typings to avoid bundling @types/cytoscape at runtime.
type CytoscapeFactory = (options: Record<string, unknown>) => CytoscapeCore;

interface CytoscapeCore {
  on(
    event: string,
    selector: string,
    handler: (event: CytoscapeEvent) => void,
  ): void;
  layout(options: Record<string, unknown>): { run(): void };
  json(): Record<string, unknown>;
  add(elements: unknown[]): void;
  remove(selector: string): void;
  destroy(): void;
  elements(selector?: string): { remove(): void };
  resize(): void;
}

interface CytoscapeEvent {
  target: {
    id(): string;
    data(): Record<string, unknown>;
  };
}

// ── Component ─────────────────────────────────────────────────────────────────

@customElement('ci-network-graph')
export class NetworkGraph extends LitElement {
  static styles = css`
    :host {
      display: block;
    }

    .graph-container {
      min-height: 400px;
      width: 100%;
      background: rgba(0, 0, 0, 0.2);
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
  @property({ type: Object }) elements: GraphElements = { nodes: [], edges: [] };
  @property({ type: String }) layoutName = 'cose';

  @state() private errorMsg = '';

  private cy: CytoscapeCore | null = null;
  private containerRef = createRef<HTMLDivElement>();

  override updated(changedProperties: Map<string | symbol, unknown>): void {
    if (changedProperties.has('elements') || changedProperties.has('layoutName')) {
      void this.renderGraph();
    }
  }

  private async renderGraph(): Promise<void> {
    if (!this.containerRef.value) return;

    this.errorMsg = '';

    try {
      const cytoscapeModule = await import('cytoscape');
      const cytoscapeFactory = cytoscapeModule.default as unknown as CytoscapeFactory;

      const nodeElements = this.elements.nodes.map((n) => ({
        group: 'nodes' as const,
        data: n.data,
        ...(n.classes ? { classes: n.classes } : {}),
      }));

      const edgeElements = this.elements.edges.map((e) => ({
        group: 'edges' as const,
        data: e.data,
        ...(e.classes ? { classes: e.classes } : {}),
      }));

      if (!this.cy) {
        // First render: create the Cytoscape instance.
        this.cy = cytoscapeFactory({
          container: this.containerRef.value,
          elements: [...nodeElements, ...edgeElements],
          layout: { name: this.layoutName },
          style: [
            {
              selector: 'node',
              style: {
                'background-color': '#4ade80',
                label: 'data(label)',
                color: '#e2e8f0',
                'text-valign': 'center',
                'text-halign': 'center',
              },
            },
            {
              selector: 'node:selected',
              style: {
                'border-width': 3,
                'border-color': '#3b82f6',
              },
            },
            {
              selector: 'edge',
              style: {
                'line-color': '#555',
                'target-arrow-color': '#555',
                'target-arrow-shape': 'triangle',
                'curve-style': 'bezier',
              },
            },
          ],
        });

        this.cy.on('tap', 'node', (event: CytoscapeEvent) => {
          const nodeId = event.target.id();
          const nodeData = event.target.data();
          this.emitNodeAction(nodeId, nodeData);
        });
      } else {
        // Subsequent renders: update elements and re-run layout.
        this.cy.elements().remove();
        this.cy.add([...nodeElements, ...edgeElements]);
        this.cy.layout({ name: this.layoutName, animate: true }).run();
      }
    } catch (err) {
      this.errorMsg = err instanceof Error ? err.message : String(err);
    }
  }

  /** Public for testing — dispatches ci-action with node-click detail. */
  emitNodeAction(nodeId: string, nodeData: Record<string, unknown> = {}): void {
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
    if (this.cy) {
      this.cy.destroy();
      this.cy = null;
    }
  }

  render() {
    if (this.errorMsg) {
      return html`<div class="error">${this.errorMsg}</div>`;
    }
    return html`<div class="graph-container" ${ref(this.containerRef)}></div>`;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'ci-network-graph': NetworkGraph;
  }
}

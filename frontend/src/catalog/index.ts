// ── Catalog registry ─────────────────────────────────────────────────────────
//
// Provides a Map-backed registry for A2UI catalog components.
// Components register themselves via `registerCatalogComponent`; the renderer
// retrieves factory functions via `getCatalogFactory`.

import { html } from 'lit';

/** A factory function that produces a Lit TemplateResult or DOM node for a given set of resolved props. */
export type CatalogFactory = (props: Record<string, unknown>) => unknown;

const registry = new Map<string, CatalogFactory>();

/**
 * All catalog component type names, in registration order.
 * Populated by the internal `register()` helper below.
 */
export const CATALOG_COMPONENTS: readonly string[] = [];

/**
 * Register a component factory under a type name.
 * If a factory is already registered for that type, it is replaced.
 */
export function registerCatalogComponent(type: string, factory: CatalogFactory): void {
  registry.set(type, factory);
}

/**
 * Look up a factory by type name.
 * Returns `undefined` if no factory has been registered for that type.
 */
export function getCatalogFactory(type: string): CatalogFactory | undefined {
  return registry.get(type);
}

// ── Internal helper ───────────────────────────────────────────────────────────

/** Tracks a name in CATALOG_COMPONENTS and registers its factory. */
function register(name: string, factory: CatalogFactory): void {
  (CATALOG_COMPONENTS as string[]).push(name);
  registerCatalogComponent(name, factory);
}

// ── Factory registrations ─────────────────────────────────────────────────────

register('MetricCard', (props) => html`
  <ci-metric-card
    .componentId=${props['componentId'] as string ?? ''}
    .label=${props['label'] as string ?? ''}
    .value=${props['value'] ?? ''}
    .unit=${props['unit'] as string ?? ''}
    .trend=${props['trend'] as string ?? ''}
    .trendLabel=${props['trendLabel'] as string ?? ''}
    @ci-action=${props['onCiAction']}
  ></ci-metric-card>
`);

register('DataTable', (props) => html`
  <ci-data-table
    .componentId=${props['componentId'] as string ?? ''}
    .columns=${props['columns'] ?? []}
    .rows=${props['rows'] ?? []}
    .pageSize=${props['pageSize'] ?? 50}
    @ci-action=${props['onCiAction']}
  ></ci-data-table>
`);

register('DotDiagram', (props) => html`
  <ci-dot-diagram
    .componentId=${props['componentId'] as string ?? ''}
    .source=${props['source'] as string ?? ''}
    .engine=${props['engine'] as string ?? 'dot'}
  ></ci-dot-diagram>
`);

register('StatChart', (props) => html`
  <ci-stat-chart
    .componentId=${props['componentId'] as string ?? ''}
    .traces=${props['traces'] ?? []}
    .layout=${props['layout'] ?? {}}
    @ci-action=${props['onCiAction']}
  ></ci-stat-chart>
`);

register('TimeseriesChart', (props) => html`
  <ci-timeseries-chart
    .componentId=${props['componentId'] as string ?? ''}
    .traces=${props['traces'] ?? []}
    .layout=${props['layout'] ?? {}}
    @ci-action=${props['onCiAction']}
  ></ci-timeseries-chart>
`);

register('NetworkGraph', (props) => html`
  <ci-network-graph
    .componentId=${props['componentId'] as string ?? ''}
    .elements=${props['elements'] ?? { nodes: [], edges: [] }}
    .layoutName=${props['layout'] as string ?? 'cose'}
    @ci-action=${props['onCiAction']}
  ></ci-network-graph>
`);

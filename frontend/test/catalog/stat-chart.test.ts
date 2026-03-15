import { describe, it, expect, vi, afterEach } from 'vitest';

// ── Plotly Mock ───────────────────────────────────────────────────────────────
//
// vi.mock is hoisted before any imports, so the factory must not reference
// variables declared outside it (they wouldn't be defined yet at hoist time).

vi.mock('plotly.js-dist-min', () => ({
  default: {
    newPlot: vi.fn().mockResolvedValue({}),
    react: vi.fn().mockResolvedValue({}),
    purge: vi.fn(),
  },
}));

import '../../src/catalog/stat-chart.js';

// ── Helper ────────────────────────────────────────────────────────────────────

type StatChartElement = HTMLElement & {
  updateComplete?: Promise<boolean>;
  traces: Array<Record<string, unknown>>;
  layout: Record<string, unknown>;
};

async function createElement(
  props: {
    componentId?: string;
    traces?: Array<Record<string, unknown>>;
    layout?: Record<string, unknown>;
  } = {},
): Promise<StatChartElement> {
  const el = document.createElement('ci-stat-chart') as StatChartElement;
  Object.assign(el, props);
  document.body.appendChild(el);
  await (el as StatChartElement).updateComplete;
  return el;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('ci-stat-chart', () => {
  afterEach(() => {
    document.querySelectorAll('ci-stat-chart').forEach(el => el.remove());
  });

  it('is defined as a custom element', () => {
    const el = document.createElement('ci-stat-chart');
    expect(el).toBeInstanceOf(HTMLElement);
    expect(customElements.get('ci-stat-chart')).toBeDefined();
  });

  it('accepts traces property', async () => {
    const traces = [{ x: [1, 2, 3], y: [4, 5, 6], type: 'bar' }];
    const el = await createElement({ traces });
    expect(el.traces).toEqual(traces);
  });

  it('accepts layout property', async () => {
    const layout = { title: 'Test Chart', width: 400 };
    const el = await createElement({ layout });
    expect(el.layout).toEqual(layout);
  });

  it('creates .chart-container div in shadow DOM', async () => {
    const traces = [{ x: [1, 2, 3], y: [4, 5, 6], type: 'bar' }];
    const el = await createElement({ traces });
    const container = el.shadowRoot!.querySelector('.chart-container');
    expect(container).not.toBeNull();
  });
});

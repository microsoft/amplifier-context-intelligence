import { describe, it, expect, vi, afterEach } from 'vitest';

// ── Plotly Mock ────────────────────────────────────────────────────────────────
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

import '../../src/catalog/timeseries-chart.js';

// ── Helper ────────────────────────────────────────────────────────────────────

type TimeseriesChartElement = HTMLElement & {
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
): Promise<TimeseriesChartElement> {
  const el = document.createElement('ci-timeseries-chart') as TimeseriesChartElement;
  Object.assign(el, props);
  document.body.appendChild(el);
  await (el as TimeseriesChartElement).updateComplete;
  return el;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('ci-timeseries-chart', () => {
  afterEach(() => {
    document.querySelectorAll('ci-timeseries-chart').forEach(el => el.remove());
  });

  it('is defined as custom element', () => {
    const el = document.createElement('ci-timeseries-chart');
    expect(el).toBeInstanceOf(HTMLElement);
    expect(customElements.get('ci-timeseries-chart')).toBeDefined();
  });

  it('accepts traces with time-based x values (ISO timestamps)', async () => {
    const traces = [
      {
        x: ['2024-01-01T00:00:00Z', '2024-01-02T00:00:00Z', '2024-01-03T00:00:00Z'],
        y: [10, 20, 30],
        type: 'scatter',
      },
    ];
    const el = await createElement({ traces });
    expect(el.traces).toEqual(traces);
  });

  it('creates .chart-container in shadow DOM', async () => {
    const traces = [
      {
        x: ['2024-01-01T00:00:00Z', '2024-01-02T00:00:00Z'],
        y: [5, 15],
        type: 'scatter',
      },
    ];
    const el = await createElement({ traces });
    const container = el.shadowRoot!.querySelector('.chart-container');
    expect(container).not.toBeNull();
  });

  it('defaults xaxis type to date', async () => {
    const el = await createElement();
    expect((el.layout as Record<string, unknown> & { xaxis?: { type?: string } }).xaxis?.type).toBe('date');
  });
});

import { describe, it, expect, vi } from 'vitest';

// ── Mocks ────────────────────────────────────────────────────────────────────
//
// vi.mock is hoisted before any imports, so the factories must not reference
// variables declared outside them (they wouldn't be defined yet at hoist time).

vi.mock('cytoscape', () => {
  const mockLayout = { run: vi.fn() };
  const mockElements = { remove: vi.fn() };
  const mockCy = {
    on: vi.fn(),
    layout: vi.fn().mockReturnValue(mockLayout),
    json: vi.fn(),
    add: vi.fn(),
    remove: vi.fn(),
    destroy: vi.fn(),
    elements: vi.fn().mockReturnValue(mockElements),
    resize: vi.fn(),
  };
  return {
    default: vi.fn().mockReturnValue(mockCy),
  };
});

vi.mock('plotly.js-dist-min', () => ({
  default: {
    newPlot: vi.fn().mockResolvedValue({}),
    react: vi.fn().mockResolvedValue({}),
    purge: vi.fn(),
  },
}));

vi.mock('@hpcc-js/wasm-graphviz', () => ({
  Graphviz: {
    load: vi.fn().mockResolvedValue({
      dot: vi.fn().mockReturnValue('<svg xmlns="http://www.w3.org/2000/svg"><g>mock</g></svg>'),
    }),
  },
}));

// ── Component side-effect imports ─────────────────────────────────────────────
// These imports register the custom elements AND trigger catalog registration.

import '../../src/catalog/metric-card.js';
import '../../src/catalog/data-table.js';
import '../../src/catalog/dot-diagram.js';
import '../../src/catalog/stat-chart.js';
import '../../src/catalog/timeseries-chart.js';
import '../../src/catalog/network-graph.js';

// ── Registry imports ──────────────────────────────────────────────────────────

import { getCatalogFactory, CATALOG_COMPONENTS } from '../../src/catalog/index.js';

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('catalog registry', () => {
  it('has all 6 components registered', () => {
    expect(CATALOG_COMPONENTS.length).toBe(6);
  });

  it('contains MetricCard and returns a defined factory', () => {
    expect(CATALOG_COMPONENTS).toContain('MetricCard');
    expect(getCatalogFactory('MetricCard')).toBeDefined();
  });

  it('contains DataTable and returns a defined factory', () => {
    expect(CATALOG_COMPONENTS).toContain('DataTable');
    expect(getCatalogFactory('DataTable')).toBeDefined();
  });

  it('contains DotDiagram and returns a defined factory', () => {
    expect(CATALOG_COMPONENTS).toContain('DotDiagram');
    expect(getCatalogFactory('DotDiagram')).toBeDefined();
  });

  it('contains StatChart and returns a defined factory', () => {
    expect(CATALOG_COMPONENTS).toContain('StatChart');
    expect(getCatalogFactory('StatChart')).toBeDefined();
  });

  it('contains TimeseriesChart and returns a defined factory', () => {
    expect(CATALOG_COMPONENTS).toContain('TimeseriesChart');
    expect(getCatalogFactory('TimeseriesChart')).toBeDefined();
  });

  it('contains NetworkGraph and returns a defined factory', () => {
    expect(CATALOG_COMPONENTS).toContain('NetworkGraph');
    expect(getCatalogFactory('NetworkGraph')).toBeDefined();
  });

  it('returns undefined for unknown component', () => {
    expect(getCatalogFactory('UnknownComponent')).toBeUndefined();
  });
});

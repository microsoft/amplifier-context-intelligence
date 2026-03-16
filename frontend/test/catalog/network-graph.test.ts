import { describe, it, expect, vi, afterEach } from 'vitest';

// ── Cytoscape Mock ────────────────────────────────────────────────────────────
//
// vi.mock is hoisted before any imports, so the factory must not reference
// variables declared outside it (they wouldn't be defined yet at hoist time).
// We expose the mockCy object via the module mock so tests can inspect calls.

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
    __mockCy: mockCy,
  };
});

import '../../src/catalog/network-graph.js';

// ── Helper ────────────────────────────────────────────────────────────────────

type NetworkGraphElement = HTMLElement & {
  updateComplete?: Promise<boolean>;
  elements: { nodes: unknown[]; edges: unknown[] };
  layoutName: string;
  componentId: string;
  emitNodeAction: (nodeId: string, nodeData?: Record<string, unknown>) => void;
};

async function createElement(
  props: {
    componentId?: string;
    elements?: { nodes: unknown[]; edges: unknown[] };
    layoutName?: string;
  } = {},
): Promise<NetworkGraphElement> {
  const el = document.createElement('ci-network-graph') as NetworkGraphElement;
  Object.assign(el, props);
  document.body.appendChild(el);
  await (el as NetworkGraphElement).updateComplete;
  return el;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('ci-network-graph', () => {
  afterEach(() => {
    document.querySelectorAll('ci-network-graph').forEach(el => el.remove());
  });

  it('is defined as custom element', () => {
    const el = document.createElement('ci-network-graph');
    expect(el).toBeInstanceOf(HTMLElement);
    expect(customElements.get('ci-network-graph')).toBeDefined();
  });

  it('accepts elements property (GraphElements with nodes/edges)', async () => {
    const elements = {
      nodes: [{ data: { id: 'n1', label: 'Node 1' } }],
      edges: [{ data: { id: 'e1', source: 'n1', target: 'n1' } }],
    };
    const el = await createElement({ elements });
    expect(el.elements).toEqual(elements);
  });

  it('accepts layoutName property', async () => {
    const el = await createElement({ layoutName: 'circle' });
    expect(el.layoutName).toBe('circle');
  });

  it('defaults layoutName to cose', async () => {
    const el = await createElement();
    expect(el.layoutName).toBe('cose');
  });

  it('creates .graph-container in shadow DOM', async () => {
    const el = await createElement({
      elements: {
        nodes: [{ data: { id: 'n1', label: 'Node 1' } }],
        edges: [],
      },
    });
    const container = el.shadowRoot!.querySelector('.graph-container');
    expect(container).not.toBeNull();
  });

  it('dispatches ci-action on node click via emitNodeAction', async () => {
    const el = await createElement({
      componentId: 'graph-1',
      elements: {
        nodes: [{ data: { id: 'n1', label: 'Node 1' } }],
        edges: [],
      },
    });

    const received: CustomEvent[] = [];
    el.addEventListener('ci-action', (e) => received.push(e as CustomEvent));

    el.emitNodeAction('n1', { label: 'Node 1' });

    expect(received).toHaveLength(1);
    expect(received[0].detail.name).toBe('node-click');
    expect(received[0].detail.nodeId).toBe('n1');
  });
});

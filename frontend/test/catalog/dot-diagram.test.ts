import { describe, it, expect, vi, afterEach } from 'vitest';

// ── WASM Mock ─────────────────────────────────────────────────────────────────
//
// vi.mock is hoisted before any imports, so the factory must not reference
// variables declared outside it (they wouldn't be defined yet at hoist time).

vi.mock('@hpcc-js/wasm-graphviz', () => ({
  Graphviz: {
    load: vi.fn().mockResolvedValue({
      dot: vi.fn().mockReturnValue('<svg xmlns="http://www.w3.org/2000/svg"><g>mock</g></svg>'),
    }),
  },
}));

import '../../src/catalog/dot-diagram.js';

// ── Helper ────────────────────────────────────────────────────────────────────

type DotDiagramElement = HTMLElement & {
  updateComplete?: Promise<boolean>;
  source: string;
  engine: string;
};

async function createElement(
  props: { componentId?: string; source?: string; engine?: string } = {},
): Promise<DotDiagramElement> {
  const el = document.createElement('ci-dot-diagram') as DotDiagramElement;
  Object.assign(el, props);
  document.body.appendChild(el);
  await el.updateComplete;
  return el;
}

/** Flush microtasks and wait for the second Lit render triggered by renderDot(). */
async function waitForAsyncRender(el: DotDiagramElement): Promise<void> {
  // Allow the mocked Promise (Graphviz.load) to resolve
  await Promise.resolve();
  await Promise.resolve();
  // Then wait for Lit to re-render after svgContent state change
  await el.updateComplete;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('ci-dot-diagram', () => {
  afterEach(() => {
    document.querySelectorAll('ci-dot-diagram').forEach(el => el.remove());
  });

  it('is defined as a custom element', () => {
    const el = document.createElement('ci-dot-diagram');
    expect(el).toBeInstanceOf(HTMLElement);
    expect(customElements.get('ci-dot-diagram')).toBeDefined();
  });

  it('has source property', async () => {
    const el = await createElement({ source: 'digraph G { a -> b; }' });
    expect(el.source).toBe('digraph G { a -> b; }');
  });

  it('has engine property defaulting to dot', async () => {
    const el = await createElement();
    expect(el.engine).toBe('dot');
  });

  it('creates .diagram-container div in shadow DOM', async () => {
    const el = await createElement({ source: 'digraph G { a -> b; }' });
    await waitForAsyncRender(el);
    const container = el.shadowRoot!.querySelector('.diagram-container');
    expect(container).not.toBeNull();
  });
});

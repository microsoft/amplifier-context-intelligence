import { describe, it, expect, afterEach } from 'vitest';
import '../../src/catalog/metric-card.js';

// ── Helper ──────────────────────────────────────────────────────────────────

type MetricCardProps = {
  componentId?: string;
  label?: string;
  value?: string | number;
  unit?: string;
  trend?: 'up' | 'down' | 'flat' | '';
  trendLabel?: string;
};

async function createElement(props: MetricCardProps = {}): Promise<HTMLElement> {
  const el = document.createElement('ci-metric-card') as HTMLElement & MetricCardProps;
  Object.assign(el, props);
  document.body.appendChild(el);
  // Wait for Lit's async rendering to complete
  await (el as HTMLElement & { updateComplete?: Promise<boolean> }).updateComplete;
  return el;
}

// ── Tests ───────────────────────────────────────────────────────────────────

describe('ci-metric-card', () => {
  afterEach(() => {
    document.querySelectorAll('ci-metric-card').forEach(el => el.remove());
  });

  it('is defined as a custom element', () => {
    const el = document.createElement('ci-metric-card');
    expect(el).toBeInstanceOf(HTMLElement);
    expect(customElements.get('ci-metric-card')).toBeDefined();
  });

  it('renders label and value in shadow DOM', async () => {
    const el = await createElement({ label: 'Sessions', value: '42' });
    const root = el.shadowRoot!;
    expect(root).not.toBeNull();
    const label = root.querySelector('.label');
    const value = root.querySelector('.value');
    expect(label).not.toBeNull();
    expect(value).not.toBeNull();
    expect(label!.textContent).toContain('Sessions');
    expect(value!.textContent).toContain('42');
  });

  it('renders unit when provided', async () => {
    const el = await createElement({ label: 'Latency', value: '1.5', unit: 'sec' });
    const root = el.shadowRoot!;
    const unit = root.querySelector('.unit');
    expect(unit).not.toBeNull();
    expect(unit!.textContent).toContain('sec');
  });

  it('renders trend indicator with trend-up class when trend is up', async () => {
    const el = await createElement({ label: 'Sessions', value: '42', trend: 'up' });
    const root = el.shadowRoot!;
    const trend = root.querySelector('.trend');
    expect(trend).not.toBeNull();
    expect(trend!.classList.contains('trend-up')).toBe(true);
  });

  it('dispatches ci-action event on click', async () => {
    const el = await createElement({ label: 'Sessions', value: '42', componentId: 'metric-1' });
    const root = el.shadowRoot!;
    const card = root.querySelector('.metric-card');
    expect(card).not.toBeNull();

    let dispatchedEvent: CustomEvent | null = null;
    el.addEventListener('ci-action', (e: Event) => {
      dispatchedEvent = e as CustomEvent;
    });

    (card as HTMLElement).click();

    expect(dispatchedEvent).not.toBeNull();
    expect((dispatchedEvent as unknown as CustomEvent).detail.name).toBe('metric-click');
    expect((dispatchedEvent as unknown as CustomEvent).detail.componentId).toBe('metric-1');
  });
});

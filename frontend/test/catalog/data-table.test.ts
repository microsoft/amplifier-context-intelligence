import { describe, it, expect, afterEach } from 'vitest';
import '../../src/catalog/data-table.js';

// ── Helper ────────────────────────────────────────────────────────────────────

type TableColumn = {
  key: string;
  label: string;
  sortable?: boolean;
};

type DataTableProps = {
  componentId?: string;
  columns?: TableColumn[];
  rows?: Record<string, unknown>[];
  pageSize?: number;
};

async function createElement(props: DataTableProps = {}): Promise<HTMLElement> {
  const el = document.createElement('ci-data-table') as HTMLElement & DataTableProps;
  Object.assign(el, props);
  document.body.appendChild(el);
  // Wait for Lit's async rendering to complete
  await (el as HTMLElement & { updateComplete?: Promise<boolean> }).updateComplete;
  return el;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('ci-data-table', () => {
  afterEach(() => {
    document.querySelectorAll('ci-data-table').forEach(el => el.remove());
  });

  it('is defined as a custom element', () => {
    const el = document.createElement('ci-data-table');
    expect(el).toBeInstanceOf(HTMLElement);
    expect(customElements.get('ci-data-table')).toBeDefined();
  });

  it('renders column headers', async () => {
    const el = await createElement({
      columns: [
        { key: 'name', label: 'Name', sortable: true },
        { key: 'status', label: 'Status' },
      ],
      rows: [],
    });
    const root = el.shadowRoot!;
    expect(root).not.toBeNull();
    const headers = root.querySelectorAll('th');
    expect(headers.length).toBe(2);
    const headerTexts = Array.from(headers).map(h => h.textContent?.trim());
    expect(headerTexts.some(t => t?.includes('Name'))).toBe(true);
    expect(headerTexts.some(t => t?.includes('Status'))).toBe(true);
  });

  it('renders row data', async () => {
    const el = await createElement({
      columns: [
        { key: 'name', label: 'Name' },
        { key: 'status', label: 'Status' },
      ],
      rows: [
        { name: 'Session A', status: 'active' },
        { name: 'Session B', status: 'idle' },
      ],
    });
    const root = el.shadowRoot!;
    const cells = root.querySelectorAll('td');
    const cellTexts = Array.from(cells).map(c => c.textContent?.trim());
    expect(cellTexts).toContain('Session A');
    expect(cellTexts).toContain('Session B');
  });

  it('dispatches ci-action on row click', async () => {
    const el = await createElement({
      componentId: 'table-1',
      columns: [{ key: 'name', label: 'Name' }],
      rows: [{ name: 'Session A' }],
    });
    const root = el.shadowRoot!;
    const row = root.querySelector('tbody tr');
    expect(row).not.toBeNull();

    let dispatchedEvent: CustomEvent | null = null;
    el.addEventListener('ci-action', (e: Event) => {
      dispatchedEvent = e as CustomEvent;
    });

    (row as HTMLElement).click();

    expect(dispatchedEvent).not.toBeNull();
    expect(dispatchedEvent!.detail.name).toBe('row-click');
  });

  it('sorts by column when header is clicked', async () => {
    const el = await createElement({
      columns: [{ key: 'name', label: 'Name', sortable: true }],
      rows: [{ name: 'Charlie' }, { name: 'Alpha' }, { name: 'Bravo' }],
    });
    const root = el.shadowRoot!;
    const header = root.querySelector('th');
    expect(header).not.toBeNull();

    (header as HTMLElement).click();
    await (el as HTMLElement & { updateComplete?: Promise<boolean> }).updateComplete;

    const cells = Array.from(root.querySelectorAll('tbody td'));
    const names = cells.map(c => c.textContent?.trim());
    expect(names).toEqual(['Alpha', 'Bravo', 'Charlie']);
  });
});

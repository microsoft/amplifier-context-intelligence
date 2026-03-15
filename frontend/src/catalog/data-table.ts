import { LitElement, html, css } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';

// ── DataTable ─────────────────────────────────────────────────────────────────
//
// Sortable data table Lit web component with row-click actions.
// Supports paginated display (pageSize), ascending/descending sort on any
// column marked sortable, and a `ci-action` CustomEvent on row click.

export interface TableColumn {
  key: string;
  label: string;
  sortable?: boolean;
}

@customElement('ci-data-table')
export class DataTable extends LitElement {
  static styles = css`
    :host {
      display: block;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.875rem;
    }

    thead {
      border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    }

    th {
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 0.7rem;
      font-weight: 600;
      color: var(--color-muted, #94a3b8);
      padding: 0.5rem 0.75rem;
      text-align: left;
      white-space: nowrap;
    }

    th.sortable {
      cursor: pointer;
      user-select: none;
    }

    th.sortable:hover {
      color: var(--color-text, #f1f5f9);
    }

    td {
      padding: 0.5rem 0.75rem;
      color: var(--color-text, #f1f5f9);
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    }

    tbody tr {
      cursor: pointer;
      transition: background 0.15s ease;
    }

    tbody tr:hover {
      background: rgba(255, 255, 255, 0.05);
    }

    .sort-indicator {
      margin-left: 0.25rem;
      display: inline-block;
      width: 0.75em;
    }

    .empty-state {
      padding: 2rem;
      text-align: center;
      color: var(--color-muted, #94a3b8);
      font-size: 0.875rem;
    }
  `;

  @property({ type: String }) componentId = '';
  @property({ type: Array }) columns: TableColumn[] = [];
  @property({ type: Array }) rows: Record<string, unknown>[] = [];
  @property({ type: Number }) pageSize = 50;

  @state() private sortKey = '';
  @state() private sortAsc = true;

  private get sortedRows(): Record<string, unknown>[] {
    const key = this.sortKey;
    if (!key) {
      return this.rows.slice(0, this.pageSize);
    }
    const asc = this.sortAsc;
    const sorted = [...this.rows].sort((a, b) => {
      const aVal = String(a[key] ?? '');
      const bVal = String(b[key] ?? '');
      const cmp = aVal.localeCompare(bVal);
      return asc ? cmp : -cmp;
    });
    return sorted.slice(0, this.pageSize);
  }

  private handleSort(col: TableColumn): void {
    if (!col.sortable) return;
    if (this.sortKey === col.key) {
      this.sortAsc = !this.sortAsc;
    } else {
      this.sortKey = col.key;
      this.sortAsc = true;
    }
  }

  private handleRowClick(row: Record<string, unknown>, index: number): void {
    this.dispatchEvent(
      new CustomEvent('ci-action', {
        bubbles: true,
        composed: true,
        detail: {
          name: 'row-click',
          componentId: this.componentId,
          row,
          index,
        },
      }),
    );
  }

  private sortIndicator(col: TableColumn): string {
    if (this.sortKey !== col.key) return '';
    return this.sortAsc ? '▲' : '▼';
  }

  render() {
    const rows = this.sortedRows;
    return html`
      <table>
        <thead>
          <tr>
            ${this.columns.map(
              col => html`
                <th
                  class=${col.sortable ? 'sortable' : ''}
                  @click=${() => this.handleSort(col)}
                >
                  ${col.label}${col.sortable
                    ? html`<span class="sort-indicator">${this.sortIndicator(col)}</span>`
                    : ''}
                </th>
              `,
            )}
          </tr>
        </thead>
        <tbody>
          ${rows.length === 0
            ? html`
                <tr>
                  <td colspan=${this.columns.length}>
                    <div class="empty-state">No data</div>
                  </td>
                </tr>
              `
            : rows.map(
                (row, index) => html`
                  <tr @click=${() => this.handleRowClick(row, index)}>
                    ${this.columns.map(col => html`<td>${String(row[col.key] ?? '')}</td>`)}
                  </tr>
                `,
              )}
        </tbody>
      </table>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'ci-data-table': DataTable;
  }
}

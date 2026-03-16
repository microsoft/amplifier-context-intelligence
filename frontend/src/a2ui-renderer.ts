import { LitElement, html, nothing } from 'lit';
import { customElement, state } from 'lit/decorators.js';
import { getCatalogFactory } from './catalog/index.js';
import type {
  BoundValue,
  DataEntry,
  ComponentNode,
  SurfaceState,
  ResolvedComponent,
  A2UIServerMessage,
  BeginRenderingPayload,
  SurfaceUpdatePayload,
  DataModelUpdatePayload,
  UserActionPayload,
} from './types/a2ui.js';

// ── Pure helper functions ─────────────────────────────────────────────────────

/**
 * RFC 6901 JSON Pointer resolution.
 *
 * Special cases:
 *   ""  → returns the entire model (root reference)
 *   "/" → returns the entire model (treated as root; the protocol uses "/" for root)
 *
 * Segment decoding follows RFC 6901:
 *   ~1 → /
 *   ~0 → ~
 */
export function resolveJsonPointer(model: unknown, path: string): unknown {
  // Empty path or bare "/" is treated as root
  if (path === '' || path === '/') return model;

  // All valid pointers start with "/"; strip it then split on "/"
  const segments = path
    .slice(1)
    .split('/')
    .map((s) => s.replace(/~1/g, '/').replace(/~0/g, '~'));

  let current: unknown = model;
  for (const segment of segments) {
    if (current === null || current === undefined) return undefined;
    if (Array.isArray(current)) {
      current = current[Number(segment)];
    } else if (typeof current === 'object') {
      current = (current as Record<string, unknown>)[segment];
    } else {
      return undefined;
    }
  }
  return current;
}

/**
 * Convert an array of `DataEntry` records into a plain JavaScript object.
 * Handles `valueString`, `valueNumber`, `valueBoolean`, and recursive `valueMap`.
 * Entries with `valueList` are currently not mapped (not required by spec).
 */
export function dataEntriesToObject(entries: DataEntry[]): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const entry of entries) {
    if (entry.valueString !== undefined) {
      result[entry.key] = entry.valueString;
    } else if (entry.valueNumber !== undefined) {
      result[entry.key] = entry.valueNumber;
    } else if (entry.valueBoolean !== undefined) {
      result[entry.key] = entry.valueBoolean;
    } else if (entry.valueMap !== undefined) {
      result[entry.key] = dataEntriesToObject(Object.values(entry.valueMap));
    }
  }
  return result;
}

/**
 * Pure helper: merge `contents` into a copy of `model` at the optional `path`.
 *
 * If `path` is undefined or empty, entries are merged at root level.
 * Path segments are separated by '/'; each segment navigates into a
 * DataEntry's `valueMap`. Intermediate entries are created when absent.
 *
 * Extracted from `handleDataModelUpdate` so it can be tested directly — the
 * inline proxy approach silently discarded writes for paths with 2+ segments
 * because the proxy Map was never flushed back to its parent DataEntry.valueMap.
 */
export function mergeDataModelEntries(
  model: Map<string, DataEntry>,
  path: string | undefined,
  contents: DataEntry[],
): Map<string, DataEntry> {
  const updated = new Map(model);

  if (!path) {
    for (const entry of contents) {
      updated.set(entry.key, entry);
    }
    return updated;
  }

  const segments = path.split('/').filter(Boolean);
  mergeAtSegments(updated, segments, contents);
  return updated;
}

/**
 * Recursive helper: write `contents` into the leaf DataEntry reached by
 * `segments`, then backpatch each ancestor's `valueMap` on the way back up.
 *
 * A new Map is created for each nested level and converted back to a
 * `Record<string, DataEntry>` after recursion so the parent entry's
 * `valueMap` always reflects the latest state.
 */
function mergeAtSegments(
  map: Map<string, DataEntry>,
  segments: string[],
  contents: DataEntry[],
): void {
  if (segments.length === 0) return;
  const [head, ...tail] = segments as [string, ...string[]];

  if (tail.length === 0) {
    // Final segment: merge contents into this entry's valueMap
    for (const entry of contents) {
      const existing = map.get(head);
      const existingMap = existing?.valueMap ?? {};
      map.set(head, {
        key: head,
        valueMap: { ...existingMap, [entry.key]: entry },
      });
    }
  } else {
    // Intermediate segment: navigate into valueMap, recurse, then backpatch.
    // We must convert back to a Record after recursion because the Map is a
    // copy — any writes inside would otherwise be silently discarded.
    const existing = map.get(head);
    const nestedMap = new Map<string, DataEntry>(
      Object.entries(existing?.valueMap ?? {}),
    );
    mergeAtSegments(nestedMap, tail, contents);
    // Backpatch: rebuild the Record from the mutated nestedMap
    const updatedValueMap: Record<string, DataEntry> = {};
    for (const [k, v] of nestedMap) updatedValueMap[k] = v;
    map.set(head, { key: head, valueMap: updatedValueMap });
  }
}

/** Returns true when `value` has the shape of a `BoundValue` (contains at least one BoundValue key). */
function isBoundValue(value: unknown): value is BoundValue {
  if (typeof value !== 'object' || value === null) return false;
  const obj = value as Record<string, unknown>;
  return (
    'literalString' in obj ||
    'literalNumber' in obj ||
    'literalBoolean' in obj ||
    'path' in obj
  );
}

/**
 * Resolve a single property value that may or may not be a `BoundValue`.
 *
 * Priority order when multiple fields are present:
 *   path > literalString > literalNumber > literalBoolean
 *
 * Non-BoundValue objects and all primitives (including null) are returned as-is.
 *
 * @param value     The raw property value from the component node.
 * @param dataModel A plain object data model used to resolve `path` references.
 */
export function resolveBoundValue(value: unknown, dataModel: Record<string, unknown>): unknown {
  // Primitives and null pass through unchanged
  if (typeof value !== 'object' || value === null) return value;

  // Non-BoundValue objects pass through unchanged
  if (!isBoundValue(value)) return value;

  // Resolve by priority: path > literalString > literalNumber > literalBoolean
  if (value.path !== undefined) {
    return resolveJsonPointer(dataModel, value.path);
  }
  if (value.literalString !== undefined) return value.literalString;
  if (value.literalNumber !== undefined) return value.literalNumber;
  if (value.literalBoolean !== undefined) return value.literalBoolean;
  return undefined;
}

/**
 * Parse a `ComponentNode` into a `ResolvedComponent` by:
 *   1. Extracting the type name from the single key of the `component` wrapper object.
 *   2. Resolving every property value through `resolveBoundValue`.
 *
 * @param node      The raw component node from the server.
 * @param dataModel Plain object data model for `path` resolution.
 */
export function parseComponentNode(
  node: ComponentNode,
  dataModel: Record<string, unknown>,
): ResolvedComponent {
  const typeName = Object.keys(node.component)[0];
  const rawProps = node.component[typeName];

  const resolvedProps: Record<string, unknown> = {};
  for (const [key, val] of Object.entries(rawProps)) {
    resolvedProps[key] = resolveBoundValue(val, dataModel);
  }

  return { id: node.id, type: typeName, resolvedProps };
}

// ── Lit element ───────────────────────────────────────────────────────────────

/** Registered as <ci-a2ui-renderer>. Manages per-surface state and renders catalog components. */
@customElement('ci-a2ui-renderer')
export class A2UIRenderer extends LitElement {
  @state() private surfaces: Map<string, SurfaceState> = new Map();

  // ── Message dispatch ──────────────────────────────────────────────────────

  /** Route an inbound A2UI server message to the appropriate handler. */
  processMessage(message: A2UIServerMessage): void {
    if ('beginRendering' in message) {
      this.handleBeginRendering(message.beginRendering);
    } else if ('surfaceUpdate' in message) {
      this.handleSurfaceUpdate(message.surfaceUpdate);
    } else if ('dataModelUpdate' in message) {
      this.handleDataModelUpdate(message.dataModelUpdate);
    } else if ('deleteSurface' in message) {
      const { surfaceId } = message.deleteSurface;
      const next = new Map(this.surfaces);
      next.delete(surfaceId);
      this.surfaces = next;
    }
  }

  // ── Message handlers ──────────────────────────────────────────────────────

  /** Create a fresh `SurfaceState` for the incoming surface. */
  private handleBeginRendering(payload: BeginRenderingPayload): void {
    const { surfaceId, catalogId = '', root, styles = {} } = payload;
    const components = new Map<string, ComponentNode>();
    components.set(root.id, root);

    const surface: SurfaceState = {
      surfaceId,
      catalogId,
      rootId: root.id,
      components,
      dataModel: new Map(),
      styles,
    };

    const next = new Map(this.surfaces);
    next.set(surfaceId, surface);
    this.surfaces = next;
  }

  /** Replace the components array for an existing surface. */
  private handleSurfaceUpdate(payload: SurfaceUpdatePayload): void {
    const { surfaceId, components } = payload;
    const surface = this.surfaces.get(surfaceId);
    if (!surface) return;

    const updatedComponents = new Map<string, ComponentNode>();
    for (const node of components) {
      updatedComponents.set(node.id, node);
    }

    const next = new Map(this.surfaces);
    next.set(surfaceId, { ...surface, components: updatedComponents });
    this.surfaces = next;
  }

  /**
   * Merge new data entries into the surface's data model.
   *
   * Delegates to the pure `mergeDataModelEntries` helper so that the path-
   * walking logic is isolated, testable, and free of the detached-proxy bug
   * that previously caused writes to multi-segment paths to be silently lost.
   */
  private handleDataModelUpdate(payload: DataModelUpdatePayload): void {
    const { surfaceId, path, contents } = payload;
    const surface = this.surfaces.get(surfaceId);
    if (!surface) return;

    const updatedModel = mergeDataModelEntries(surface.dataModel, path, contents);

    const next = new Map(this.surfaces);
    next.set(surfaceId, { ...surface, dataModel: updatedModel });
    this.surfaces = next;
  }

  /**
   * Dispatch an `a2ui-action` CustomEvent that bubbles up through the DOM.
   * Catalog components call this when the user interacts with them.
   */
  handleAction(payload: UserActionPayload): void {
    this.dispatchEvent(
      new CustomEvent<UserActionPayload>('a2ui-action', {
        detail: payload,
        bubbles: true,
        composed: true,
      }),
    );
  }

  // ── Rendering ─────────────────────────────────────────────────────────────

  override render() {
    return html`${[...this.surfaces.values()].map((s) => this.renderSurface(s))}`;
  }

  private renderSurface(surface: SurfaceState) {
    const rootNode = surface.components.get(surface.rootId);
    if (!rootNode) return nothing;

    const dataModel = dataEntriesToObject(
      [...surface.dataModel.values()],
    );

    const resolved = parseComponentNode(rootNode, dataModel);
    return this.renderCatalogComponent(resolved);
  }

  private renderCatalogComponent(resolved: ResolvedComponent) {
    const factory = getCatalogFactory(resolved.type);
    if (!factory) {
      return html`<div data-component-id=${resolved.id} data-type=${resolved.type}></div>`;
    }
    return factory(resolved.resolvedProps);
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'ci-a2ui-renderer': A2UIRenderer;
  }
}

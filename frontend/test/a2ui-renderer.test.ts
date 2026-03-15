import { describe, it, expect } from 'vitest';
import {
  resolveJsonPointer,
  dataEntriesToObject,
  resolveBoundValue,
  parseComponentNode,
  mergeDataModelEntries,
} from '../src/a2ui-renderer.js';
import type { DataEntry, ComponentNode, BoundValue } from '../src/types/a2ui.js';

// ── resolveJsonPointer ──────────────────────────────────────────────────────

describe('resolveJsonPointer', () => {
  const model = {
    user: {
      name: 'Alice',
      scores: [10, 20, 30],
    },
  };

  it('resolves root-level path /user', () => {
    expect(resolveJsonPointer(model, '/user')).toEqual({
      name: 'Alice',
      scores: [10, 20, 30],
    });
  });

  it('resolves nested path /user/name', () => {
    expect(resolveJsonPointer(model, '/user/name')).toBe('Alice');
  });

  it('resolves numeric index /user/scores/1', () => {
    expect(resolveJsonPointer(model, '/user/scores/1')).toBe(20);
  });

  it('returns undefined for missing path', () => {
    expect(resolveJsonPointer(model, '/user/missing')).toBeUndefined();
  });

  it('returns entire model for empty path "/"', () => {
    expect(resolveJsonPointer(model, '/')).toEqual(model);
  });
});

// ── dataEntriesToObject ─────────────────────────────────────────────────────

describe('dataEntriesToObject', () => {
  it('converts flat entries with valueString/valueNumber/valueBoolean', () => {
    const entries: DataEntry[] = [
      { key: 'name', valueString: 'Alice' },
      { key: 'age', valueNumber: 30 },
      { key: 'active', valueBoolean: true },
    ];
    expect(dataEntriesToObject(entries)).toEqual({
      name: 'Alice',
      age: 30,
      active: true,
    });
  });

  it('converts nested valueMap entries', () => {
    const entries: DataEntry[] = [
      {
        key: 'user',
        valueMap: {
          name: { key: 'name', valueString: 'Bob' },
          score: { key: 'score', valueNumber: 42 },
        },
      },
    ];
    expect(dataEntriesToObject(entries)).toEqual({
      user: {
        name: 'Bob',
        score: 42,
      },
    });
  });
});

// ── resolveBoundValue ───────────────────────────────────────────────────────

describe('resolveBoundValue', () => {
  it('resolves literalString', () => {
    const bv: BoundValue = { literalString: 'hello' };
    expect(resolveBoundValue(bv, {})).toBe('hello');
  });

  it('resolves literalNumber', () => {
    const bv: BoundValue = { literalNumber: 42 };
    expect(resolveBoundValue(bv, {})).toBe(42);
  });

  it('resolves literalBoolean', () => {
    const bv: BoundValue = { literalBoolean: false };
    expect(resolveBoundValue(bv, {})).toBe(false);
  });

  it('resolves path against data model', () => {
    const bv: BoundValue = { path: '/user/name' };
    const model = { user: { name: 'Carol' } };
    expect(resolveBoundValue(bv, model)).toBe('Carol');
  });

  it('prefers path over literal when both present', () => {
    const bv: BoundValue = { path: '/score', literalNumber: 0 };
    const model = { score: 99 };
    expect(resolveBoundValue(bv, model)).toBe(99);
  });

  it('returns non-BoundValue objects as-is', () => {
    const obj = { someRandomField: 'value' };
    expect(resolveBoundValue(obj, {})).toBe(obj);
  });

  it('returns primitives as-is', () => {
    expect(resolveBoundValue('raw string', {})).toBe('raw string');
    expect(resolveBoundValue(123, {})).toBe(123);
    expect(resolveBoundValue(true, {})).toBe(true);
    expect(resolveBoundValue(null, {})).toBeNull();
  });
});

// ── mergeDataModelEntries ────────────────────────────────────────────────────

describe('mergeDataModelEntries', () => {
  it('merges contents at root when path is undefined', () => {
    const model: Map<string, DataEntry> = new Map();
    const contents: DataEntry[] = [{ key: 'name', valueString: 'Alice' }];
    const result = mergeDataModelEntries(model, undefined, contents);
    expect(result.get('name')).toEqual({ key: 'name', valueString: 'Alice' });
  });

  it('merges contents at single-segment path', () => {
    const model: Map<string, DataEntry> = new Map();
    const contents: DataEntry[] = [{ key: 'name', valueString: 'Bob' }];
    const result = mergeDataModelEntries(model, 'user', contents);
    const userEntry = result.get('user');
    expect(userEntry?.valueMap?.['name']).toEqual({ key: 'name', valueString: 'Bob' });
  });

  it('merges contents at two-segment path (multi-segment path bug fix)', () => {
    const model: Map<string, DataEntry> = new Map();
    const contents: DataEntry[] = [{ key: 'city', valueString: 'London' }];
    const result = mergeDataModelEntries(model, 'user/address', contents);
    // result["user"].valueMap["address"].valueMap["city"] should be set
    const userEntry = result.get('user');
    const addressEntry = userEntry?.valueMap?.['address'];
    expect(addressEntry?.valueMap?.['city']).toEqual({ key: 'city', valueString: 'London' });
  });

  it('preserves existing entries in the model', () => {
    const existing: DataEntry = { key: 'role', valueString: 'admin' };
    const model: Map<string, DataEntry> = new Map([['role', existing]]);
    const contents: DataEntry[] = [{ key: 'name', valueString: 'Carol' }];
    const result = mergeDataModelEntries(model, undefined, contents);
    expect(result.get('role')).toEqual(existing);
    expect(result.get('name')).toEqual({ key: 'name', valueString: 'Carol' });
  });

  it('preserves existing valueMap entries at leaf when merging with path', () => {
    const model: Map<string, DataEntry> = new Map([
      ['user', { key: 'user', valueMap: { name: { key: 'name', valueString: 'Dan' } } }],
    ]);
    const contents: DataEntry[] = [{ key: 'age', valueNumber: 25 }];
    const result = mergeDataModelEntries(model, 'user', contents);
    const userEntry = result.get('user');
    expect(userEntry?.valueMap?.['name']).toEqual({ key: 'name', valueString: 'Dan' });
    expect(userEntry?.valueMap?.['age']).toEqual({ key: 'age', valueNumber: 25 });
  });
});

// ── parseComponentNode ──────────────────────────────────────────────────────

describe('parseComponentNode', () => {
  it('extracts type and properties from component wrapper with BoundValue resolution', () => {
    const node: ComponentNode = {
      id: 'node-1',
      component: {
        button: {
          label: { literalString: 'Click me' } as BoundValue,
          disabled: { literalBoolean: false } as BoundValue,
        },
      },
    };
    const result = parseComponentNode(node, {});
    expect(result.id).toBe('node-1');
    expect(result.type).toBe('button');
    expect(result.resolvedProps).toEqual({ label: 'Click me', disabled: false });
  });

  it('handles component with no bound values', () => {
    const node: ComponentNode = {
      id: 'node-2',
      component: {
        text: {
          content: 'Hello, world!',
        },
      },
    };
    const result = parseComponentNode(node, {});
    expect(result.id).toBe('node-2');
    expect(result.type).toBe('text');
    expect(result.resolvedProps).toEqual({ content: 'Hello, world!' });
  });
});

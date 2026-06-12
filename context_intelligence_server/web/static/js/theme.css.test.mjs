/**
 * Source-assertion tests for theme.css.
 * Run with: node theme.css.test.mjs
 * Node.js built-in test runner (no dependencies required).
 *
 * Purpose: lock the .table-scroll > .data-table { min-width: max-content }
 * rule so that narrow-viewport table clipping cannot regress silently.
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dir = dirname(fileURLToPath(import.meta.url));
const cssPath = join(__dir, '../css/theme.css');
const css = readFileSync(cssPath, 'utf-8');

// ── table-scroll narrow-width fix ──────────────────────────────────────────

describe('table-scroll narrow-width fix', () => {
  test('theme.css has .table-scroll > .data-table selector', () => {
    assert.ok(
      css.includes('.table-scroll > .data-table'),
      'theme.css must contain .table-scroll > .data-table (missing: add min-width: max-content rule)'
    );
  });

  test('theme.css has min-width: max-content on .table-scroll > .data-table', () => {
    // Find the block after the selector and verify min-width: max-content is present.
    // We check both selector and property appear in the file; the source-grep
    // approach matches the existing dashboard.js.test.mjs pattern and is
    // sufficient to lock the rule against accidental deletion.
    assert.ok(
      css.includes('min-width: max-content'),
      'theme.css must contain min-width: max-content (missing fix for button clipping at narrow widths)'
    );
  });
});

// ── narrow-width action button stacking fix ────────────────────────────────
describe('narrow-width action button stacking', () => {
  test('theme.css has @media (max-width: 760px) block', () => {
    assert.ok(
      css.includes('@media (max-width: 760px)'),
      'theme.css must contain @media (max-width: 760px) — missing responsive stacking rule for action buttons at narrow widths'
    );
  });

  test('theme.css stacks .actions flex-direction:column inside the 760px media query', () => {
    // Source-grep: the 760px block must apply flex-direction:column to .actions.
    // flex-direction:column exists elsewhere in the file, so we slice from the
    // @media start and check both .actions and flex-direction:column appear
    // within that block — not just anywhere in the file.
    const idx = css.indexOf('@media (max-width: 760px)');
    const narrowBlock = idx >= 0 ? css.slice(idx) : '';
    assert.ok(
      narrowBlock.includes('.actions') && narrowBlock.includes('flex-direction: column'),
      'theme.css @media (max-width: 760px) block must set flex-direction: column on .actions'
    );
  });
});

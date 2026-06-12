/**
 * Tests for dashboard.html structure requirements.
 * Run with: node dashboard.html.test.mjs
 * Node.js built-in test runner (no dependencies required).
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dir = dirname(fileURLToPath(import.meta.url));
const htmlPath = join(__dir, '../../dashboard.html');
const html = readFileSync(htmlPath, 'utf-8');

// ── Nav link removal ────────────────────────────────────────────────────────

describe('Nav link removal', () => {
  test('does NOT contain localhost:7474 link in nav', () => {
    assert.ok(
      !html.includes('localhost:7474'),
      'Nav should not contain href to localhost:7474 (Neo4j browser link)'
    );
  });
});

// ── Neo4j stat chip ─────────────────────────────────────────────────────────

describe('Neo4j stat chip', () => {
  test('has element with id="neo4j-status"', () => {
    assert.ok(
      html.includes('id="neo4j-status"'),
      'Should have a stat value element with id="neo4j-status"'
    );
  });

  test('Neo4j stat chip has stat-label "Neo4j"', () => {
    assert.ok(
      html.includes('>Neo4j<'),
      'Should have a stat label with text "Neo4j"'
    );
  });
});

// ── Auth overlay structure ───────────────────────────────────────────────────

describe('Auth overlay structure', () => {
  test('has element with id="auth-overlay"', () => {
    assert.ok(
      html.includes('id="auth-overlay"'),
      'Should have a fullscreen auth overlay div with id="auth-overlay"'
    );
  });

  test('auth overlay has z-index:9999', () => {
    // Extract the auth-overlay div region
    const idx = html.indexOf('id="auth-overlay"');
    assert.ok(idx !== -1, 'auth-overlay must exist');
    // Check nearby context for z-index
    const context = html.slice(Math.max(0, idx - 200), idx + 200);
    assert.ok(
      context.includes('z-index') && context.includes('9999'),
      'auth-overlay should have z-index:9999 in its style'
    );
  });

  test('has password input with id="auth-token-input"', () => {
    assert.ok(
      html.includes('id="auth-token-input"'),
      'Should have a password input with id="auth-token-input"'
    );
  });

  test('auth-token-input is of type password', () => {
    // Find the input element
    const match = html.match(/id="auth-token-input"[^>]*>|<input[^>]*id="auth-token-input"[^>]*>/);
    assert.ok(match, 'auth-token-input element must exist');
    // The input or surrounding context should have type="password"
    const idx = html.indexOf('id="auth-token-input"');
    const context = html.slice(Math.max(0, idx - 150), idx + 150);
    assert.ok(
      context.includes('type="password"') || context.includes("type='password'"),
      'auth-token-input should be type="password"'
    );
  });

  test('has submit button with id="auth-submit-btn"', () => {
    assert.ok(
      html.includes('id="auth-submit-btn"'),
      'Should have a submit button with id="auth-submit-btn"'
    );
  });

  test('has error paragraph with id="auth-error"', () => {
    assert.ok(
      html.includes('id="auth-error"'),
      'Should have an error message element with id="auth-error"'
    );
  });

  test('auth-error is hidden by default', () => {
    const idx = html.indexOf('id="auth-error"');
    assert.ok(idx !== -1, 'auth-error must exist');
    const context = html.slice(Math.max(0, idx - 100), idx + 100);
    assert.ok(
      context.includes('display:none') || context.includes('display: none'),
      'auth-error should be hidden by default (display:none)'
    );
  });

  test('overlay contains Amplifier avatar image', () => {
    // The overlay should contain the Amplifier avatar URL
    const idx = html.indexOf('id="auth-overlay"');
    assert.ok(idx !== -1, 'auth-overlay must exist');
    // Find the closing tag of auth-overlay
    const overlayEnd = html.indexOf('</div>', idx);
    const overlayRegion = html.slice(idx, overlayEnd + 1000); // generous window
    assert.ok(
      overlayRegion.includes('avatars.githubusercontent.com'),
      'auth-overlay should contain Amplifier avatar image'
    );
  });

  test('overlay is positioned before nav', () => {
    const overlayIdx = html.indexOf('id="auth-overlay"');
    const navIdx = html.indexOf('<nav ');
    assert.ok(overlayIdx !== -1, 'auth-overlay must exist');
    assert.ok(navIdx !== -1, 'nav must exist');
    assert.ok(
      overlayIdx < navIdx,
      'auth-overlay should appear before the nav element in the DOM'
    );
  });
});

// ── Auth gate script ─────────────────────────────────────────────────────────

describe('Auth gate script', () => {
  test('script checks localStorage for ci_api_key', () => {
    assert.ok(
      html.includes('ci_api_key'),
      'Script should reference ci_api_key in localStorage'
    );
  });

  test('script defines tryAuth function or equivalent', () => {
    assert.ok(
      html.includes('tryAuth') || html.includes('function tryAuth'),
      'Script should contain tryAuth function'
    );
  });

  test('script POSTs RETURN 1 to /cypher for validation', () => {
    assert.ok(
      html.includes('RETURN 1') && html.includes('/cypher'),
      'tryAuth should validate by POSTing "RETURN 1" to /cypher'
    );
  });

  test('script uses Bearer token in Authorization header', () => {
    assert.ok(
      html.includes('Bearer'),
      'tryAuth should set Authorization: Bearer <token> header'
    );
  });

  test('auth-submit-btn has click handler attached', () => {
    assert.ok(
      html.includes('auth-submit-btn') && html.includes('addEventListener'),
      'auth-submit-btn should have addEventListener for click'
    );
  });

  test('auth-token-input has Enter key handler', () => {
    const idx = html.indexOf('auth-token-input');
    assert.ok(idx !== -1, 'auth-token-input must exist');
    // Check that keydown or keypress with Enter is handled
    assert.ok(
      html.includes('Enter') || html.includes('keydown') || html.includes('keypress'),
      'auth-token-input should have Enter key handler'
    );
  });
});

// ── Pipeline health hint (C2) ──────────────────────────────────────────────

const indexPath = join(__dir, '../../index.html');
const indexHtml = readFileSync(indexPath, 'utf-8');

describe('In-dashboard Queues tab (C2)', () => {
  test('dashboard.html has NO /queues link anywhere', () => {
    assert.ok(
      !html.includes('href="/queues"'),
      'dashboard.html must not contain any href="/queues" (Queues is now an in-page tab)'
    );
  });

  test('index.html has NO /queues link', () => {
    assert.ok(
      !indexHtml.includes('href="/queues"'),
      'index.html must not contain any href="/queues"'
    );
  });

  test('has a tab bar with role="tablist"', () => {
    assert.ok(html.includes('class="tab-bar"'), 'Should have a tab bar with class="tab-bar"');
    const idx = html.indexOf('class="tab-bar"');
    const open = html.slice(Math.max(0, idx - 120), idx + 120);
    assert.ok(open.includes('role="tablist"'), 'tab-bar should have role="tablist"');
  });

  test('has Overview and Queues tab buttons with ids + data-tab', () => {
    assert.ok(html.includes('id="tab-overview"'), 'Should have id="tab-overview"');
    assert.ok(html.includes('id="tab-queues"'), 'Should have id="tab-queues"');
    assert.ok(html.includes('data-tab="overview"'), 'Should have data-tab="overview"');
    assert.ok(html.includes('data-tab="queues"'), 'Should have data-tab="queues"');
  });

  test('Queues tab button has role=tab + aria-controls=panel-queues + aria-selected', () => {
    const idx = html.indexOf('id="tab-queues"');
    assert.ok(idx !== -1, 'tab-queues must exist');
    const open = html.slice(Math.max(0, idx - 160), idx + 200);
    assert.ok(open.includes('role="tab"'), 'tab-queues should have role="tab"');
    assert.ok(open.includes('aria-controls="panel-queues"'), 'tab-queues should have aria-controls="panel-queues"');
    assert.ok(open.includes('aria-selected'), 'tab-queues should have aria-selected');
  });

  test('has #panel-overview and #panel-queues tabpanels', () => {
    assert.ok(html.includes('id="panel-overview"'), 'Should have id="panel-overview"');
    assert.ok(html.includes('id="panel-queues"'), 'Should have id="panel-queues"');
  });

  test('Queues panel is hidden by default', () => {
    const idx = html.indexOf('id="panel-queues"');
    assert.ok(idx !== -1, 'panel-queues must exist');
    const open = html.slice(Math.max(0, idx - 160), idx + 160);
    assert.ok(open.includes('hidden'), 'panel-queues should carry the hidden attribute by default');
  });

  test('stat chips stay outside panels: #neo4j-status appears before #panel-overview (D-3)', () => {
    const statsIdx = html.indexOf('id="neo4j-status"');
    const panelIdx = html.indexOf('id="panel-overview"');
    assert.ok(statsIdx !== -1, 'neo4j-status must exist');
    assert.ok(panelIdx !== -1, 'panel-overview must exist');
    assert.ok(
      statsIdx < panelIdx,
      'neo4j-status (global stat chips) must appear before panel-overview so it shows on both tabs'
    );
  });

  test('Queues panel carries invariant + totals + dead-letter ids', () => {
    for (const id of ['invariant-card', 'invariant-eq', 'invariant-badge', 'totals-row', 'dead-letter-body']) {
      assert.ok(html.includes(`id="${id}"`), `Should have id="${id}"`);
    }
  });

  test('health-hint strip is present with hint ids', () => {
    assert.ok(html.includes('class="health-hint"'), 'Should have class="health-hint"');
    assert.ok(html.includes('id="hint-pill"'), 'Should have id="hint-pill"');
    assert.ok(html.includes('id="hint-inqueue"'), 'Should have id="hint-inqueue"');
    assert.ok(html.includes('id="hint-dead"'), 'Should have id="hint-dead"');
  });

  test('health-hint stays inside the Overview panel (between panel-overview and panel-queues)', () => {
    const overviewIdx = html.indexOf('id="panel-overview"');
    const hintIdx = html.indexOf('class="health-hint"');
    const queuesIdx = html.indexOf('id="panel-queues"');
    assert.ok(overviewIdx !== -1 && hintIdx !== -1 && queuesIdx !== -1, 'all three anchors must exist');
    assert.ok(
      overviewIdx < hintIdx && hintIdx < queuesIdx,
      'health-hint should live inside the Overview panel'
    );
  });

  test('health-hint is an accessible live region', () => {
    const idx = html.indexOf('class="health-hint"');
    assert.ok(idx !== -1, 'health-hint strip must exist');
    const open = html.slice(Math.max(0, idx - 120), idx + 120);
    assert.ok(
      /role="status"[^>]*aria-live="polite"|aria-live="polite"[^>]*role="status"/.test(open),
      'health-hint strip should have role="status" and aria-live="polite"'
    );
  });

  test('health-hint Queues affordance is a tab-switch button id="hint-go-queues" (not a link)', () => {
    assert.ok(html.includes('id="hint-go-queues"'), 'Should have a tab-switch button id="hint-go-queues"');
  });

  test('hint-pill span has no literal dot glyph (fix C: .pill::before draws it)', () => {
    const idx = html.indexOf('id="hint-pill"');
    assert.ok(idx !== -1, 'hint-pill must exist');
    const slice = html.slice(idx, idx + 80);
    assert.ok(!slice.includes('●'), 'hint-pill should not contain a literal ● glyph');
    assert.ok(!/\u25CF/i.test(slice), 'hint-pill should not contain a literal U+25CF glyph');
  });

  test('all four dashboard tables are wrapped in .table-scroll (D-2)', () => {
    const scrollCount = (html.match(/class="[^"]*\btable-scroll\b[^"]*"/g) || []).length;
    assert.ok(
      scrollCount >= 4,
      `Expected at least 4 .table-scroll wrappers (Active/Completed/Recent/Dead-letter), found ${scrollCount}`
    );
  });
});

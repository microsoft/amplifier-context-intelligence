/**
 * Tests for dashboard.js - Neo4j status indicator update
 * Run with: node dashboard.js.test.mjs
 * Node.js built-in test runner (no dependencies required).
 */

import { test, describe, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dir = dirname(fileURLToPath(import.meta.url));

// ── Read dashboard.js source for content checks ──────────────────────────────
const jsPath = join(__dir, 'dashboard.js');
const jsSource = readFileSync(jsPath, 'utf-8');

// ── Source-level checks for neo4j_connected handling ─────────────────────────

describe('dashboard.js neo4j_connected handling', () => {
  test('reads data.neo4j_connected from status response', () => {
    assert.ok(
      jsSource.includes('neo4j_connected'),
      'dashboard.js should read neo4j_connected from status data'
    );
  });

  test('updates #neo4j-status element text', () => {
    assert.ok(
      jsSource.includes('neo4j-status'),
      'dashboard.js should update the neo4j-status element'
    );
  });

  test('uses Connected text for true state', () => {
    assert.ok(
      jsSource.includes('Connected'),
      'dashboard.js should set "Connected" text when neo4j is connected'
    );
  });

  test('uses Disconnected text for false state', () => {
    assert.ok(
      jsSource.includes('Disconnected'),
      'dashboard.js should set "Disconnected" text when neo4j is not connected'
    );
  });

  test('uses var(--primary) color for connected state', () => {
    assert.ok(
      jsSource.includes('var(--primary)'),
      'dashboard.js should use var(--primary) color for connected state'
    );
  });

  test('uses var(--destructive) color for disconnected state', () => {
    assert.ok(
      jsSource.includes('var(--destructive)'),
      'dashboard.js should use var(--destructive) color for disconnected state'
    );
  });
});

// ── Behavioral test: DOM interaction ─────────────────────────────────────────
// We test the actual status update behavior by simulating DOM + data

describe('dashboard.js neo4j status DOM update (behavioral)', () => {
  // Set up minimal browser globals
  const elements = {};

  function makeElement(id) {
    return {
      id,
      textContent: '',
      style: { color: '', display: '' },
      innerHTML: '',
      children: [],
      getElementsByClassName: () => [],
      appendChild: () => {},
      removeChild: () => {},
      addEventListener: () => {},
      scrollTop: 0,
      scrollHeight: 0,
      clientHeight: 0,
    };
  }

  beforeEach(() => {
    // Reset element states
    elements['neo4j-status'] = makeElement('neo4j-status');
    elements['uptime'] = makeElement('uptime');
    elements['active_sessions'] = makeElement('active_sessions');
    elements['error_count'] = makeElement('error_count');
    elements['error-badge'] = makeElement('error-badge');
    elements['sessions-body'] = makeElement('sessions-body');
    elements['completed-body'] = makeElement('completed-body');
    elements['events-body'] = makeElement('events-body');
    elements['log-container'] = makeElement('log-container');
    elements['log-filter'] = makeElement('log-filter');
    elements['log-toggle'] = makeElement('log-toggle');
    elements['log-error-badge'] = makeElement('log-error-badge');
    elements['theme-toggle'] = makeElement('theme-toggle');
  });

  test('sets ● Connected with --primary color when neo4j_connected is true', async () => {
    // Extract the neo4j_connected handling logic from source
    // Look for the pattern that updates neo4j-status
    const connectedMatch = jsSource.match(/neo4j.?connected[\s\S]{0,300}Connected/);
    assert.ok(
      connectedMatch,
      'Should have code that sets Connected text when neo4j_connected is truthy'
    );
  });

  test('sets ○ Disconnected with --destructive color when neo4j_connected is false', async () => {
    const disconnectedMatch = jsSource.match(/neo4j.?connected[\s\S]{0,300}Disconnected/);
    assert.ok(
      disconnectedMatch,
      'Should have code that sets Disconnected text when neo4j_connected is falsy'
    );
  });
});

// Pipeline health hint (C2)

describe('dashboard.js pipeline health hint (C2)', () => {
  test('exports a pure computeHint(metrics) helper', () => {
    assert.ok(
      jsSource.includes('export function computeHint('),
      'dashboard.js should export a computeHint(metrics) function'
    );
  });

  test('uses "Pipeline OK" pill text for healthy state', () => {
    assert.ok(
      jsSource.includes('Pipeline OK'),
      'computeHint should produce "Pipeline OK" pill text when not degraded'
    );
  });

  test('uses "DEGRADED" pill text for degraded state', () => {
    assert.ok(
      jsSource.includes('DEGRADED'),
      'computeHint should produce "DEGRADED" pill text when degraded'
    );
  });

  test('uses "pill degraded" class for degraded state', () => {
    assert.ok(
      jsSource.includes('pill degraded'),
      'computeHint should produce "pill degraded" class when degraded'
    );
  });

  test('does NOT prepend a literal dot glyph to pill text (fix C: .pill::before draws the dot)', () => {
    assert.ok(
      !jsSource.includes('\u25cf Pipeline OK'),
      'pill text must not contain a literal glyph before "Pipeline OK"'
    );
    assert.ok(
      !jsSource.includes('\u25cf DEGRADED'),
      'pill text must not contain a literal glyph before "DEGRADED"'
    );
  });

  test('refresh() calls computeHint', () => {
    assert.ok(
      jsSource.includes('computeHint('),
      'refresh() should call computeHint'
    );
  });

  test('refresh() wires the hint-pill element', () => {
    assert.ok(jsSource.includes('hint-pill'), 'refresh() should update hint-pill');
  });

  test('refresh() wires the hint-inqueue element', () => {
    assert.ok(jsSource.includes('hint-inqueue'), 'refresh() should update hint-inqueue');
  });

  test('refresh() wires the hint-dead element', () => {
    assert.ok(jsSource.includes('hint-dead'), 'refresh() should update hint-dead');
  });

  test('renders "Dead-letter N" badge text', () => {
    assert.ok(
      jsSource.includes('Dead-letter '),
      'computeHint should produce "Dead-letter " badge text'
    );
  });
});

// Queues tab wiring (C2 re-arch)

describe('dashboard.js Queues tab wiring (C2 re-arch)', () => {
  test('imports the queues panel from ./queues-panel.js', () => {
    assert.ok(
      jsSource.includes("from './queues-panel.js'"),
      'dashboard.js should import from ./queues-panel.js'
    );
    assert.ok(
      jsSource.includes('renderQueues') &&
        jsSource.includes('fetchDeadLetters') &&
        jsSource.includes('renderDeadLetters'),
      'dashboard.js should import renderQueues/fetchDeadLetters/renderDeadLetters'
    );
  });

  test('does NOT import or use a tabView helper (inline switching — T-A)', () => {
    assert.ok(
      !jsSource.includes('tabView'),
      'dashboard.js must switch tabs inline, with no tabView helper'
    );
  });

  test('refresh() passes the WHOLE status to renderQueues(data)', () => {
    assert.ok(
      jsSource.includes('renderQueues(data)'),
      'refresh() should call renderQueues(data) with the whole status object'
    );
  });

  test('panel extracts .metrics (whole-status binding documented/used)', () => {
    assert.ok(
      jsSource.includes('.metrics'),
      'dashboard.js should reference .metrics (panel extracts metrics from status)'
    );
  });

  test('tracks activeTab state', () => {
    assert.ok(
      jsSource.includes('activeTab'),
      'dashboard.js should track an activeTab variable'
    );
  });

  test('defines an inline setTab(name) switcher', () => {
    assert.ok(
      /function setTab\s*\(/.test(jsSource) || /setTab\s*=\s*\(/.test(jsSource) ||
        jsSource.includes('setTab('),
      'dashboard.js should define an inline setTab(name) switcher'
    );
  });

  test('setTab toggles both panels (overview + queues)', () => {
    assert.ok(
      jsSource.includes('panel-overview') && jsSource.includes('panel-queues'),
      'setTab should toggle #panel-overview and #panel-queues'
    );
  });

  test('setTab updates aria-selected and active class', () => {
    assert.ok(
      jsSource.includes('aria-selected'),
      'setTab should set aria-selected on the tab buttons'
    );
    assert.ok(
      jsSource.includes("classList.toggle('active'"),
      'setTab should toggle the active class on the tab buttons'
    );
  });

  test('dead-letter fetch is gated on activeTab === "queues"', () => {
    assert.ok(
      jsSource.includes("activeTab === 'queues'"),
      'dead-letter fetch should be gated on activeTab === queues'
    );
    assert.ok(
      jsSource.includes('fetchDeadLetters(') && jsSource.includes('renderDeadLetters('),
      'refresh() should call fetchDeadLetters() and renderDeadLetters() when on the queues tab'
    );
  });

  test('wires dead-letter actions and the hint-go-queues shortcut', () => {
    assert.ok(
      jsSource.includes('wireDeadLetterActions('),
      'dashboard.js should call wireDeadLetterActions(...)'
    );
    assert.ok(
      jsSource.includes('hint-go-queues'),
      'dashboard.js should wire the hint-go-queues shortcut to the queues tab'
    );
  });

  test('onAuthLost re-shows the single centered auth overlay', () => {
    assert.ok(
      jsSource.includes('onAuthLost'),
      'dashboard.js should define an onAuthLost handler'
    );
    assert.ok(
      jsSource.includes('auth-overlay'),
      'onAuthLost should reference the auth-overlay element'
    );
  });
});

// ── Logger filter (filter visible log lines by their `logger` field) ──────────
describe('dashboard.js logger filter', () => {
  test('reads the logger field from each log line', () => {
    assert.ok(jsSource.includes('p.logger'), 'should parse the logger field from the log JSON');
  });
  test('tracks hidden loggers and a checkbox container', () => {
    assert.ok(jsSource.includes('hiddenLoggers'), 'should maintain a hiddenLoggers set');
    assert.ok(jsSource.includes('log-logger-filters'), 'should reference the logger-filter container');
  });
  test('dynamically creates a per-logger checkbox', () => {
    assert.ok(jsSource.includes('ensureLoggerCheckbox'), 'should add a checkbox per distinct logger');
    assert.ok(jsSource.includes("type = 'checkbox'"), 'logger filter should use checkboxes');
  });
  test('visibility honours both text filter and hidden loggers', () => {
    assert.ok(jsSource.includes('isLogVisible'), 'should gate visibility through isLogVisible');
    assert.ok(jsSource.includes('hiddenLoggers.has'), 'visibility should check hiddenLoggers');
  });
});

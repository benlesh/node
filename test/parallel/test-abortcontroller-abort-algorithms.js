// Flags: --expose-internals
'use strict';
const common = require('../common');
const assert = require('node:assert/strict');
const { addAbortAlgorithm, removeAbortAlgorithm } = require('internal/abort_controller');

// Keep intentional uncaught-exception reports outside node:test.
const errors = [new Error('source algorithm'), new Error('dependent algorithm')];
const reports = [];
process.on('uncaughtException', common.mustCall((error) => reports.push(error), 2));

const controller = new AbortController();
const source = controller.signal;
const dependent = AbortSignal.any([source]);
const transitive = AbortSignal.any([dependent]);
const log = [];
for (const [index, signal] of [source, dependent, transitive].entries()) {
  if (index < errors.length) {
    addAbortAlgorithm(signal, () => {
      log.push(['throw', index]);
      throw errors[index];
    });
  }
  const removed = common.mustNotCall();
  addAbortAlgorithm(signal, () => {
    log.push(['algorithm', index]);
    removeAbortAlgorithm(signal, removed);
  });
  addAbortAlgorithm(signal, removed);
  signal.addEventListener('abort', common.mustCall(() => log.push(['event', index])));
}

// Throwing algorithms must not escape abort() or interrupt its remaining steps.
controller.abort('reason');
assert.deepStrictEqual(log, [
  ['throw', 0], ['algorithm', 0], ['event', 0],
  ['throw', 1], ['algorithm', 1], ['event', 1],
  ['algorithm', 2], ['event', 2],
]);
assert.deepStrictEqual(reports, []);
for (const signal of [source, dependent, transitive]) {
  assert.strictEqual(signal.aborted, true);
  assert.strictEqual(signal.reason, 'reason');
  const key = Object.getOwnPropertySymbols(signal).find((key) => key.description === 'kAbortAlgorithms');
  assert.notStrictEqual(key, undefined);
  assert.strictEqual(signal[key], undefined);
  addAbortAlgorithm(signal, common.mustNotCall());
}
controller.abort();
process.on('exit', () => assert.deepStrictEqual(reports, errors));

// Flags: --experimental-observable
'use strict';
const common = require('../common');
const assert = require('node:assert/strict');

// Uncaught exceptions are tested outside node:test so its own error listener
// does not treat these intentional reports as failures.
const reports = [];
process.on(
  'uncaughtException',
  common.mustCall((reason) => reports.push(reason), 4),
);
const log = [];
let sink;
const source = new SharedObservable((s) => {
  sink = s;
  s.addTeardown(() => log.push(1));
  s.addTeardown(() => {
    log.push(2);
    throw new Error('cleanup');
  });
});
source.subscribe(() => {
  throw new Error('observer');
});
source.subscribe((value) => log.push(value));
sink.next(3);
sink.complete();
sink.addTeardown(() => log.push('late'));
sink.error(new Error('inactive'));
new Observable((s) => s.error(new Error('unhandled'))).subscribe();
assert.deepStrictEqual(log, [3, 2, 1, 'late']);
assert.deepStrictEqual(reports, []);
process.on('exit', () => {
  assert.deepStrictEqual(
    reports.map((reason) => reason.message),
    ['observer', 'cleanup', 'inactive', 'unhandled'],
  );
});

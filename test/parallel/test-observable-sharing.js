// Flags: --experimental-observable
'use strict';
require('../common');
const { test } = require('node:test');
const assert = require('node:assert/strict');

test('ordinary observations execute independently; share establishes one lazy boundary', () => {
  let starts = 0;
  let cleanups = 0;
  const source = new Observable((s) => {
    starts++;
    s.addTeardown(() => cleanups++);
  });
  const shared = source.share();
  assert.strictEqual(starts, 0);
  const a = new AbortController();
  const b = new AbortController();
  shared.subscribe({}, { signal: a.signal });
  shared.subscribe({}, { signal: b.signal });
  assert.strictEqual(starts, 1);
  assert.strictEqual(shared.activeSubscribers, 2);
  a.abort();
  assert.strictEqual(cleanups, 0);
  b.abort();
  assert.strictEqual(cleanups, 1);
  shared.subscribe();
  assert.strictEqual(starts, 2);
  source.subscribe();
  source.subscribe();
  assert.strictEqual(starts, 4);
});

test('joining during next receives the retained value exactly once', () => {
  const log = [];
  const source = new SharedObservable({
    maxReplayValues: 1,
    producer(s) {
      s.next(7);
      s.complete({ close: true });
    },
  });
  source.subscribe((value) => {
    log.push(`A:${value}`);
    source.subscribe((value) => log.push(`B:${value}`));
  });
  assert.deepStrictEqual(log, ['A:7', 'B:7']);
  assert.deepStrictEqual(source.replayValues, [7]);
  assert.strictEqual(source.activeSubscribers, 0);
});

test('take detaches and runs upstream teardown before its final next', () => {
  const log = [];
  new Observable((s) => {
    s.addTeardown(() => log.push('teardown'));
    s.next(1);
    s.next(2);
  })
    .take(1)
    .subscribe({
      next: (value) => log.push(value),
      complete: () => log.push('complete'),
    });
  assert.deepStrictEqual(log, ['teardown', 1, 'complete']);
});

test('shared input has per-observation operator state and declared ancestry', () => {
  let sink;
  const source = new SharedObservable((s) => {
    sink = s;
  });
  const view = source.take(2);
  const a = [];
  const b = [];
  view.subscribe((value) => a.push(value));
  sink.next(1);
  view.subscribe((value) => b.push(value));
  sink.next(2);
  sink.next(3);
  assert.deepStrictEqual(a, [1, 2]);
  assert.deepStrictEqual(b, [2, 3]);
  assert.deepStrictEqual(view.sharedSources, [source]);
  assert.strictEqual(view instanceof SharedObservable, false);
  assert.strictEqual(source instanceof Observable, true);
  assert.strictEqual(SharedObservable.from, undefined);
});

test('sharedSources stops at the nearest shared boundary across repeated sharing', () => {
  const source = new Observable(() => {});
  const fn = (value) => value;
  const a = source.share();
  const b = a.map(fn);
  const c = b.share();
  const d = c.filter(fn);

  assert.strictEqual(d.sharedSources[0], c);
  assert.strictEqual(d.sharedSources.length, 1);
  assert.strictEqual(c.sharedSources.length, 1);
  assert.strictEqual(c.sharedSources[0], a);
  assert.strictEqual(b.sharedSources.length, 1);
  assert.strictEqual(b.sharedSources[0], a);
});

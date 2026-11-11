// Flags: --experimental-observable
'use strict';
const common = require('../common');
const assert = require('node:assert/strict');
const { test } = require('node:test');
const vm = require('node:vm');
const { getEventListeners } = require('node:events');

test('Observable.from rejects primitive inputs with an input-specific diagnostic', () => {
  for (const value of [5, 'abc', null, undefined, true, 1n, Symbol('input')]) {
    assert.throws(() => Observable.from(value), {
      name: 'TypeError',
      code: 'ERR_INVALID_ARG_TYPE',
      message: 'Observable.from() requires an object',
    });
  }
});

test('conversion errors retain their intended diagnostics', () => {
  assert.throws(() => new Observable(5), {
    code: 'ERR_INVALID_ARG_TYPE',
    message: 'Expected a configuration object',
  });
  assert.throws(() => new Observable({ producer: 5 }), {
    code: 'ERR_INVALID_ARG_TYPE',
    message: 'Expected a callback',
  });
  assert.throws(() => new Observable(() => {}).share({
    closeOnLastUnsubscribe: true,
    keepAliveOnLastUnsubscribe: true,
  }), {
    code: 'ERR_INVALID_ARG_TYPE',
    message: 'Last-unsubscribe closure and keep-alive are mutually exclusive',
  });
  let error;
  Observable.from({ [Symbol.iterator]: () => ({ next: () => 5 }) }).subscribe({
    error: (reason) => { error = reason; },
  });
  assert.strictEqual(error?.code, 'ERR_INVALID_ARG_TYPE');
  assert.strictEqual(error.message, 'Iterator result must be an object');
});

test('abort algorithms run before earlier public listeners and resist propagation suppression', () => {
  const controller = new AbortController();
  const log = [];
  let sink;
  controller.signal.addEventListener('abort', common.mustCall((event) => {
    log.push('public');
    assert.strictEqual(sink.active, false);
    event.stopImmediatePropagation();
  }));
  new Observable((s) => {
    sink = s;
    s.addTeardown(() => log.push('cleanup'));
  }).subscribe({}, { signal: controller.signal });
  controller.abort('cancel');
  assert.deepStrictEqual(log, ['cleanup', 'public']);
  assert.strictEqual(sink.signal.reason, 'cancel');
});

test('an untrusted abort event does not cancel an observation', () => {
  const controller = new AbortController();
  let sink;
  const values = [];
  new Observable((s) => {
    sink = s;
  }).subscribe((v) => values.push(v), {
    signal: controller.signal,
  });
  controller.signal.dispatchEvent(new Event('abort'));
  sink.next(1);
  assert.strictEqual(sink.active, true);
  assert.deepStrictEqual(values, [1]);
  controller.abort();
  assert.strictEqual(sink.active, false);
});

test('abort algorithms follow composite signals and remove their observation', () => {
  const controller = new AbortController();
  const signal = AbortSignal.any([controller.signal]);
  let sink;
  const source = new SharedObservable((s) => {
    sink = s;
  });
  source.subscribe({}, { signal });
  signal.addEventListener('abort', (event) => event.stopImmediatePropagation());
  controller.abort('composite');
  assert.strictEqual(source.activeSubscribers, 0);
  assert.strictEqual(sink.active, false);
  assert.strictEqual(sink.signal.reason, 'composite');
});

test('timeout cancellation works without public listeners', async () => {
  let sink;
  new Observable((s) => {
    sink = s;
  }).subscribe({}, { signal: AbortSignal.timeout(1) });
  await new Promise((resolve) => setTimeout(resolve, 30));
  assert.strictEqual(sink.active, false);
  assert.strictEqual(sink.signal.reason.name, 'TimeoutError');
});

test('EventTarget.when is lazy, shares explicitly, and removes listeners synchronously', () => {
  const target = new EventTarget();
  const source = target.when('message');
  const a = new AbortController();
  const b = new AbortController();
  const c = new AbortController();
  assert.strictEqual(getEventListeners(target, 'message').length, 0);
  source.subscribe({}, { signal: a.signal });
  source.subscribe({}, { signal: b.signal });
  assert.strictEqual(getEventListeners(target, 'message').length, 2);
  a.abort();
  b.abort();
  assert.strictEqual(getEventListeners(target, 'message').length, 0);
  const shared = source.share();
  shared.subscribe({}, { signal: c.signal });
  shared.take(1).subscribe();
  assert.strictEqual(getEventListeners(target, 'message').length, 1);
  target.dispatchEvent(new Event('message'));
  assert.strictEqual(shared.activeSubscribers, 1);
  c.abort();
  assert.strictEqual(getEventListeners(target, 'message').length, 0);
});

test('EventTarget.when retains capture and passive behavior', () => {
  for (const passive of [false, true]) {
    const target = new EventTarget();
    target
      .when('message', { capture: true, passive })
      .take(1)
      .subscribe((event) => event.preventDefault());
    assert.strictEqual(target.dispatchEvent(new Event('message', { cancelable: true })), passive);
    assert.strictEqual(getEventListeners(target, 'message').length, 0);
  }
});

test('native Promise branding accepts foreign-realm Promises and rejects thenables', async () => {
  const promise = vm.runInNewContext('Promise.resolve(42)');
  const log = [];
  Observable.from(promise).subscribe((value) => log.push(value));
  await promise;
  assert.deepStrictEqual(log, [42]);
  assert.throws(() => Observable.from({ then() {} }), TypeError);
});

test('Web IDL descriptors, required arguments, and brands', () => {
  for (const [Type, name] of [
    [Observable, 'Observable'],
    [SharedObservable, 'SharedObservable'],
  ]) {
    assert.throws(() => Type(() => {}), TypeError);
    assert.strictEqual(Object.prototype.toString.call(new Type(() => {})), `[object ${name}]`);
  }
  assert.throws(() => new Subscriber(), TypeError);
  assert.strictEqual(Subscriber.length, 0);
  assert.strictEqual(Subscriber.prototype.error.length, 1);
  assert.strictEqual(Subscriber.prototype.complete.length, 0);
  assert.strictEqual(Observable.prototype.share.length, 0);
  assert.strictEqual(EventTarget.prototype.when.length, 1);
  const descriptor = Object.getOwnPropertyDescriptor(Observable.prototype, 'subscribe');
  assert.strictEqual(descriptor.enumerable, true);
  assert.strictEqual(descriptor.configurable, true);
  assert.strictEqual(descriptor.writable, true);
  assert.throws(() => Observable.prototype.subscribe.call({}), TypeError);
  assert.throws(
    () => SharedObservable.prototype.__lookupGetter__('replayValues').call({}),
    TypeError,
  );
  assert.throws(() => EventTarget.prototype.when.call({}, 'message'), TypeError);
  assert.throws(() => new EventTarget().when(), TypeError);
  let converted = false;
  assert.throws(
    () =>
      Observable.prototype.take.call(
        {},
        {
          valueOf() {
            converted = true;
            return 1;
          },
        },
      ),
    TypeError,
  );
  assert.strictEqual(converted, false);
});

test('internal callbacks ignore Object.prototype observer methods', () => {
  const descriptor = Object.getOwnPropertyDescriptor(Object.prototype, 'error');
  let called = false;
  Object.defineProperty(Object.prototype, 'error', {
    configurable: true,
    value() {
      called = true;
    },
  });
  try {
    const values = [];
    Observable.from([1, 2])
      .map((v) => v * 2)
      .subscribe((v) => values.push(v));
    assert.deepStrictEqual(values, [2, 4]);
    assert.strictEqual(called, false);
  } finally {
    if (descriptor) Object.defineProperty(Object.prototype, 'error', descriptor);
    else delete Object.prototype.error;
  }
});

test('internal composition ignores patched public Subscriber and EventTarget methods', () => {
  const savedNext = Subscriber.prototype.next;
  const savedAdd = EventTarget.prototype.addEventListener;
  const savedRemove = EventTarget.prototype.removeEventListener;
  const savedAbort = AbortController.prototype.abort;
  const savedSubscribe = Observable.prototype.subscribe;
  const target = new EventTarget();
  const stop = new AbortController();
  const values = [];
  const source = target
    .when('message')
    .map(() => 1)
    .take(1);
  const fail = () => {
    throw new Error('Public method was used internally');
  };
  try {
    Subscriber.prototype.next = fail;
    EventTarget.prototype.addEventListener = fail;
    EventTarget.prototype.removeEventListener = fail;
    AbortController.prototype.abort = fail;
    Observable.prototype.subscribe = fail;
    savedSubscribe.call(source, (value) => values.push(value), { signal: stop.signal });
    target.dispatchEvent(new Event('message'));
    assert.deepStrictEqual(values, [1]);
    assert.strictEqual(getEventListeners(target, 'message').length, 0);
  } finally {
    Subscriber.prototype.next = savedNext;
    EventTarget.prototype.addEventListener = savedAdd;
    EventTarget.prototype.removeEventListener = savedRemove;
    AbortController.prototype.abort = savedAbort;
    Observable.prototype.subscribe = savedSubscribe;
  }
});

test('sharing and composition do not use mutable collection prototypes', () => {
  const push = Array.prototype.push;
  const shift = Array.prototype.shift;
  const slice = Array.prototype.slice;
  const iterator = Array.prototype[Symbol.iterator];
  const values = [];
  const fail = () => { throw new Error('Mutable collection method used'); };
  try {
    Array.prototype.push = fail;
    Array.prototype.shift = fail;
    Array.prototype.slice = fail;
    Array.prototype[Symbol.iterator] = fail;
    const source = new Observable((s) => { s.next(1); s.next(2); s.complete(); });
    const shared = source.map((v) => v * 2).share({ maxReplayValues: 1, closeOnComplete: true });
    shared.subscribe((value) => push.call(values, value));
    shared.take(1).subscribe((value) => push.call(values, value));
  } finally {
    Array.prototype.push = push;
    Array.prototype.shift = shift;
    Array.prototype.slice = slice;
    Array.prototype[Symbol.iterator] = iterator;
  }
  assert.deepStrictEqual(values, [2, 4, 4]);
});

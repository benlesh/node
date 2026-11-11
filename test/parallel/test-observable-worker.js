// Flags: --experimental-observable
'use strict';
const common = require('../common');
const assert = require('node:assert/strict');
const { Worker, isMainThread, parentPort } = require('node:worker_threads');

if (isMainThread) {
  const worker = new Worker(__filename);
  worker.once('error', common.mustNotCall());
  worker.once('exit', common.mustCall((code) => assert.strictEqual(code, 0)));
  worker.once('message', common.mustCall((value) => {
    assert.deepStrictEqual(value, [2, 4]);
    worker.postMessage('done');
  }));
} else {
  assert.strictEqual(typeof Observable, 'function');
  assert.strictEqual(typeof SharedObservable, 'function');
  assert.strictEqual(typeof Subscriber, 'function');
  const result = [];
  const source = Observable.from([1, 2]).map((x) => x * 2).share({
    maxReplayValues: 2,
    closeOnComplete: true,
  });
  source.subscribe();
  source.subscribe((value) => result.push(value));
  // MessagePort inherits the Node EventTarget implementation.
  parentPort.when('message').take(1).subscribe(common.mustCall((event) => {
    assert.strictEqual(event.data, 'done');
    parentPort.close();
  }));
  parentPort.postMessage(result);
}

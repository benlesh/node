'use strict';
require('../common');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');

const script = `
  console.log(JSON.stringify([
    typeof Observable,
    typeof SharedObservable,
    typeof Subscriber,
    typeof EventTarget.prototype.when,
  ]));
`;
for (const [args, enabled] of [
  [[], false],
  [['--experimental-observable'], true],
  [['--experimental-observable', '--no-experimental-observable'], false],
  [['--no-experimental-observable', '--experimental-observable'], true],
]) {
  const result = spawnSync(process.execPath, [...args, '-e', script], {
    encoding: 'utf8',
    env: { ...process.env, NODE_OPTIONS: '' },
  });
  assert.strictEqual(result.status, 0, result.stderr);
  assert.deepStrictEqual(JSON.parse(result.stdout), Array(4).fill(enabled ? 'function' : 'undefined'));
}

const result = spawnSync(process.execPath, ['-e', script], {
  encoding: 'utf8',
  env: { ...process.env, NODE_OPTIONS: '--experimental-observable' },
});
assert.strictEqual(result.status, 0, result.stderr);
assert.deepStrictEqual(JSON.parse(result.stdout), Array(4).fill('function'));

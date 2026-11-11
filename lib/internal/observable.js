'use strict';

// https://github.com/WICG/observable/issues/221
const {
  ArrayFrom,
  ArrayPrototypeEvery,
  ArrayPrototypeIncludes,
  ArrayPrototypePush,
  ArrayPrototypeShift,
  ArrayPrototypeSlice,
  BigInt,
  BigIntAsUintN,
  Boolean,
  FunctionPrototypeCall,
  MathTrunc,
  Number,
  NumberIsFinite,
  NumberIsSafeInteger,
  ObjectDefineProperty,
  ObjectFreeze,
  ObjectGetOwnPropertyDescriptor,
  ObjectGetOwnPropertyDescriptors,
  ObjectKeys,
  ObjectSetPrototypeOf,
  PromisePrototypeThen,
  PromiseReject,
  PromiseResolve,
  ReflectApply,
  SafeArrayIterator,
  SafeSet,
  SafeWeakMap,
  SymbolAsyncIterator,
  SymbolIterator,
  SymbolToStringTag,
  TypeError,
  uncurryThis,
} = primordials;

const { nextTick } = require('process');
const {
  codes: {
    ERR_ILLEGAL_CONSTRUCTOR,
    ERR_INVALID_THIS,
    ERR_MISSING_ARGS,
    ERR_OUT_OF_RANGE,
  },
} = require('internal/errors');
const { setOwnProperty } = require('internal/util');
const { isPromise } = require('internal/util/types');
const {
  AbortController,
  AbortSignal,
  addAbortAlgorithm,
  removeAbortAlgorithm,
} = require('internal/abort_controller');
const { EventTarget, isEventTarget } = require('internal/event_target');

const controllerAbort = uncurryThis(AbortController.prototype.abort);
const controllerSignal = uncurryThis(
  ObjectGetOwnPropertyDescriptor(AbortController.prototype, 'signal').get,
);
const signalAborted = uncurryThis(
  ObjectGetOwnPropertyDescriptor(AbortSignal.prototype, 'aborted').get,
);
const signalReason = uncurryThis(
  ObjectGetOwnPropertyDescriptor(AbortSignal.prototype, 'reason').get,
);
const eventTargetAddEventListener = uncurryThis(EventTarget.prototype.addEventListener);
const eventTargetRemoveEventListener = uncurryThis(EventTarget.prototype.removeEventListener);
const configKeys = [
  'closeOnComplete',
  'closeOnError',
  'closeOnLastUnsubscribe',
  'keepAliveOnLastUnsubscribe',
  'maxReplayValues',
];

const observables = new SafeWeakMap();
const subscribers = new SafeWeakMap();
const token = { __proto__: null };
const defaults = ObjectFreeze({
  __proto__: null,
  maxReplayValues: 0,
  closeOnComplete: false,
  closeOnError: false,
  closeOnLastUnsubscribe: false,
  keepAliveOnLastUnsubscribe: false,
});

function makeException(message) {
  // Web IDL conversion failures are TypeErrors with Node's argument error code.
  // eslint-disable-next-line no-restricted-syntax
  const error = new TypeError(message);
  setOwnProperty(error, 'code', 'ERR_INVALID_ARG_TYPE');
  return error;
}

function state(value) {
  const result = observables.get(value);
  if (!result) throw new ERR_INVALID_THIS('Observable');
  return result;
}

function execution(value) {
  const result = subscribers.get(value);
  if (!result) throw new ERR_INVALID_THIS('Subscriber');
  return result;
}

function callable(value) {
  if (typeof value !== 'function') throw makeException('Expected a callback');
  return value;
}

function dictionary(value) {
  if (value == null) return { __proto__: null };
  if (typeof value !== 'object' && typeof value !== 'function') {
    throw makeException('Expected a configuration object');
  }
  return value;
}

function capacity(value) {
  const number = +value;
  if (!NumberIsSafeInteger(number) || number < 0) {
    throw new ERR_OUT_OF_RANGE('maxReplayValues', 'a nonnegative safe integer', number);
  }
  return number === 0 ? 0 : number;
}

function configuration(input, inherited = defaults) {
  const config = dictionary(input);
  const result = { __proto__: null };
  // Read each member once, including when share() returns its receiver.
  for (const key of new SafeArrayIterator(configKeys)) {
    const value = config[key];
    if (value === undefined) result[key] = inherited[key];
    else if (key === 'maxReplayValues') result[key] = capacity(value);
    else result[key] = Boolean(value);
  }
  if (result.closeOnLastUnsubscribe && result.keepAliveOnLastUnsubscribe) {
    throw makeException('Last-unsubscribe closure and keep-alive are mutually exclusive');
  }
  return result;
}

function declarations(input) {
  if (input == null) return null;
  const result = [];
  for (const source of input) {
    if (!observables.get(source)?.shared) throw makeException('Expected a SharedObservable');
    if (!ArrayPrototypeIncludes(result, source)) ArrayPrototypePush(result, source);
  }
  return result.length ? ObjectFreeze(result) : null;
}

function initialize(object, input, shared) {
  const config =
    typeof input === 'function' ? { __proto__: null, producer: input } : dictionary(input);
  const producer = callable(config.producer);
  const sources = declarations(config.sharedSources);
  observables.set(object, {
    __proto__: null,
    producer,
    sources,
    shared,
    config: shared ? configuration(config) : null,
    current: null,
    replay: [],
    replayHead: 0,
    terminal: null,
    subscriptions: new SafeSet(),
  });
}

function resetReplay(owner) {
  owner.replay = [];
  owner.replayHead = 0;
}

function replaySnapshot(owner) {
  const { replay, replayHead } = owner;
  if (replayHead === 0) return ArrayPrototypeSlice(replay);
  const result = [];
  for (let index = replayHead; index < replay.length; index++)
    ArrayPrototypePush(result, replay[index]);
  for (let index = 0; index < replayHead; index++)
    ArrayPrototypePush(result, replay[index]);
  return result;
}

function report(error) {
  // Match EventTarget's uncaught-exception reporting without consulting globals.
  nextTick(() => {
    throw error;
  });
}

function invoke(callback, args) {
  try {
    ReflectApply(callback, undefined, args);
  } catch (error) {
    report(error);
  }
}

function observerRecord(input = { __proto__: null }, keys = ['complete', 'error', 'next']) {
  if (typeof input === 'function') return { __proto__: null, next: input };
  const object = dictionary(input);
  const result = { __proto__: null };
  for (const key of new SafeArrayIterator(keys)) {
    const callback = object[key];
    if (callback !== undefined) result[key] = callable(callback);
  }
  return result;
}

function removeAbort(subscription) {
  if (subscription.signal) removeAbortAlgorithm(subscription.signal, subscription.abort);
}

function cleanup(exec, reason) {
  // The owner transition and notification guards are committed before script runs.
  exec.active = false;
  controllerAbort(exec.controller, reason);
  const callbacks = exec.teardowns;
  exec.teardowns = [];
  for (let index = callbacks.length - 1; index >= 0; index--) invoke(callbacks[index], []);
}

function notifyTerminal(subscription, kind, reason) {
  // Keep cancellation wired through teardown and earlier terminal callbacks.
  if (!subscription.canceled) {
    const callback = subscription.callbacks[kind];
    if (callback) invoke(callback, kind === 'error' ? [reason] : []);
    else if (kind === 'error') report(reason);
  }
  removeAbort(subscription);
}

function terminate(exec, kind, reason, close) {
  const owner = exec.owner;
  if (!exec.active || (owner.shared && owner.current !== exec)) {
    if (kind === 'error') report(reason);
    return;
  }
  const recipients = ArrayFrom(exec.subscriptions);
  if (owner.shared) {
    owner.current = null;
    if (close || owner.config[kind === 'error' ? 'closeOnError' : 'closeOnComplete']) {
      owner.terminal = { __proto__: null, kind, reason };
    } else resetReplay(owner);
  }
  exec.subscriptions.clear();
  cleanup(exec);
  for (const subscription of new SafeArrayIterator(recipients))
    notifyTerminal(subscription, kind, reason);
}

class Subscriber {
  constructor(key, exec) {
    if (key !== token) throw new ERR_ILLEGAL_CONSTRUCTOR();
    subscribers.set(this, exec);
  }
  get active() {
    return execution(this).active;
  }
  get signal() {
    return controllerSignal(execution(this).controller);
  }
  next(value) {
    const exec = execution(this);
    if (!arguments.length) throw new ERR_MISSING_ARGS('value');
    if (!exec.active) return;
    const owner = exec.owner;
    if (owner.shared && owner.config.maxReplayValues) {
      const { replay } = owner;
      const capacity = owner.config.maxReplayValues;
      // Grow lazily: the configured capacity may exceed the maximum array length.
      // Once full, replacing the oldest entry takes constant time.
      if (replay.length < capacity) ArrayPrototypePush(replay, value);
      else {
        replay[owner.replayHead] = value;
        owner.replayHead = (owner.replayHead + 1) % capacity;
      }
    }
    for (const subscription of new SafeArrayIterator(ArrayFrom(exec.subscriptions))) {
      if (!exec.active) break;
      if (exec.subscriptions.has(subscription) && subscription.callbacks.next)
        invoke(subscription.callbacks.next, [value]);
    }
  }
  error(reason, config) {
    const exec = execution(this);
    if (!arguments.length) throw new ERR_MISSING_ARGS('reason');
    const close = Boolean(dictionary(config).close);
    terminate(exec, 'error', reason, close);
  }
  complete(config) {
    const exec = execution(this);
    const close = Boolean(dictionary(config).close);
    terminate(exec, 'complete', undefined, close);
  }
  addTeardown(callback) {
    const exec = execution(this);
    callable(callback);
    if (exec.active) ArrayPrototypePush(exec.teardowns, callback);
    else invoke(callback, []);
  }
}

function subscribe(source, callbacks, options) {
  const owner = state(source);
  const signal = dictionary(options).signal;
  if (signal !== undefined && signalAborted(signal)) return;
  const terminal = owner.terminal;
  let exec = owner.shared ? owner.current : null;
  const start = !terminal && !exec;
  if (start) {
    exec = {
      __proto__: null,
      owner,
      active: true,
      controller: new AbortController(),
      teardowns: [],
      subscriptions: new SafeSet(),
    };
    if (owner.shared) {
      owner.current = exec;
      // One set serves delivery and activeSubscribers. A new execution gets
      // a fresh set so old cancellation callbacks cannot affect it.
      owner.subscriptions = exec.subscriptions;
    }
  }
  const subscriptions = terminal ? owner.subscriptions : exec.subscriptions;
  // Terminal callbacks run after detachment and must still honor cancellation.
  const subscription = { __proto__: null, callbacks, signal, canceled: false };
  subscription.abort = () => {
    if (subscription.canceled) return;
    subscription.canceled = true;
    subscriptions.delete(subscription);
    removeAbort(subscription);
    if (!exec?.active || subscriptions.size) return;
    if (owner.shared) {
      if (owner.config.keepAliveOnLastUnsubscribe) return;
      owner.current = null;
      if (owner.config.closeOnLastUnsubscribe)
        owner.terminal = { __proto__: null, kind: 'complete' };
      else resetReplay(owner);
    }
    cleanup(exec, signal === undefined ? undefined : signalReason(signal));
  };
  subscriptions.add(subscription);
  if (signal) addAbortAlgorithm(signal, subscription.abort);
  if (owner.shared) {
    for (const value of new SafeArrayIterator(replaySnapshot(owner))) {
      if (!subscriptions.has(subscription)) break;
      if (callbacks.next) invoke(callbacks.next, [value]);
    }
  }
  if (terminal) {
    subscriptions.delete(subscription);
    notifyTerminal(subscription, terminal.kind, terminal.reason);
  } else if (start && exec.active) {
    const sink = new Subscriber(token, exec);
    try {
      FunctionPrototypeCall(owner.producer, undefined, sink);
    } catch (error) {
      subscriberError(sink, error);
    }
  }
}

function sourcesOf(...inputs) {
  const result = [];
  for (const input of new SafeArrayIterator(inputs)) {
    const data = state(input);
    const sources = data.shared ? [input] : data.sources;
    if (sources === null) continue;
    for (const source of new SafeArrayIterator(sources)) {
      if (!ArrayPrototypeIncludes(result, source)) ArrayPrototypePush(result, source);
    }
  }
  return result.length ? ObjectFreeze(result) : null;
}

function forward(sink) {
  return {
    __proto__: null,
    next: (value) => subscriberNext(sink, value),
    error: (error) => subscriberError(sink, error),
    complete: () => subscriberComplete(sink),
  };
}

function follow(source, sink, callbacks = forward(sink), signal = subscriberSignal(sink)) {
  subscribe(source, callbacks, { __proto__: null, signal });
}

function controllerFor(sink) {
  const controller = new AbortController();
  const signal = controllerSignal(controller);
  const parent = subscriberSignal(sink);
  const abort = () => controllerAbort(controller, signalReason(parent));
  if (signalAborted(parent)) abort();
  else {
    addAbortAlgorithm(parent, abort);
    addAbortAlgorithm(signal, () => removeAbortAlgorithm(parent, abort));
  }
  return controller;
}

// Preserve the existing Web IDL unsigned long long conversion for take/drop.
function unsignedLongLong(value) {
  const number = +value;
  if (!NumberIsFinite(number) || number === 0) return 0;
  return Number(BigIntAsUintN(64, BigInt(MathTrunc(number))));
}

class SharedObservable {
  constructor(input) {
    initialize(this, input, true);
  }
  get activeSubscribers() {
    return sharedState(this).subscriptions.size;
  }
  get replayValues() {
    return ObjectFreeze(replaySnapshot(sharedState(this)));
  }
  get maxReplayValues() {
    return sharedState(this).config.maxReplayValues;
  }
}

class Observable {
  constructor(input) {
    initialize(this, input, false);
  }
  static from(input) {
    return from(input);
  }
  get sharedSources() {
    return state(this).sources;
  }
  subscribe(observer = { __proto__: null }, options = { __proto__: null }) {
    state(this);
    subscribe(this, observerRecord(observer), options);
  }
  share(config) {
    const owner = state(this);
    const normalized = configuration(config, owner.config ?? defaults);
    if (
      owner.shared &&
      ArrayPrototypeEvery(configKeys, (key) => normalized[key] === owner.config[key])
    )
      return this;
    const source = this;
    const result = new SharedObservable((sink) => follow(source, sink));
    const data = state(result);
    data.config = normalized;
    data.sources = sourcesOf(source);
    return result;
  }
  map(mapper) {
    state(this);
    callable(mapper);
    const source = this;
    return derived(source, (sink) => {
      let index = 0;
      follow(source, sink, {
        __proto__: null,
        ...forward(sink),
        next(value) {
          let mapped;
          try {
            mapped = mapper(value, index++);
          } catch (error) {
            subscriberError(sink, error);
            return;
          }
          subscriberNext(sink, mapped);
        },
      });
    });
  }
  filter(predicate) {
    state(this);
    callable(predicate);
    const source = this;
    return derived(source, (sink) => {
      let index = 0;
      follow(source, sink, {
        __proto__: null,
        ...forward(sink),
        next(value) {
          let matches;
          try {
            matches = predicate(value, index++);
          } catch (error) {
            subscriberError(sink, error);
            return;
          }
          if (matches) subscriberNext(sink, value);
        },
      });
    });
  }
  take(amount) {
    state(this);
    if (!arguments.length) throw new ERR_MISSING_ARGS('amount');
    amount = unsignedLongLong(amount);
    const source = this;
    return derived(source, (sink) => {
      let remaining = amount;
      if (!remaining) {
        subscriberComplete(sink);
        return;
      }
      const upstream = controllerFor(sink);
      follow(
        source,
        sink,
        {
          __proto__: null,
          ...forward(sink),
          next(value) {
            if (!subscriberActive(sink) || !remaining) return;
            const last = --remaining === 0;
            if (last) controllerAbort(upstream);
            subscriberNext(sink, value);
            if (last) subscriberComplete(sink);
          },
        },
        controllerSignal(upstream),
      );
    });
  }
  drop(amount) {
    state(this);
    if (!arguments.length) throw new ERR_MISSING_ARGS('amount');
    amount = unsignedLongLong(amount);
    const source = this;
    return derived(source, (sink) => {
      let remaining = amount;
      follow(source, sink, {
        __proto__: null,
        ...forward(sink),
        next(value) {
          if (remaining) remaining--;
          else subscriberNext(sink, value);
        },
      });
    });
  }
  takeUntil(input) {
    state(this);
    const source = this;
    const notifier = from(input);
    return derived(
      source,
      (sink) => {
        follow(notifier, sink, {
          __proto__: null,
          next: () => subscriberComplete(sink),
          error: () => subscriberComplete(sink),
        });
        if (subscriberActive(sink)) follow(source, sink);
      },
      notifier,
    );
  }
  flatMap(mapper) {
    state(this);
    callable(mapper);
    const source = this;
    return derived(source, (sink) => {
      let index = 0;
      let active = false;
      let outerDone = false;
      const queue = [];
      subscriberAddTeardown(sink, () => {
        queue.length = 0;
      });
      function drain() {
        if (!subscriberActive(sink) || active) return;
        if (!queue.length) {
          if (outerDone) subscriberComplete(sink);
          return;
        }
        active = true;
        let inner;
        try {
          const input = mapper(ArrayPrototypeShift(queue), index++);
          if (!subscriberActive(sink)) return;
          inner = from(input);
        } catch (error) {
          subscriberError(sink, error);
          return;
        }
        if (!subscriberActive(sink)) return;
        follow(inner, sink, {
          __proto__: null,
          ...forward(sink),
          complete() {
            active = false;
            drain();
          },
        });
      }
      follow(source, sink, {
        __proto__: null,
        ...forward(sink),
        next(value) {
          ArrayPrototypePush(queue, value);
          drain();
        },
        complete() {
          outerDone = true;
          drain();
        },
      });
    });
  }
  switchMap(mapper) {
    state(this);
    callable(mapper);
    const source = this;
    return derived(source, (sink) => {
      let index = 0;
      let current = null;
      let outerDone = false;
      follow(source, sink, {
        __proto__: null,
        ...forward(sink),
        next(value) {
          const previous = current;
          const next = controllerFor(sink);
          // Publish replacement identity before old cleanup can reenter.
          current = next;
          previous && controllerAbort(previous);
          if (!subscriberActive(sink) || current !== next) return;
          let inner;
          try {
            const input = mapper(value, index++);
            if (!subscriberActive(sink) || current !== next) return;
            inner = from(input);
          } catch (error) {
            subscriberError(sink, error);
            return;
          }
          if (!subscriberActive(sink) || current !== next) return;
          follow(
            inner,
            sink,
            {
              __proto__: null,
              ...forward(sink),
              complete() {
                if (current !== next) return;
                current = null;
                controllerAbort(next);
                if (outerDone) subscriberComplete(sink);
              },
            },
            controllerSignal(next),
          );
        },
        complete() {
          outerDone = true;
          if (!current) subscriberComplete(sink);
        },
      });
    });
  }
  inspect(input = { __proto__: null }) {
    state(this);
    const callbacks = observerRecord(input, ['abort', 'complete', 'error', 'next', 'subscribe']);
    const source = this;
    return derived(source, (sink) => {
      let terminal = false;
      try {
        if (callbacks.subscribe) FunctionPrototypeCall(callbacks.subscribe, undefined);
      } catch (error) {
        subscriberError(sink, error);
        return;
      }
      if (!subscriberActive(sink)) return;
      addAbortAlgorithm(subscriberSignal(sink), () => {
        if (!terminal && callbacks.abort)
          invoke(callbacks.abort, [signalReason(subscriberSignal(sink))]);
      });
      function notify(kind, args) {
        if (kind !== 'next') terminal = true;
        try {
          if (callbacks[kind]) ReflectApply(callbacks[kind], undefined, args);
        } catch (error) {
          terminal = true;
          subscriberError(sink, error);
          return;
        }
        ReflectApply(subscriberMethods[kind], sink, args);
      }
      follow(source, sink, {
        __proto__: null,
        next: (value) => notify('next', [value]),
        error: (reason) => notify('error', [reason]),
        complete: () => notify('complete', []),
      });
    });
  }
  catch(callback) {
    state(this);
    callable(callback);
    const source = this;
    return derived(source, (sink) => {
      follow(source, sink, {
        __proto__: null,
        ...forward(sink),
        error(reason) {
          let replacement;
          try {
            const input = callback(reason);
            if (!subscriberActive(sink)) return;
            replacement = from(input);
          } catch (error) {
            subscriberError(sink, error);
            return;
          }
          if (subscriberActive(sink)) follow(replacement, sink);
        },
      });
    });
  }
  finally(callback) {
    state(this);
    callable(callback);
    const source = this;
    return derived(source, (sink) => {
      subscriberAddTeardown(sink, callback);
      follow(source, sink);
    });
  }
}

function derived(source, producer, ...others) {
  const result = new Observable(producer);
  state(result).sources = sourcesOf(source, ...new SafeArrayIterator(others));
  return result;
}

function sharedState(value) {
  const result = state(value);
  if (!result.shared) throw makeException('Expected a SharedObservable');
  return result;
}

// Instance inheritance only: SharedObservable has no static from() method.
ObjectSetPrototypeOf(SharedObservable.prototype, Observable.prototype);

function getMethod(input, key) {
  const method = input[key];
  return method == null ? undefined : callable(method);
}

function objectResult(value) {
  if (value === null || (typeof value !== 'object' && typeof value !== 'function')) {
    throw makeException('Iterator result must be an object');
  }
  return value;
}

function from(input) {
  if (input === null || (typeof input !== 'object' && typeof input !== 'function')) {
    throw makeException('Observable.from() requires an object');
  }
  if (observables.has(input)) return input;
  const asyncMethod = getMethod(input, SymbolAsyncIterator);
  const method = asyncMethod ?? getMethod(input, SymbolIterator);
  if (method) {
    const async = asyncMethod !== undefined;
    return new Observable((sink) => {
      // Reacquire the iterator on every subscription, including its method getter.
      const iterator = objectResult(
        FunctionPrototypeCall(
          getMethod(input, async ? SymbolAsyncIterator : SymbolIterator),
          input,
        ),
      );
      const next = callable(iterator.next);
      let done = false;
      subscriberAddTeardown(sink, () => {
        if (done) return;
        done = true;
        const close = getMethod(iterator, 'return');
        if (!close) return;
        const result = FunctionPrototypeCall(close, iterator);
        if (async)
          PromisePrototypeThen(
            PromisePrototypeThen(PromiseResolve(result), objectResult),
            undefined,
            report,
          );
        else objectResult(result);
      });
      function accept(result) {
        objectResult(result);
        if (result.done) {
          done = true;
          subscriberComplete(sink);
          return;
        }
        subscriberNext(sink, result.value);
      }
      if (async) {
        function advance() {
          if (!subscriberActive(sink)) return;
          let result;
          try {
            result = FunctionPrototypeCall(next, iterator);
          } catch (error) {
            result = PromiseReject(error);
          }
          PromisePrototypeThen(
            PromiseResolve(result),
            (value) => {
              if (!subscriberActive(sink)) return;
              try {
                accept(value);
              } catch (error) {
                done = true;
                subscriberError(sink, error);
                return;
              }
              advance();
            },
            (error) => {
              done = true;
              subscriberError(sink, error);
            },
          );
        }
        advance();
      } else {
        while (subscriberActive(sink)) {
          try {
            accept(FunctionPrototypeCall(next, iterator));
          } catch (error) {
            done = true;
            subscriberError(sink, error);
          }
        }
      }
    });
  }
  // Native branding accepts Promises from other vm contexts, but not thenables.
  if (isPromise(input)) {
    return new Observable((sink) => {
      PromisePrototypeThen(
        input,
        (value) => {
          subscriberNext(sink, value);
          subscriberComplete(sink);
        },
        (error) => subscriberError(sink, error),
      );
    });
  }
  throw makeException('Expected an Observable, async iterable, iterable, or Promise');
}

function when(type, options = { __proto__: null }) {
  if (!arguments.length) throw new ERR_MISSING_ARGS('type');
  const target = this;
  if (!isEventTarget(target)) throw new ERR_INVALID_THIS('EventTarget');
  type = `${type}`;
  const input = dictionary(options);
  const config = { __proto__: null, capture: Boolean(input.capture) };
  const passive = input.passive;
  if (passive !== undefined) config.passive = Boolean(passive);
  return new Observable((sink) => {
    const listener = (event) => subscriberNext(sink, event);
    eventTargetAddEventListener(target, type, listener, config);
    subscriberAddTeardown(sink, () =>
      eventTargetRemoveEventListener(target, type, listener, config.capture),
    );
  });
}

// Native composition must not dispatch through mutable public methods.
const subscriberNext = uncurryThis(Subscriber.prototype.next);
const subscriberError = uncurryThis(Subscriber.prototype.error);
const subscriberComplete = uncurryThis(Subscriber.prototype.complete);
const subscriberAddTeardown = uncurryThis(Subscriber.prototype.addTeardown);
const subscriberActive = uncurryThis(
  ObjectGetOwnPropertyDescriptor(Subscriber.prototype, 'active').get,
);
const subscriberSignal = uncurryThis(
  ObjectGetOwnPropertyDescriptor(Subscriber.prototype, 'signal').get,
);
const subscriberMethods = {
  __proto__: null,
  next: Subscriber.prototype.next,
  error: Subscriber.prototype.error,
  complete: Subscriber.prototype.complete,
};

for (const ctor of new SafeArrayIterator([Observable, SharedObservable, Subscriber])) {
  const descriptors = ObjectGetOwnPropertyDescriptors(ctor.prototype);
  for (const key of new SafeArrayIterator(ObjectKeys(descriptors))) {
    if (key !== 'constructor') {
      ObjectDefineProperty(ctor.prototype, key, { __proto__: null, enumerable: true });
    }
  }
  ObjectDefineProperty(ctor.prototype, SymbolToStringTag, {
    __proto__: null,
    configurable: true,
    value: ctor.name,
  });
}
ObjectDefineProperty(Observable, 'from', { __proto__: null, enumerable: true });
ObjectDefineProperty(Subscriber, 'length', { __proto__: null, value: 0 });
ObjectDefineProperty(Subscriber.prototype.error, 'length', { __proto__: null, value: 1 });
ObjectDefineProperty(Subscriber.prototype.complete, 'length', { __proto__: null, value: 0 });
ObjectDefineProperty(Observable.prototype.share, 'length', { __proto__: null, value: 0 });

module.exports = { __proto__: null, Observable, SharedObservable, Subscriber, when };

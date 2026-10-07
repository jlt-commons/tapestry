# Tapestry

[![Build Status](https://github.com/jolt-lang/tapestry/actions/workflows/ci.yml/badge.svg)](https://github.com/jolt-lang/tapestry/actions)

Structured concurrency primitives for Clojure running on
[Jolt](https://github.com/jolt-lang/jolt) (Clojure on Chez Scheme — no JVM).

## About

Tapestry models a unit of concurrent work as a `fiber`: a derefable handle whose
body runs on its own thread. Results, errors, timeouts, and cancellation all
flow through that handle. The concurrency substrate is
`clojure.core.async`.

Each fiber runs on its own daemon thread. `interrupt!` and `timeout!` settle the
fiber's result and interrupt that thread, so a body blocked in `Thread/sleep`,
a deref, a `join`, or another interruptible wait throws `InterruptedException`
and stops.

Cancellation surfaces as `clojure.lang.ExceptionInfo` carrying `{:type
:tapestry.core/interrupted}` or `{:type :tapestry.core/timeout}`.

### Project State

Tapestry is pre-1.0. This fork runs on Jolt and replaces the former
manifold/`java.util.concurrent` substrate with `core.async`.

## Installation

Requires the [jolt](https://jolt-lang.github.io/) binary, version 0.8.16 or
later. Add to your deps.edn:

```
jolt-lang/tapestry {:git/url "https://github.com/jolt-lang/tapestry"
                    :git/sha "..."}
```

## Usage

#### Creating Fibers
```clojure
(require '[tapestry.core :refer [fiber]])

;; Fibers are derefable like futures
@(fiber (Thread/sleep 1000) :done)
;; => :done, after 1s

;; Or multiple derefs, deref is non-blocking once realized
(let [f (fiber (+ 1 2 3 4))]
  @f ;; => 10
  @f ;; => 10
  )

;; Deref's with timeouts are supported
(let [f (fiber (Thread/sleep 10000))]
  (deref f 100 :timed-out))
;; => :timed-out, after 100ms

;; Or, Like `core.async`'s `go-loop'

@(fiber-loop [i 0]
   (if (= i 5)
     (* 2 i)
     (do (Thread/sleep 100)
         (recur (inc i)))))
;; => 10, after aprox 500ms of sleeping
```

#### Interrupting and introspecting a Fiber
```clojure
(require '[tapestry.core :refer [fiber interrupt! alive? errored?]])

(let [f (fiber (Thread/sleep 10000))]
  (alive? f) ;; true
  (interrupt! f) ;; the sleep throws InterruptedException
  (errored? f) ;; true — the fiber's result was cancelled
  @f ;; throws ExceptionInfo {:type :tapestry.core/interrupted}
  (alive? f) ;; false once the body has unwound
  )
```

#### Timeouts

Tapestry supports setting timeouts on fibers which will cause them to be
cancelled when the timeout is hit.

```clojure
(require '[tapestry.core :refer [fiber timeout!]])

(let [f (fiber (Thread/sleep 10000))]
  (timeout! f 100)
  @f) ;; throws ExceptionInfo {:type :tapestry.core/timeout} after 100ms
```

You can also specify a default value

```clojure
(require '[tapestry.core :refer [fiber timeout!]])

(let [f (fiber (Thread/sleep 10000))]
  (timeout! f 100 :default)
  @f) ;; => :default
```

You can use dynamic bindings to set a timeout on a bunch of fibers. Note that
each fiber will have a timeout that starts from when the fiber was spawned.

```clojure
(require '[tapestry.core :refer [fiber with-timeout]])

(with-timeout 100 ;; Accepts a duration or number of millis
  (let [f (fiber (Thread/sleep 10000))]
    @f)) ;; throws ExceptionInfo {:type :tapestry.core/timeout}
```

#### Processing Sequences
```clojure
(require '[tapestry.core :refer [parallelly asyncly pfor]])

(def urls
  ["https://google.com"
   "https://teknql.tech/"])

;; Realize a seq in parallel, in whatever order the results come back
(asyncly #(slurp %) urls)

;; Same, but preserving order
(parallelly #(slurp %) urls)

;; `for` comprehension on steroids
(pfor [url urls]
  (slurp url))
```

#### Concurrency limiting

```clojure
(require '[tapestry.core :refer [fiber with-max-parallelism]])

;; Fibers spawned within `with-max-parallelism` are gated on a shared
;; semaphore; at most 3 bodies run at a time.
(with-max-parallelism 3
  (let [order-a-summary (fiber (process-order! order-a))
        order-b-summary (fiber (process-order! order-b))
        order-c-summary (fiber (process-order! order-c))
        order-d-summary (fiber (process-order! order-d))]
    {:a @order-a-summary
     :b @order-b-summary
     :c @order-c-summary
     :d @order-d-summary}))

;; You can also bound the parallelism of sequence processing functions by
;; specifying an optional bound:

(asyncly 3 slurp urls)

(parallelly 3 slurp urls)
```

#### Streams (channels)

`asyncly` and `parallelly` also accept `core.async` channels, allowing you to
describe parallel execution pipelines. Over a channel they return a channel;
`parallelly` emits results in input order as they finish, `asyncly` in
completion order. A channel can't carry an exception, so in stream mode the
first error goes to the stream error handler (see `set-stream-error-handler!`),
the in-flight calls are interrupted, and the result channel closes. Over a seq,
the error is thrown instead. `periodically` returns a channel that emits `(f)`
every `period`; close it to stop.

```clojure
(require '[clojure.core.async :as a]
         '[tapestry.core :as tapestry])

(let [count     (atom 0)
      generator (tapestry/periodically 1000 #(swap! count inc))]
  (a/go-loop []
    (when-some [v (a/<! generator)]
      (println "Count is now:" v)
      (recur)))
  (Thread/sleep 5000)
  (a/close! generator))

(a/<!!
 (a/into []
   (tapestry/asyncly 5 some-operation (a/to-chan [1 2 3]))))
```

#### Working with Agents

```clojure
(let [counter (agent 0)]
  (tapestry.core/send counter inc)
  (await counter)
  @counter)
;; => 1
```

## Experimental Features

> **Note:** The following APIs are experimental and may evolve more rapidly than
> the stable `tapestry.core` API.

#### Structured Scopes

Scopes provide structured concurrency for tapestry fibers — unifying lifecycle
management, shutdown policies, timeouts, and parallelism control into a single
construct.

Any fibers spawned with `tapestry.core/fiber` inside a scope are automatically
registered. On scope exit, all fibers are awaited, and shutdown policies are
enforced.

```clojure
(require '[tapestry.experimental.scope :refer [with-scope]]
         '[tapestry.core :refer [fiber]])

;; Basic scope — awaits all fibers before returning
(with-scope {}
  (let [a (fiber (fetch-user id))
        b (fiber (fetch-orders id))]
    {:user @a :orders @b}))

;; Shutdown on failure — if any fiber throws, siblings are interrupted
;; and the error propagates from the scope
(with-scope {:shutdown :on-failure}
  (let [user   (fiber (fetch-user id))
         orders (fiber (fetch-orders id))]
    {:user @user :orders @orders}))

;; Combined options
(with-scope {:shutdown        :on-failure
             :timeout         5000
             :max-parallelism 4}
  (let [a (fiber (do-a))
        b (fiber (do-b))]
    {:a @a :b @b}))
```

Scopes nest naturally — inner scope fibers are tracked by the inner scope only:

```clojure
(with-scope {:shutdown :on-failure}
  (let [a (fiber
            (with-scope {:shutdown :on-failure}
              (let [x (fiber (subtask-1))
                    y (fiber (subtask-2))]
                (+ @x @y))))
        b (fiber (other-work))]
    {:a @a :b @b}))
```

#### alts

```clojure
(require '[tapestry.experimental :refer [alts]])

;; Runs all expressions in parallel, returning the first successful
;; result. Remaining fibers are interrupted. If every operation fails,
;; the first error is thrown.
(alts
  (do (Thread/sleep 100)
      :first)
  (do (Thread/sleep 10)
      :second))
;; => :second after 10ms

;; With options — throws the timeout error if nothing succeeds in time
(alts {:timeout 3000}
  (fetch-from-primary)
  (fetch-from-replica))
```

#### Queues

Blocking queues over `clojure.core.async` with the notion of `closing` ala
`manifold` and `core.async`. Synchronous (rendezvous) by default; pass a
capacity for a bounded queue or `:unbounded`.

```clojure
(require '[tapestry.queue :as q]
         '[tapestry.core :refer [fiber alive?]])

;; By default a queue has no buffer
(let [q     (q/queue)
      take* (fiber (q/take! q))]
  (alive? take*)
  (q/try-take! q) ;; => nil, no value available
  (q/put! q :value) ;; => true, value put successfuly
  @take* ;; => :value, resolved in the fiber above
  (q/close! q) ;; Closing a queue will make it so no further items are accepted,
               ;; but previously queued items will be delivered via `take!`
  (q/put! q :value) ;; Returns false
  )

;; Bounded queue of capacity 2
(let [q (q/queue 2)]
  (q/put! q :a)
  (q/put! q :b)
  (q/try-put! q :c 0) ;; => false, full
  (q/take! q)) ;; => :a

;; Snapshot of the current contents (approximate under concurrency)
(q/items q)
```

## Advisories

- Interruption stops a body only at an interruptible wait. A body spinning on
  the CPU, or blocked where the JVM wouldn't interrupt either (entering a
  `locking` monitor), runs until it reaches one, and its result is reported as
  cancelled in the meantime. A scope waits for such bodies on exit, so check
  `(Thread/interrupted)` in long loops.
- `with-max-parallelism` and `with-scope :max-parallelism` require a positive
  integer; other values throw at scope entry.

## CLJ Kondo Config

Add the following to your `.clj-kondo/config.edn`

```clojure
{:lint-as {tapestry.core/fiber-loop clojure.core/loop
           tapestry.core/pfor       clojure.core/for}}
```

## Long Term Wish List

- [x] Drop manifold — the substrate is now `core.async`.
- [x] Implement structured concurrency (`tapestry.experimental.scope`).
- [ ] Consider implement linking ala erlang
- [ ] Consider implementing an OTP-like interface
- [ ] `(parallelize ...)` macro to automatically re-write call graphs

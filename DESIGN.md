# Ractor::Wrapper — Design Document

This document describes how `Ractor::Wrapper` is implemented. It is intended
for three audiences:

 -  **Power users** who want to understand how the gem behaves well enough to
    make informed decisions about incorporating it into a system.
 -  **Contributors** who need to understand the internals before changing them.
 -  **Future maintainers** who want a permanent record of the architectural
    decisions baked into the library.

The `README.md` focuses on how to _use_ the gem. In contrast, this document
focuses on how it _works_ and _why_ it was built that way.

Note: This document was largely reverse-engineered from the code and its
reference documentation by Claude Opus 4.7, with some human edits.

---

## 1. Problem statement

A Ractor-shareable object is one that can be referenced from more than one
Ractor at once. The current Ruby rules (as of 4.0) require shareable objects
to be deeply immutable, which rules out a large fraction of Ruby objects
that Ruby programs actually need to share. Examples include `Net::HTTP`
sessions, database connections, file handles, cache objects, and parsers
with internal state.

The canonical Ractor answer is "don't share it; write an actor." But writing
an actor from scratch means hand-crafting a message loop, a serialization
convention, block handling, lifecycle, error propagation, and everything
else. It also means rewriting every existing library in that style.

`Ractor::Wrapper` provides this plumbing once, so that any ordinary Ruby
object can be exposed to other Ractors through a shareable stub that
proxies calls back to a single controlled home. This allows both legacy
non-shareable objects to be used in a multi-Ractor environment, and new
Ractor-aware objects to be written easily.

The core design goal is to reproduce, as faithfully as reasonable, the
semantics of calling the object directly, including arbitrary arguments and
return values, keyword arguments, blocks, exceptions, and even re-entrant calls
from blocks. The gem deliberately accepts some limitations in service of that
goal, and those limitations are discussed throughout this document.

---

## 2. High-level structure

The entire library is in `lib/ractor/wrapper.rb`. The classes involved are:

 -  **`Ractor::Wrapper`** — the Ractor-shareable public-facing handle. Holds
    the `Configuration`, a `Stub`, a `Ractor::Port` to send messages to the
    server. It also provides the `call` method that provides the central
    interface to invoke methods on the wrapped object, the main caller-side
    message loop and block-yield handler, and wrapper lifecycle operations
    (`async_stop`, `join`, `recover_object`).
 -  **`Ractor::Wrapper::Stub`** — a Ractor-shareable proxy that mimics the
    method interface of the original object. Uses `method_missing` to forward
    arbitrary method calls to the wrapper.
 -  **`Ractor::Wrapper::Configuration`** — builder for wrapper options,
    including per-method settings.
 -  **`Ractor::Wrapper::MethodSettings`** — a frozen value object holding the
    copy/move/void choices for a single method.
 -  **`Ractor::Wrapper::Server`** — the backend that runs the object and
    services messages. In isolated mode it runs in its own Ractor; in local
    mode it runs as one or more threads inside the Ractor that created the
    wrapper.
 -  **`Ractor::Wrapper::Server::Dispatcher`** — a thread-safe work distributor
    used only in threaded mode. Handles routing of new calls (via a shared
    queue) and fiber resumes (via per-worker queues).
 -  **Message types** (all `Data.define` frozen structs):
    `InitMessage`, `CallMessage`, `ReturnMessage`, `ExceptionMessage`,
    `FiberYieldMessage`, `BlockingYieldMessage`, `FiberReturnMessage`,
    `FiberExceptionMessage`, `StopMessage`, `JoinMessage`, `JoinReplyMessage`,
    `WorkerStoppedMessage`.

Viewed from the outside, the runtime architecture looks like this:

```
Caller Ractor(s)                     Wrapper                  Wrapped object
┌──────────────────┐        ┌───────────────────────┐         ┌────────────┐
│ stub.some_method │───────▶│ Wrapper#call          │         │  <object>  │
│                  │ (Stub) │   sends CallMessage   │         │            │
│                  │        └─────────┬─────────────┘         └──────▲─────┘
│                  │                  │ @port                        │
│                  │        ┌─────────▼──────────────┐               │
│                  │        │ Server main fiber /    │ fibers /      │
│ ◀── ReturnMsg ───│────────│ worker threads         │ threads ◀─────┘
│ ◀── ExceptionMsg │        │ (dispatch, fiber mgmt) │
│ ◀── YieldMsg ────│──▶ run block ─▶ send result ─▶  │
└──────────────────┘        └────────────────────────┘
```

The rest of this document expands each of these boxes.

---

## 3. Configuration

Configuration is a two-stage process: keyword arguments to `Wrapper.new`,
optionally followed by a block that yields a mutable `Configuration` instance.
The block runs _before_ the wrapper materializes its server, so it is the last
chance to adjust behavior before the wrapper is frozen for Ractor shareability.

Inside `Wrapper#initialize` the sequence is:

 1. Validate that the supplied object is not already a moved
    `Ractor::MovedObject`.
 2. Build a fresh `Configuration`, seeding it with constructor kwargs.
 3. Yield it to the user's block, if any. Block-provided settings override
    kwargs because they are applied later.
 4. Resolve `Configuration#final_method_settings` into a frozen hash.
 5. Create the `Stub`, freeze the wrapper, and start the server.

### 3.1 Wrapper-level settings

| Setting | Type | Default | Purpose |
|---|---|---|---|
| `name` | `String` | `object_id.to_s` | Identifies the wrapper in log output and as the Ractor name. |
| `use_current_ractor` | `Boolean` | `false` | Selects the execution mode (see §4). |
| `threads` | `Integer` | `0` | Number of worker threads. `0` means sequential. |
| `enable_logging` | `Boolean` | `false` | Enables internal stderr logging. |

### 3.2 Method-level settings — `MethodSettings`

`MethodSettings` governs how a single method communicates its data. Each
instance is frozen and carries five values:

| Field | Values | Meaning |
|---|---|---|
| `arguments` | `:copy` \| `:move` | How positional and keyword arguments are shipped to the server. |
| `results` | `:copy` \| `:move` \| `:void` | How return values come back. `:void` returns `nil`. |
| `block_arguments` | `:copy` \| `:move` | How arguments to a caller-side block are shipped to the caller. |
| `block_results` | `:copy` \| `:move` \| `:void` | How the block's return value is shipped back to the server. |
| `block_environment` | `:caller` \| `:wrapped` | Where the block runs (see §7). |

The settings are stored in `Configuration#@method_settings`, keyed by method
name (Symbol) or `nil` for the defaults. `final_method_settings` produces the
resolved hash by:

 1. Starting with a hard-coded fallback of all `:copy` and
    `block_environment: :caller`.
 2. Overlaying the `nil` entry (defaults the user supplied or got from kwargs)
    on top of that fallback.
 3. For each named-method entry, overlaying it on top of those defaults.

Looking a method up via `Wrapper#method_settings(name)` returns the
per-method `MethodSettings` if present, otherwise the defaults. Because
`MethodSettings` is frozen and the outer hash is frozen, this value is
safely shared between the caller's Ractor and the server's Ractor — it
travels inside each `CallMessage`.

### 3.3 Why the config travels with every message

Embedding `settings` inside `CallMessage` is intentional. The server
needs to know, per call: whether to move or copy the return value,
whether to expect to run a block locally or to yield one, and whether to
void the return. Sending the settings with the message keeps the server
stateless with respect to configuration — it does not need a synchronized
copy of the settings hash, and configuration changes (were they ever
added) would not race with in-flight calls.

### 3.4 Copy / move / void — what they really mean

 -  `:copy` — the value is `Marshal`-style deep-cloned by Ractor when it
    crosses the boundary. Safe but potentially expensive for large payloads.
 -  `:move` — the value is transferred; the sender can no longer use it.
    This is sent as the `move:` keyword of `Ractor::Port#send`. Useful for
    large buffers and unique resources. The ownership model is subtle: if
    the caller passes a large string as a `:move` argument, it becomes a
    `Ractor::MovedObject` for the caller and must not be touched again.
 -  `:void` — the return value (or block result) is dropped and the
    recipient sees `nil`. This exists because many Ruby methods return a value
    "by accident"; they don't intend to return a specific value, but whatever
    final object is evaluated at the end of the method still gets functionally
    returned. If that object is large, the cost of shipping it could be both
    substantial and unnecessary: `:copy` might be expensive, and `:move` could
    be disastrous, making parts of the internal state unreachable. `:void` is
    an escape hatch that simply disables returning any value in such cases.

---

## 4. Execution modes

There are two orthogonal axes of execution mode, giving four combinations:

|                                  | Sequential (`threads: 0`) | Threaded (`threads: N>0`) |
|---|---|---|
| **Isolated** (`use_current_ractor: false`) | Object moved to a new Ractor; one method at a time. | Object moved to a new Ractor; N worker threads inside it. |
| **Local** (`use_current_ractor: true`) | Object stays in the current Ractor; one thread serves calls. | Object stays in the current Ractor; N worker threads serve calls. |

### 4.1 Isolated mode

`Wrapper#setup_isolated_server` spawns a new `Ractor` that immediately
calls `Server.run_isolated`. The object is _not_ passed as a Ractor
constructor argument, as doing so would copy the object. Instead, once the
Ractor is running, the wrapper sends a `InitMessage` (containing the object
and the stub) with `move: true`, and the server's first action inside
`receive_remote_object` is to receive and unpack it.

Because the object now lives in the server's Ractor, the original caller
can no longer touch it directly. It can, however, retrieve the object at
the end of the wrapper's life via `Wrapper#recover_object`, which is
implemented as `@ractor.value` — the Ractor's terminal value is the
wrapped object, returned from `Server#run`.

### 4.2 Local mode (`use_current_ractor: true`)

`Wrapper#setup_local_server` does not spawn a Ractor at all. It creates
a `Ractor::Port`, marks the wrapper frozen (so the stub is shareable),
and starts a regular `Thread` that runs `Server.run_local`. The wrapped
object is never moved; it stays with its creator.

This is the right mode for objects that cannot be moved between Ractors.
The canonical example is a SQLite3 database handle, which is bound to the
Ractor that created it. It's also appropriate when you want to keep
direct access to the object from the creating Ractor (say, for quick
synchronous probes that avoid the wrapper's message path entirely), which
is only safe to do outside method windows driven by the wrapper.

Tradeoffs vs isolated mode:

 -  No `recover_object` (the object was never moved; `recover_object` raises).
 -  No isolation: a bug in the wrapped object can corrupt state in the
    host Ractor, since they share a Ractor.
 -  Slightly lower overhead per call: no cross-Ractor marshalling of
    arguments / return values if both caller and server happen to be in
    the same Ractor (but note that in local mode other Ractors can still
    call through the stub, and _those_ calls pay the normal crossing cost).

### 4.3 Sequential vs threaded

`threads: 0` (sequential) means no `Dispatcher` is created and calls are
executed directly by the server's main message-handling loop. One method
runs at a time.

`threads: N > 0` creates a `Dispatcher` and spawns N worker threads.
Workers pull `CallMessage`s off the dispatcher's shared queue and execute
them concurrently. The concurrency ceiling is N regardless of how many
callers are blocked on calls.

A sharp note from the constructor doc: the `threads` value should be
sized to the concurrency of _independent_ calls, not to the re-entrancy
depth. A suspended method (waiting for a block result) does not occupy a
worker — it only occupies a fiber. The worker returns to the dispatch
loop and can service another call. This is why the threading model costs
very little even for deeply re-entrant workloads: fibers are cheap,
threads are not, and the library deliberately spends the former.

---

## 5. The Stub

`Ractor::Wrapper::Stub` is minimal by design:

```
Stub
  @wrapper  (frozen reference to Wrapper)
  method_missing(name, ...) → @wrapper.call(name, ...)
  respond_to_missing?(name, include_all) → @wrapper.call(:respond_to?, ...)
```

It freezes itself in `initialize`. Because its only instance variable is
a frozen reference to a frozen `Wrapper`, and the `Wrapper` is itself
shareable after construction, the stub is transitively shareable. That
means you can pass it freely across Ractor boundaries and every Ractor
can call methods on the wrapped object through it.

A few design choices worth calling out:

 -  **Why `method_missing` instead of pre-generating methods?** The stub
    must work for any wrapped object, including ones that define methods
    dynamically at runtime. There is no way to introspect the wrapped
    object's method list from another Ractor without paying a message
    round-trip anyway.
 -  **`respond_to_missing?` proxies through.** If you ask the stub whether
    the underlying object responds to `:foo`, the answer has to come from
    the server, because only it can see the object. So `respond_to?` is
    itself a round-trip call. This is slower than a direct lookup but
    semantically faithful.
 -  **Return-value substitution.** If the wrapped object returns `self`,
    the server substitutes the stub for the return value. This preserves
    method chaining through the stub boundary — `stub.tap { ... }` works as
    expected — even though `self` from the server's perspective is the
    bare object, not the stub. The same substitution is performed for
    block arguments (see §7).
 -  **No `call` method.** `Wrapper#call` is the low-level escape hatch; the
    stub always goes through `method_missing`. This means you cannot use
    `stub.call(:foo)` to bypass the proxy.

---

## 6. Messaging protocol

All messages are frozen `Data` structs, making them shareable and immutable.
This section catalogs them by direction.

### 6.1 From caller to server

| Message | Sender | Receiver | Purpose |
|---|---|---|---|
| `InitMessage(object, stub)` | `Wrapper#setup_isolated_server` | Server's `receive_remote_object` | One-shot initialization for isolated mode. Sent with `move: true`. |
| `CallMessage(method_name, args, kwargs, block_arg, transaction, settings, reply_port)` | `Wrapper#call` | Server main loop | Request a method invocation. |
| `FiberReturnMessage(value, fiber_id)` | `Wrapper#send_block_result` | Server main loop | Block result (fiber-suspend path). |
| `FiberExceptionMessage(exception, fiber_id)` | `Wrapper#send_block_exception` | Server main loop | Block exception (fiber-suspend path). |
| `StopMessage()` | `Wrapper#async_stop` | Server main loop | Request graceful shutdown. |
| `JoinMessage(reply_port)` | `Wrapper#join` (local mode only) | Server main loop | Request notification when server has fully stopped. |

### 6.2 From server to caller

| Message | Sender | Receiver | Purpose |
|---|---|---|---|
| `ReturnMessage(value)` | `Server#handle_method` (or main-loop refusal) | `Wrapper#call` | Normal method result. |
| `ExceptionMessage(exception)` | Server (several sites) | `Wrapper#call` | Method raised an exception; server-side refusal; or crash cleanup. |
| `FiberYieldMessage(args, kwargs, fiber_id)` | `Server#fiber_yield_block` | `Wrapper#call` / `handle_yield` | Request to run a block on the caller side (fiber-suspend path). |
| `BlockingYieldMessage(args, kwargs, reply_port)` | `Server#blocking_yield_block` | `Wrapper#call` / `handle_yield` | Same but blocking-fallback path. |
| `JoinReplyMessage()` | `Server#send_join_reply` | `Wrapper#join` | Terminal notification that the server has finished cleaning up. |

### 6.3 Within the server

| Message | Sender | Receiver | Purpose |
|---|---|---|---|
| `WorkerStoppedMessage(worker_num)` | `Server#cleanup_worker` | Server main loop | A worker thread has terminated. Carried on the main `@port`. |

### 6.4 Port topology

Each `CallMessage` carries its own `reply_port`, which is a fresh
`Ractor::Port` created by `Wrapper#call`. This gives the caller a private
channel for all replies to that call: return value, exceptions, and both
variants of yield message. The reply port is closed when the call returns
(success or exception) via the `ensure` block.

The server has a single main `@port` that it receives on. It multiplexes
everything: new calls, stop/join requests, fiber resumes, and worker
death notifications. This is why every message that the server needs to
dispatch internally carries enough information to route itself (e.g.
`FiberReturnMessage` includes the `fiber_id`).

### 6.5 Observability using `transaction`

The `transaction` field on `CallMessage` is a 16-character base-36 random
string created by `Wrapper#make_transaction`. It exists to correlate log lines
across caller and server for a single call; it is not used for dispatch.

---

## 7. Blocks, re-entrancy, and fiber magic

This is the most subtle part of the library. The core problem: a caller
may pass a block (`stub.each { |x| stub.process(x) }`). Where does that
block's body _run_?

### 7.1 The two block environments

`block_environment: :caller` (default) — the block runs in the caller's
Ractor, with full access to the caller's lexical scope. The server has
to ask the caller to run each invocation, wait for the result, and then
resume the method.

`block_environment: :wrapped` — the block runs in the server Ractor, in
the wrapped object's context. No inter-Ractor communication is needed per
block call. The block is captured as a `Ractor.shareable_proc`, which
means it can only reference shareable state. Closures over caller-side
mutable variables will fail at shareability-check time.

The tradeoff is: `:caller` is the common case because most blocks _do_
close over state (accumulators, config, etc.), but paying a round-trip
per invocation of a block called in a tight loop (think `each` over a
large collection) can be very expensive. `:wrapped` is the escape hatch
when you really want the block body to live alongside the method and the
block is self-contained. The README's Enumerator-over-SQLite example is
one such case.

### 7.2 How the block arg is represented

`Wrapper#make_block_arg` looks at the `block_environment` setting and
constructs one of three things:

 -  `nil` — no block was given.
 -  `:send_block_message` (a sentinel symbol) — `:caller` mode. The server must
    construct a local proc that forwards each invocation back across the wire.
 -  A `Ractor.shareable_proc` — `:wrapped` mode. The shareable proc travels
    directly inside the `CallMessage` and is invoked in-Ractor.

On the server side, `Server#make_block` translates this into the actual
proc passed to the wrapped method:

 -  `nil` → no block is passed; `__send__(name, *args, **kwargs, &nil)` is
    equivalent to calling without a block.
 -  A shareable proc → used directly.
 -  `:send_block_message` → a proc is constructed that, when invoked,
    performs the round-trip yield dance described in §7.3.

### 7.3 Caller-side block invocation — the fiber-suspend path

When the wrapped object invokes a `:caller`-environment block, the proc
created by `make_block` runs in the server. That proc needs to:

 1. Ship the arguments over to the caller.
 2. Wait for the caller to produce a result (or exception).
 3. Return that result (or raise that exception) to the wrapped method
    so execution continues.

Naively, step 2 is a blocking wait. But the server cannot just block, because
other callers (or the same caller, via re-entrancy) might have messages
waiting, and we do not want to deadlock. Moreover, the server's concurrency
model is "handle one message at a time in the main loop," which cannot be
respected if methods can arbitrarily block it.

The solution is to run method bodies inside `Fiber`s. When a method needs to
yield to a caller-side block, its fiber calls `Fiber.yield`. Control returns to
the main loop, which processes further messages. When a matching
`FiberReturnMessage` / `FiberExceptionMessage` arrives, the main loop looks up
the suspended fiber by `fiber_id` and resumes it with the reply message. The
fiber picks up where it left off and continues the method.

Here is the full choreography, in sequence-diagram form, for one block
invocation (assume sequential mode, `:caller` block):

```
Caller Ractor              Wrapper owner / Server             Wrapped object

Wrapper#call
  reply_port = Port.new
  send(CallMessage) ────────▶ main_loop
                                dispatch_call
                                  start_method_fiber ──▶ Fiber F
                                                           handle_method
                                                             object.m(&block) ─▶ method runs
                                                                                    yield arg
                                                               block.call(arg) ◀──┘
                                                               (our synthetic proc)
                                                                 fiber_yield_block
                                                                   send(FiberYieldMessage
                                                                     fiber_id=F.id) ────▶ reply_port
  loop: receive                                                     Fiber.yield ←─ suspends F
    FiberYieldMessage ◀────── reply_port
    handle_yield
      run block in caller
      result = ...
      send(FiberReturnMessage) ──▶ server @port
                                main_loop
                                  dispatch_fiber_resume
                                    resume_method_fiber(msg)
                                      F.resume(msg) ──▶ fiber_yield_block returns value
                                                           method continues, returns result
                                                         handle_method sends ReturnMessage ▶ reply_port
  loop: receive
    ReturnMessage ◀──────────── reply_port
    return value
```

Critical invariants:

 -  **Fibers cannot migrate between threads.** A fiber can only be resumed from
    the thread that last resumed it. In sequential mode this is trivially
    satisfied, since the main loop is the only place that runs fibers. In
    threaded mode, §8 explains how the `Dispatcher` preserves this invariant.
 -  **The main loop never blocks inside a method.** It only blocks on
    `@port.receive`. All method work is delegated to a fiber (sequential)
    or a worker thread (threaded).
 -  **Fiber ids are just `object_id`s.** They are unique while the fiber
    is alive, which is long enough for routing. Once a fiber completes it
    is removed from the `@pending_fibers` / per-worker `pending` hash, so
    stale `fiber_id`s cannot collide with fresh fibers.

### 7.4 Caller-side block invocation — the blocking-fallback path

The fiber-suspend path depends on one thing: the block-invoking proc being
called from the very same fiber that `handle_method` started in. That is true
for straightforward method bodies. It is _not_ true in two cases:

 -  The wrapped method invokes the block from a nested fiber. The classic
    example is an `Enumerator`, whose `each` runs the user's block in a
    generator fiber, not the outer fiber.
 -  The wrapped method invokes the block from a spawned thread.

Calling `Fiber.yield` from either context does something different from what we
need: it either yields the wrong fiber, or raises a `FiberError`. To stay
functional in these cases, `Server#make_block` captures the expected fiber at
construction time and checks at call time:

```ruby
if Fiber.current.equal?(expected_fiber)
  fiber_yield_block(...)   # the fast path
else
  blocking_yield_block(...) # the fallback
end
```

The blocking fallback:

 1. Creates a fresh temporary `reply_port`.
 2. Sends a `BlockingYieldMessage` carrying that port.
 3. Calls `reply_port.receive` — a real, thread-level block.
 4. The caller's `handle_yield` sends the reply directly to the temporary port.

This path _does_ block the invoking thread (or spawned thread / nested fiber).
That is its defining limitation. Two consequences follow:

 -  **In sequential mode with a nested-fiber block invocation, the server main
    loop is blocked.** No other messages can be processed while the block runs
    in the caller. If the block tries to re-enter the wrapper, it will deadlock.
    The re-entering call goes to the server's port, but the server cannot
    handle it. This is the limitation the README's caveats warn about.
 -  **In threaded mode, only one worker is blocked.** Other workers continue to
    service other calls. But the blocked worker still cannot service anything
    else, so a long-running nested-fiber block still reduces effective
    concurrency by one.

The hybrid design (fast path where possible, fallback where necessary)
is a deliberate trade-off: correctness in the common case, plus continued
functionality in the exotic case, at the cost of a deadlock hazard that
users have to be aware of when their block is re-entrant _and_ invoked
from a nested fiber or thread.

### 7.5 `self`-substitution for blocks

When a `:caller` block is invoked, it may receive the wrapped object
itself as an argument (think `each_with_object(self) { ... }`). Before
shipping the block arguments over the port, `make_block`'s synthetic
proc replaces any argument that is `equal?(@object)` with `@stub`. This
keeps the caller from ever seeing the bare object and accidentally
performing direct operations on it from the wrong Ractor.

---

## 8. Worker thread dispatch — the `Dispatcher`

Threaded mode introduces the `Dispatcher` class, which solves two
problems at once: **work distribution** and **fiber affinity**.

### 8.1 Why not a single shared queue?

The obvious design would be: one thread-safe queue, all workers pull.
That works for new calls. It does _not_ work for fiber resumes. If
worker A started fiber F, then F suspended, a `FiberReturnMessage` for F
arrives, and worker B dequeues it, then B cannot resume F, because Ruby
requires fibers to be resumed from their last resuming thread.

### 8.2 Queue layout

The `Dispatcher` holds:

 -  A **shared queue** (`@shared_queue`) for new `CallMessage`s. Any
    worker may dequeue.
 -  **Per-worker queues** (`@worker_queues`, indexed by `worker_num`) for
    fiber resumes. Only worker `N` dequeues from `@worker_queues[N]`.
 -  A **fiber→worker map** (`@fiber_to_worker`) so the main loop can route
    incoming `FiberReturnMessage` / `FiberExceptionMessage` to the correct
    per-worker queue.
 -  Flags: `@closed` and `@crashed`, driving graceful vs abortive shutdowns.
 -  A single `@mutex` + `@cond` pair guarding all of the above.

Producers call `@cond.broadcast` rather than `@cond.signal` so a worker
waiting on its per-worker queue is not starved by shared-queue activity.

### 8.3 `dequeue` priority

Each worker thread calls `@dispatcher.dequeue(worker_num, accept_calls:)`
in a loop. Inside the mutex, `dequeue` returns the first of:

 1. An item from **its own per-worker queue** (a fiber resume). Always
    considered first, even after close — in-flight fibers must complete.
 2. `TERMINATE` if `@crashed` is set and the per-worker queue is empty.
 3. An item from the **shared queue**, but only if `accept_calls` is
    `true` and the dispatcher is not closed.
 4. A one-shot `CLOSED` sentinel if `@closed` and this worker has not yet
    been told. This wakes the worker so it can transition into a
    "drain pending and exit" state.
 5. Otherwise, `@cond.wait`.

The `accept_calls` flag is the worker's way of saying "I've started
stopping; don't hand me new work." Once `CLOSED` has been delivered the
worker sets `stopping = true` and passes `accept_calls: false` on every
subsequent `dequeue`.

### 8.4 Fiber lifecycle in threaded mode

When a worker picks up a `CallMessage`:

 1. `start_worker_fiber` creates a fiber whose body is `handle_method`.
 2. The fiber's `object_id` is the `fiber_id`. The worker registers it
    with `@dispatcher.register_fiber(fiber_id, worker_num)` and stores it
    in a local `pending` hash.
 3. The worker resumes the fiber. If the fiber completes synchronously
    (no block yield), the worker removes it from `pending` and calls
    `@dispatcher.unregister_fiber`.
 4. If the fiber suspends (via `Fiber.yield` in `fiber_yield_block`), it
    remains in `pending` and is left registered. The worker goes back to
    the dispatch loop.
 5. Eventually a `FiberReturnMessage` / `FiberExceptionMessage` arrives at the
    server's main port. The main loop calls `@dispatcher.enqueue_fiber_resume`,
    which looks up `@fiber_to_worker[fiber_id]` and pushes onto the right
    per-worker queue.
 6. The owning worker dequeues it, resumes the fiber, and either completes it
    or suspends it again.

If a fiber-resume arrives for a `fiber_id` that is no longer registered,
`enqueue_fiber_resume` returns `false` and the main loop logs
"Discarding orphan fiber resume." This can happen if the worker that
owned the fiber crashed, since `cleanup_worker` unregisters all pending
fibers before the server observes `WorkerStoppedMessage`.

### 8.5 Why the main loop still routes fiber resumes

An alternative would be for workers to receive their fiber resumes
directly (e.g., each worker owning a port). The current design keeps
everything flowing through `@port` so there is exactly one place the
server receives messages. That simplifies:

- Shutdown: draining one port drains everything.
- Logging: one locus of message observation.
- Caller-side symmetry: callers only need to know one port.

The cost is an extra hop (main loop → dispatcher → worker), but this is
cheap because the main loop does no work beyond enqueue.

---

## 9. Server lifecycle

`Server#run` is the top of the state machine:

```ruby
def run
  receive_remote_object if @isolated
  start_workers if @threads_requested
  main_loop
  stop_workers if @threads_requested
  cleanup
  @object
rescue Exception => e
  @crash_exception = e
  @object
ensure
  crash_cleanup if @crash_exception
end
```

Expressed as phases:

```
  ┌────────────────────┐
  │   init (isolated)  │  receive_remote_object
  └─────────┬──────────┘
            ▼
  ┌────────────────────┐
  │    start workers   │  only if threads > 0
  └─────────┬──────────┘
            ▼
  ┌────────────────────┐  accepts CallMessage, FiberReturn/Exception,
  │    RUNNING         │  StopMessage, JoinMessage, WorkerStoppedMessage.
  │    (main_loop)     │  Exits on: StopMessage, or unexpected worker death.
  └─────────┬──────────┘
            ▼
  ┌────────────────────┐  sequential: drain_pending_fibers (inline);
  │    STOPPING        │  threaded:   stop_workers (close dispatcher,
  │                    │              wait for WorkerStoppedMessage from each).
  └─────────┬──────────┘  Refuses new calls with StoppedError.
            ▼
  ┌────────────────────┐  Close @port, drain remaining messages,
  │    CLEANUP         │  respond to outstanding join requests.
  └─────────┬──────────┘
            ▼
         (return @object — terminal value for the Ractor in isolated mode)

  ──────────────────────────── crash path ──────────────────────────────
  Any uncaught exception from the above jumps to:
  ┌────────────────────┐  crash_cleanup:
  │    CRASH CLEANUP   │   • abort_pending_fibers (sequential)
  │    (ensure block)  │   • drain_dispatcher_after_crash (threaded)
  └────────────────────┘   • drain_inbox_after_crash
                           • join_workers_after_crash (threaded)
                           • respond to join requests
```

### 9.1 Running phase — `main_loop`

`Server#main_loop` reads from `@port.receive` and dispatches on message type:

 -  `CallMessage` → `dispatch_call`, which in sequential mode starts a new
    fiber inline (`start_method_fiber`), or in threaded mode pushes onto
    the dispatcher's shared queue (`@dispatcher.enqueue_call`).
 -  `FiberReturnMessage` / `FiberExceptionMessage` → `dispatch_fiber_resume`,
    which resumes the fiber inline (sequential) or enqueues onto the
    correct per-worker queue (threaded).
 -  `JoinMessage` → added to `@join_requests`; reply is sent when the
    server finishes.
 -  `StopMessage` → initiates graceful shutdown. In sequential mode, the
    main loop first calls `drain_pending_fibers` to let any suspended
    methods complete.
 -  `WorkerStoppedMessage` → an _unexpected_ worker death during running
    phase. Treat it as a fatal signal and break out of the loop. The
    cleanup code down-stream takes over.

### 9.2 Stopping phase — sequential vs threaded

In sequential mode, the stopping logic is just `drain_pending_fibers`:
continue to receive messages on `@port`, refuse any new `CallMessage`,
forward `FiberReturnMessage`/`FiberExceptionMessage` to their fibers,
queue any late `JoinMessage`s, and exit once `@pending_fibers` is empty.

In threaded mode, `stop_workers` is more involved:

 1. Call `@dispatcher.close`, which flips `@closed` and drains (returns)
    any `CallMessage`s that were queued but never picked up. Those
    messages are refused with `StoppedError`. This is important: without
    this step, callers whose messages arrived _after_ stop but before a
    worker could dequeue would hang forever.
 2. Each worker, on its next `dequeue`, receives the one-shot `CLOSED`
    sentinel. It sets `stopping = true` and stops accepting new calls.
 3. The main loop continues to receive messages, but now it must:
     -  Refuse any `CallMessage` that still arrives.
     -  Forward `FiberReturnMessage`/`FiberExceptionMessage` through the
        dispatcher so that workers can finish their suspended methods.
     -  Acknowledge `WorkerStoppedMessage` and decrement `@active_workers`.
     -  Queue late `JoinMessage`s.
 4. Loop until all workers have reported stopped.

### 9.3 Cleanup phase

`cleanup` closes `@port` and then drains anything left in it. Callers
whose messages arrive after port close get `Ractor::ClosedError` on
send — nothing the server can do for them. But any `CallMessage` that
was already in the port before close is still refused, and any late
`JoinMessage` is answered immediately.

Finally, all queued join requests get a `JoinReplyMessage`.

### 9.4 Why `main_loop` breaks out on unexpected worker death

The `WorkerStoppedMessage` in the running-phase branch of `main_loop` covers
the case where a worker thread dies _without_ going through the graceful stop
path, typically because it raised an exception we did not catch. When this
happens the server declares the situation unsafe: the invariant "every pending
fiber has a living worker to resume it" is broken, and rather than try to fix
it in place (by e.g. restarting the worker), the server shuts down. This is
opinionated: the author chose reliability of shutdown semantics over continued
availability of other workers.

### 9.5 Join

`Wrapper#join` has two implementations:

 -  **Isolated mode:** `@ractor.join` — relies on the underlying
    `Ractor#join` to block until the server Ractor terminates.
 -  **Local mode:** there is no Ractor to join, so the wrapper sends a
    `JoinMessage` with a fresh reply port and waits for
    `JoinReplyMessage`. The server adds the reply port to its
    `@join_requests` list and replies in cleanup or crash cleanup.

The docstring on `Wrapper#join` notes an important deviation from
`Thread#join` / `Ractor#join`: a crashed wrapper does _not_ propagate
its exception out of `join`. The reasoning is that wrapper crashes are
typically internal bugs (in the server's dispatch code, not in the
wrapped object's methods), and we already deliver `CrashedError` to any
pending caller; re-raising in `join` would just produce a duplicate
error at an awkward point.

### 9.6 Recovering the object

In isolated mode, `@ractor.value` returns the wrapped object after the
Ractor has terminated. `Server#run` is written to always return `@object`
from both the success and rescue paths, so even a crashed server
surrenders the object (modulo Ruby's own post-mortem rules). This is
intentional: the object is yours, you may want to clean it up yourself,
and the wrapper should not hold it hostage.

In local mode, `recover_object` raises. The object never moved, so there is
nothing to recover.

---

## 10. Graceful stop and crash cleanup in detail

Graceful stop is covered in §9. This section focuses on what happens
when something goes wrong.

### 10.1 What constitutes a crash

Any uncaught exception inside `Server#run` ends up in the `rescue` clause.
This can originate from:

 -  A bug in the server's dispatch code itself.
 -  An unexpected `Ractor::ClosedError` when the port is already closed
    (though most sites catch this explicitly).
 -  An exception during fiber management.

Notably, exceptions raised by the _wrapped object's methods_ are **not**
crashes. They are caught by `handle_method`'s `rescue ::Exception` clause and
converted into an `ExceptionMessage` sent to the caller. The server itself
stays alive.

A worker thread crash is detected via `WorkerStoppedMessage` (its normal stop
notification) arriving during the running phase, or via the worker's own
`ensure` block catching its exception (see `worker_loop`'s `crash_exception`
path).

### 10.2 `crash_cleanup`

The goal of `crash_cleanup` is to deliver a `CrashedError` to every caller who
would otherwise hang, and to unblock any join waiters. It does as much as
possible, swallowing further errors, because by this point the server is
definitely going away and best-effort is the only realistic policy.

Steps:

 1. **Threaded mode:** `drain_dispatcher_after_crash` calls
    `@dispatcher.crash_close`. This (a) sets `@crashed`, so future
    `dequeue` calls return `TERMINATE` on empty per-worker queues,
    causing workers to exit instead of waiting forever; and (b) returns
    the shared queue's undispatched messages so the server can send
    `CrashedError` to each.
 2. **Sequential mode:** `abort_pending_fibers` calls `fiber.raise(error)`
    on each suspended fiber. The exception emerges from the fiber's
    `Fiber.yield` call; `handle_method`'s `rescue ::Exception` catches it
    and sends an `ExceptionMessage(CrashedError)` to the fiber's reply
    port. So the caller observes `CrashedError` — the same error class
    they would get in threaded mode.
 3. `drain_inbox_after_crash` closes `@port` and drains anything left,
    sending `CrashedError` to any `CallMessage` senders and
    `JoinReplyMessage` to any `JoinMessage` senders.
 4. **Threaded mode:** `join_workers_after_crash` waits for all workers to
    finish. Workers' own `cleanup_worker` runs in their ensure blocks: they
    abort _their_ pending fibers (delivering `CrashedError` to each caller),
    unregister the fibers, and make a best effort to send
    `WorkerStoppedMessage` back to the main loop.
 5. Any remaining `@join_requests` are answered.

### 10.3 Why `fiber.raise` for sequential cleanup?

It might be simpler to iterate over `@pending_fibers` and send `CrashedError`
directly to each fiber's reply port. The reason `fiber.raise` is preferred is
that it runs the fiber's rescue and ensure blocks, allowing the method (and any
wrapper code around it) to clean up, e.g., releasing locks, closing file
handles opened inside the method. This better respects the wrapped object's
invariants at the cost of being slightly slower and more fallible.

### 10.4 The best-effort nature of cleanup

`crash_cleanup` wraps almost everything in `rescue ::Exception`. This is
deliberate: we are already handling a crash and the priority is to get
through the cleanup steps without aborting partway. A lost `CrashedError`
delivery is regrettable but tolerable; a stuck wrapper is not.

---

## 11. Design trade-offs and known limitations

This section collects the trade-offs already mentioned throughout,
plus a few others, in one place for the benefit of readers deciding
whether the library fits their use case.

### 11.1 Re-entrancy from nested fibers or spawned threads can deadlock

The fiber-suspend path is only available when the block invocation
happens on the same fiber that started the method. Nested fibers
(most visibly inside `Enumerator`) and spawned threads fall back to
the blocking path, which does not release the server to service
further messages. If such a block tries to re-enter the wrapper, the
re-entering call arrives at `@port` but nothing can pick it up — the
server is blocked inside the very call that sent it. This deadlocks.

Mitigations:
 -  Configure the method with `block_environment: :wrapped` if the
    block is self-contained.
 -  Avoid re-entering the wrapper from blocks called from within
    Enumerator generators or user-spawned threads.
 -  In threaded mode the blast radius is one worker, not the whole
    server. With enough workers this is survivable.

### 11.2 Blocks configured as `:caller` cannot outlive the method call

The synthetic proc generated by `make_block` relies on the caller still
being in its `Wrapper#call` reply loop. If the wrapped object saves the
block (as a callback, say) and invokes it later, the caller is long
gone and the fiber-yield / blocking-yield both have no one to reply.
The library does not currently detect this at save time — the failure
manifests at invocation time when the message goes nowhere.

If you need to register a callback, prefer `block_environment: :wrapped`
so the block travels as a `Ractor.shareable_proc` and is invoked
in-place.

### 11.3 Exceptions lose their backtrace

As of Ruby 4.0, exceptions transferred between Ractors are always
copied (not moved) and the backtrace is cleared. This is a Ruby bug
(tracked at bugs.ruby-lang.org issue 21818) that the library cannot
work around, and it applies both to exceptions raised by the wrapped
method and to exceptions raised by caller-side blocks.

### 11.4 Non-shareable, non-movable types cannot cross the boundary

Ractor's own rules apply. Threads, procs (non-shareable), backtraces,
and a few other types cannot be passed as arguments or returned as
values. `:move` can help with some cases (large strings, arrays of
mutable values), but some types cannot be moved at all.

### 11.5 Worker count is a hard ceiling on parallelism

The `threads` setting is the maximum number of concurrent method bodies.
The library does not grow or shrink the pool. Sizing it right requires
knowing both the workload's natural concurrency and, if you are using
blocking-fallback paths, the expected number of simultaneously-blocked
workers.

Suspended fibers (the common re-entrancy case) do _not_ occupy a worker,
so re-entrancy depth does not need to be part of the calculation.

### 11.6 No method-level timeouts

A misbehaving wrapped method that never returns will block its worker
(or the server itself, in sequential mode) indefinitely. There is no
built-in timeout. Callers can avoid their own indefinite wait by
implementing timeouts around their stub calls, but the server-side
work will still occupy the thread.

### 11.7 Experimental status

The library is self-described as experimental, and the README repeats
this warning prominently. This is true both of the library and of
Ractors in general in Ruby 4.0. Expect behavior to evolve as Ruby's
Ractor implementation matures; internals may change in lock-step.

---

## 12. Putting it together — a worked walkthrough

To make all of the above concrete, here is the full story for a single
call to `stub.find_by_id(42)` against a SQLite3 wrapper configured with
`use_current_ractor: true, threads: 2`, from a caller in a different
Ractor. Assume `:caller` block environment and no block is passed in
this example.

 1. **Caller Ractor invokes `stub.find_by_id(42)`.** `Stub#method_missing`
    forwards to `Wrapper#call(:find_by_id, 42)`.
 2. **`Wrapper#call` prepares a `CallMessage`.** It creates a fresh
    `Ractor::Port` as `reply_port`, generates a `transaction` id, fetches
    the per-method `MethodSettings`, computes `block_arg = nil` (no block
    was given), and sends the `CallMessage` on `@port`.
 3. **Server's main loop (running in a Thread in the host Ractor)
    receives the message.** It calls `dispatch_call`, which since threads
    were requested, calls `@dispatcher.enqueue_call(message)`. The
    dispatcher pushes onto its shared queue and broadcasts.
 4. **Some worker (say worker 0) dequeues `[:call, message]`.** It calls
    `start_worker_fiber`, which creates a fiber around
    `handle_method(message, worker_num: 0)`, registers the fiber with the
    dispatcher, and resumes it.
 5. **Inside the fiber, `handle_method` calls
    `@object.__send__(:find_by_id, 42)`.** No block is involved, so
    `make_block` returns `nil`. The DB object does its work and returns a row.
 6. **`handle_method` sends a `ReturnMessage(row)` to
    `message.reply_port`.** The fiber completes. The worker removes it
    from its local `pending` and unregisters from the dispatcher. The
    worker loops back to `@dispatcher.dequeue`.
 7. **Caller's `Wrapper#call` `receive`s the reply.** Its loop matches
    `ReturnMessage`, returns `row`. The `ensure` block closes the
    `reply_port`. `Stub#method_missing` returns `row` to the caller.

Now insert a block: `stub.find_by_id(42) { |r| transform(r) }`.

 -  Between steps 5 and 6, the wrapped method invokes the block. Because
    `block_environment: :caller` is the default, `make_block` has wrapped
    it in a proc that:
     -  Substitutes the stub for any argument equal to `@object`.
     -  Checks that `Fiber.current` equals the fiber that `handle_method`
        started in. It does, so:
     -  Sends a `FiberYieldMessage(args: [row], kwargs: {}, fiber_id: F)`
        to `reply_port`.
     -  Calls `Fiber.yield`.
 -  Worker 0 is now free and returns to the dispatch loop (the fiber is
    suspended but registered).
 -  The caller's `call` loop receives `FiberYieldMessage`, runs `handle_yield`,
    which calls the block, captures `transform(row)`, and sends a
    `FiberReturnMessage(transformed, F)` to the server's main `@port`.
 -  The main loop receives the `FiberReturnMessage`, calls
    `dispatch_fiber_resume`, which goes through the dispatcher:
    `@dispatcher.enqueue_fiber_resume(message)` looks up
    `@fiber_to_worker[F]` (which is worker 0), pushes onto worker 0's
    per-worker queue, and broadcasts.
 -  Worker 0's `dequeue` finds the per-worker queue non-empty, returns
    `[:resume, message]`. `resume_worker_fiber` resumes fiber F with
    the message. Inside `fiber_yield_block`, `Fiber.yield` returns the
    `FiberReturnMessage`; the block-proc unwraps `.value` and returns
    `transformed` to the wrapped method.
 -  The wrapped method completes, returns, `handle_method` sends
    `ReturnMessage` back to `reply_port`. The fiber ends, the worker
    cleans it up and goes back to the dispatch loop.
 -  The caller's `call` loop finally receives `ReturnMessage` and returns.

This is the full dance for one call with one block invocation. Every step can
be logged, and the `transaction` id ties them together in the output.

---

## 13. Summary

`Ractor::Wrapper` achieves shared access to a non-shareable object by:

 -  Running the object in a controlled server (either a dedicated Ractor
    or a set of threads in the creating Ractor).
 -  Exposing a frozen, shareable `Stub` that forwards calls via a
    message-passing protocol with per-call reply ports.
 -  Using fibers to run method bodies so that they can suspend cleanly
    when caller-side blocks need to execute, without blocking the
    server's main message loop.
 -  Using a custom `Dispatcher` in threaded mode that routes new calls
    through a shared queue but fiber resumes through per-worker queues,
    preserving Ruby's fiber-to-thread affinity.
 -  Falling back to a blocking path when a block is invoked from a nested
    fiber or spawned thread, trading re-entrancy for continued functionality.
 -  Modeling configuration as a frozen value object that travels with
    each call, keeping the server stateless with respect to settings.
 -  Providing a carefully staged lifecycle (running → stopping →
    cleanup) with a separate crash-cleanup path that makes a
    best-effort attempt to unblock every pending caller and join
    waiter when something goes wrong.

The net effect is a library that tries to make using a non-shareable object
from multiple Ractors look as close as possible to calling it directly, while
being honest about the edges where the abstraction leaks.

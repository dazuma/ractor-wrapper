# Implementation Plan: Fiber-Based Server Execution

Companion to `design/fibers.md`. Red-green TDD with a commit checkpoint after
every green step. Conventional Commits format. Run `toys test` (and ideally
`toys ci`) before each commit.

## Decisions (resolved up-front)

- **`YieldMessage` is deleted cleanly** and renamed to `BlockingYieldMessage`.
  The library is experimental; clean breaking changes to internal constants are
  acceptable.
- **Crash-during-suspended-fiber tests** trigger the crash from the test side
  via `wrapper.instance_variable_get(:@port).send(CrashingJoinMessage.new(...))`
  while a fiber is suspended (using a synchronization latch from inside the
  caller block to time the crash).
- **No README changes.** New documentation lives only in YARD (step 4.2).

---

## Phase 0 — Test infrastructure

**0.1** Add to `test/helper.rb`:
- A `with_timeout(seconds = 2, &block)` helper that runs the block in a thread,
  fails with the thread's backtrace if it doesn't return in time, kills the
  thread, and returns the block's value otherwise.
- An `each_item(items, &block)` method on `RemoteObject` (yields each item) for
  reuse by deadlock tests.

**Commit:** `test: Infrastructure updates for fiber changes`

---

## Phase 1 — Sequential-mode fiber path

### 1.1 Basic re-entrant call (sequential)

**Red.** Test: `stub.each_item(["a", "b"]) { |x| stub.process(x) }` completes
within 2s and returns the expected values. Currently deadlocks.

**Green.**
- Replace `YieldMessage` with `BlockingYieldMessage` (pure rename throughout).
- Add new message types: `FiberYieldMessage(:args, :kwargs, :fiber_id)`,
  `FiberReturnMessage(:value, :fiber_id)`,
  `FiberExceptionMessage(:exception, :fiber_id)`.
- Update caller side (`Wrapper#call`, `handle_yield`) to handle both
  `FiberYieldMessage` and `BlockingYieldMessage` — fiber path sends
  `BlockReturn`/`BlockException` to `@port`; blocking path sends
  `Return`/`Exception` to `message.reply_port` (unchanged).
- Rewrite `Server#make_block` to use the fiber path (no hybrid check yet — the
  basic test goes through this path correctly).
- Update sequential-mode `Server#main_loop`: spawn a `Fiber` per `CallMessage`,
  store in `@pending_fibers`, and on `FiberReturnMessage`/`FiberExceptionMessage`
  resume the right fiber.

**Commit:** `feat: Sequential-mode fiber execution for re-entrant block calls`

### 1.2 Nested re-entry (sequential)

**Red.** Test: depth-3 re-entrant calls (method A yields → block calls method B
→ method B yields → block calls method C). Verify all values returned correctly
within 2s.

**Green.** Likely passes already after 1.1. If anything needs fixing, fix it.

**Commit:** `test: Cover nested re-entry in sequential mode` (only if changes
were made; otherwise fold the test into the next commit).

### 1.3 Hybrid fallback for nested-fiber callbacks

**Red.** Test: wrapped method runs `Enumerator.new { |y| y << "a"; y << "b" }
.each(&block)`. Without hybrid detection, `Fiber.yield` from inside the
Enumerator's generator yields the wrong fiber and corrupts state. Test asserts
the call completes correctly (no re-entry needed for this test — just non-
re-entrant block invocation through an Enumerator).

**Green.** In `make_block`, capture `expected_fiber = ::Fiber.current` and
check `::Fiber.current.equal?(expected_fiber)` inside the proc. If equal, use
`fiber_yield_block`; otherwise use `blocking_yield_block` (the original
blocking implementation, preserved as-is).

**Commit:** `feat: Hybrid fiber-yield fallback for nested-fiber callbacks`

### 1.4 Documented limitation: Enumerator + re-entry still deadlocks

**Red.** Test: wrapped method invokes block via Enumerator AND the block makes
a re-entrant wrapper call. With a short timeout (e.g., 1s), assert the call
does NOT complete (we expect a deadlock — this documents the unchanged
limitation of the blocking fallback path).

No production code change.

**Commit:** `test: Document that Enumerator+re-entry still deadlocks`

### 1.5 Graceful stop with suspended fiber (sequential)

**Red.** Test: start a method-with-block; have the block block on a latch;
call `wrapper.async_stop`; verify (a) a new method call raises `StoppedError`,
(b) release the latch and the original call returns its correct value.

**Green.** Add a sequential-mode "stopping" phase: after `StopMessage`, refuse
new `CallMessage`s with `StoppedError`, but continue draining
`FiberReturnMessage`/`FiberExceptionMessage` from `@port` until
`@pending_fibers` is empty.

**Commit:** `feat: Drain pending fibers during graceful stop (sequential mode)`

### 1.6 Crash with suspended fiber (sequential)

**Red.** Test: start a method-with-block; in the block, signal a latch then
block; from the test side, send a `CrashingJoinMessage` to
`wrapper.instance_variable_get(:@port)`; release the latch; assert caller
receives `CrashedError`.

**Green.** Add `abort_pending_fibers(@pending_fibers)` to `crash_cleanup` in
the sequential branch. Each fiber's `handle_method` rescue chain sends the
exception to its own `reply_port`.

**Commit:** `feat: Abort pending fibers on server crash (sequential mode)`

---

## Phase 2 — Threaded-mode fiber path

### 2.1a Standalone Dispatcher class (with unit tests)

Build the multi-queue routing logic as a private nested class
`Server::Dispatcher` so it can be unit-tested in isolation. No `Server`
changes in this step — the existing threaded path keeps using `@queue`.

**Why first.** The dispatcher coordinates a mutex, a condition variable, a
shared queue, per-worker queues, and a fiber→worker map. Bugs in this layer
are pernicious to diagnose through end-to-end tests. Isolating it lets the
implementer cover edge cases (priority ordering, dead-fiber handling,
close transitions) directly.

**API to build.** All methods thread-safe; all queues/maps guarded by a
single internal `Mutex` + `ConditionVariable`.

```ruby
class Dispatcher
  # +num_workers+: number of per-worker queues to allocate.
  def initialize(num_workers)

  # Push a new CallMessage onto the shared queue. Returns true normally,
  # false if +close+ has been called (caller should refuse the message).
  def enqueue_call(message)

  # Push a FiberReturn/FiberException onto the queue of the worker that
  # owns the fiber. Returns true if dispatched, false if the fiber_id is
  # not registered (caller may discard — fiber likely already finished
  # or was aborted).
  def enqueue_fiber_resume(message)

  # Block until the worker has work. Priority order:
  #   1. Per-worker queue (always — these are resumes for fibers this
  #      worker owns; must be drained even after close).
  #   2. Shared queue, but only if +accept_calls+ is true.
  # Returns one of:
  #   [:resume, message]  – from per-worker queue
  #   [:call,   message]  – from shared queue
  #   [:closed, nil]      – returned exactly once per worker, the first
  #                         time the worker would otherwise have blocked
  #                         after +close+ has been called. Used to wake
  #                         the worker so it can transition to draining
  #                         state. Subsequent calls behave normally.
  # If +accept_calls+ is false and the per-worker queue is empty, blocks
  # until a resume arrives in the per-worker queue (workers in draining
  # state should call with +accept_calls: false+ and exit on their own
  # once their local pending hash is empty — see 2.1b for caller logic).
  def dequeue(worker_num, accept_calls:)

  # Atomically associate +fiber_id+ with +worker_num+ so subsequent
  # +enqueue_fiber_resume+ calls land on the right worker queue.
  def register_fiber(fiber_id, worker_num)

  # Remove the fiber→worker mapping. Idempotent.
  def unregister_fiber(fiber_id)

  # Mark closed and wake all blocked workers. Idempotent.
  def close
end
```

**Unit tests** (new file `test/test_dispatcher.rb`). Use background threads
with bounded `with_timeout` joins to assert blocking behavior. Cover at
least:
1. `enqueue_call` then `dequeue(0, accept_calls: true)` returns
   `[:call, message]`.
2. `register_fiber(fid, 1)` + `enqueue_fiber_resume(msg with fid)` makes
   `dequeue(1, ...)` return `[:resume, msg]`; `dequeue(0, ...)` does NOT
   see it (per-worker isolation).
3. Per-worker queue takes priority: with both a queued call and a queued
   resume for worker 0, `dequeue(0, accept_calls: true)` returns the
   resume first.
4. `enqueue_fiber_resume` for an unregistered fiber returns false and
   does not block any worker.
5. `accept_calls: false` causes worker to ignore the shared queue: with a
   pending call and no resume, `dequeue(0, accept_calls: false)` blocks;
   adding a resume for worker 0 unblocks it with `[:resume, ...]`.
6. After `close`, `enqueue_call` returns false; the next `dequeue` for
   each worker returns `[:closed, nil]` exactly once; further `dequeue`
   calls revert to normal blocking/return behavior (so workers can still
   drain resumes).
7. After `close`, a worker blocked in `dequeue` is woken with
   `[:closed, nil]`.
8. Concurrency stress: N producer threads enqueue M calls each, K worker
   threads dequeue and count; total dequeued equals N*M.

**Commit:** `feat: Add Dispatcher class for fiber-aware work routing`

### 2.1b Wire Dispatcher into threaded server (deadlock fix)

**Red.** Test (in `test/test_wrapper.rb`): `threads: 2`, depth-3 re-entrant
call (analogous to the sequential test from 1.2), completes within 2s.
Currently deadlocks (all worker threads blocked on temp reply ports).

**Green.** Replace threaded-mode plumbing with `Dispatcher`.

- Remove `@queue` (the existing `::Thread::Queue`). Add `@dispatcher =
  Dispatcher.new(threads)` in `Server#initialize` for threaded mode.
- Rewrite `worker_thread` (rename to `worker_loop` per design naming).
  Per-worker local state: `pending = {}` (fiber_id → fiber), `stopping =
  false`. Loop:
  ```
  loop do
    break if stopping && pending.empty?
    case @dispatcher.dequeue(worker_num, accept_calls: !stopping)
    in [:call, message]
      start a new fiber for this CallMessage (see start_method_fiber);
      register fiber_id with @dispatcher; on completion unregister.
    in [:resume, message]
      look up fiber in pending; resume with message; on completion
      delete from pending and unregister with @dispatcher.
    in [:closed, nil]
      stopping = true
    end
  end
  ```
  Each worker still sends `WorkerStoppedMessage` on exit (existing
  pattern preserved).
- Make `make_block` use the fiber path in threaded mode too. Remove the
  `use_fiber_path = !@threads_requested` gate from step 1.3 — replace
  with unconditional `use_fiber_path = true`. The hybrid fiber-current
  check from 1.3 still applies.
- Update `Server#main_loop` threaded dispatch:
    - `CallMessage` → `@dispatcher.enqueue_call(message)`. (Shouldn't
      fail in the running phase, but if it returns false, refuse.)
    - `FiberReturnMessage`/`FiberExceptionMessage` →
      `@dispatcher.enqueue_fiber_resume(message)`. If it returns false
      (fiber not registered), log and discard.
- `start_method_fiber` becomes per-worker (called from inside
  `worker_loop` rather than from `main_loop`). The sequential branch of
  `main_loop` still calls it directly.

**Commit:** `feat: Multi-queue fiber dispatch in threaded mode`

### 2.2 Graceful stop with suspended fibers (threaded)

**Red.** Test: same shape as 1.5 but with `threads: 2`. (Block holds a latch;
test calls `async_stop`; new call refused with `StoppedError`; latch released;
original call returns its value.)

**Green.** On `StopMessage` in threaded mode, call `@dispatcher.close` and
replace the existing `stop_workers` body (which closed `@queue`) with a
threaded-stopping phase. The per-worker logic from 2.1b already handles the
`[:closed, nil]` signal: each worker sets `stopping = true` and only exits
once its local `pending` is empty. The main loop continues routing
`FiberReturnMessage`/`FiberExceptionMessage` to per-worker queues during the
stopping phase via `@dispatcher.enqueue_fiber_resume`. New `CallMessage`s
arriving during stopping are refused via `refuse_method` (the main-loop
stopping phase already does this in current code; keep that behavior).

**Commit:** `feat: Drain pending fibers during graceful stop (threaded mode)`

### 2.3 Concurrent callers with re-entrant blocks

**Red.** Test: multiple threads in the test process simultaneously call
methods-with-blocks that re-enter the wrapper (`threads: 2`). All complete
with correct results within 5s.

No production code change expected after 2.1b/2.2; this is contention
coverage. Validates that `Dispatcher` correctly serializes shared-queue
access and per-worker dispatch under concurrent load.

**Commit:** `test: Cover concurrent re-entrant calls in threaded mode` (only
if changes were needed; otherwise fold the test into 2.2's commit)

### 2.4 Worker thread crash with pending fibers

**Red.** Test: arrange for a worker to crash (e.g., wrap an object whose
method explodes after the block has yielded) while another fiber on the same
worker is suspended; verify the suspended fiber's caller receives
`CrashedError`.

**Green.** In `worker_loop`'s `ensure` block, before sending
`WorkerStoppedMessage`:
1. Capture a `CrashedError` describing the worker's failure.
2. Call `abort_pending_fibers(pending, error)` (the helper already exists
   from step 1.6 — reuse it).
3. Unregister each aborted fiber from `@dispatcher`.

Each aborted fiber's existing `handle_method` rescue chain sends the
exception to its own `reply_port`.

**Commit:** `feat: Abort fibers in crashed worker thread`

### 2.5 Main-thread force-terminate after worker crash

**Red.** Test: main loop receives an unexpected `WorkerStoppedMessage` while
other workers have suspended fibers; those fibers' callers should receive
`CrashedError`.

**Green.** Extend `crash_cleanup` (threaded branch) to call
`@dispatcher.close` so that all remaining workers are woken with
`[:closed, nil]`. Each worker enters draining state; if its `pending` is
empty it exits cleanly, otherwise its own `ensure` block (from 2.4) aborts
its remaining fibers. `crash_cleanup` then `join`s the worker threads via
`join_workers_after_crash` (existing helper).

If any registered fibers remain (because their owning worker died without
draining — should not happen normally, but defensively): also call
`abort_pending_fibers` on any fibers still tracked by main-side state. The
implementer should check whether the design needs main-side fiber tracking
in addition to per-worker tracking; if every fiber is owned by exactly one
worker and that worker handles its own cleanup, no main-side abort is
needed.

**Commit:** `feat: Force-terminate remaining fibers on worker crash`

---

## Phase 3 — Functional coverage (no protocol changes)

### 3.1 Add tests from design §Test Plan items 7, 8, 9, 10, 14:
- Block return values pass through fiber suspend/resume.
- Exception in block propagates through fiber path.
- Multiple yields from one method (each with re-entrant call).
- Move semantics through fiber path (`block_arguments: :move`,
  `block_results: :move`, including `Ractor::MovedError` on the sender).
- `block_environment: :wrapped` regression — shareable proc still runs in
  server context, no `FiberYieldMessage` involvement.

These should pass without code changes; if any fails, fix and bundle into the
same commit.

**Commit:** `test: Functional coverage for fiber suspend/resume`

---

## Phase 4 — Polish

### 4.1 Logging

Add `fiber_id:` keyword to `Server#maybe_log` and pass it at:
- Main loop, when dispatching `BlockReturn`/`BlockException` and resuming
  fibers.
- `fiber_yield_block`, when sending `FiberYieldMessage` and after `Fiber.yield`
  returns.
- `abort_pending_fibers`, when raising into suspended fibers.
- Worker loop, when starting and resuming fibers.

Caller-side `Wrapper#maybe_log` is unchanged — `transaction` is sufficient for
correlation.

**Commit:** `feat: Include fiber_id in server log metadata`

### 4.2 YARD documentation updates

Per design §"Documentation (YARD) Updates":
1. Class-level: re-entrant calls from blocks now safe; brief mention of fiber-
   based execution model with a short example.
2. `configure_method` / `block_environment`: hybrid fallback caveat (Enumerator
   / spawned thread → blocking path → re-entrant deadlock risk unchanged) and
   guidance.
3. `threads:` parameter: no longer needs to be sized to nesting depth.
4. `async_stop` / `join`: graceful stop allows suspended block-yielding calls
   to complete; new calls refused with `StoppedError`.
5. `CrashedError`: can result from a crash while a method is suspended at a
   block-yield point.

**Commit:** `docs: Document fiber-based execution and re-entrant call support`

### 4.3 Final CI

Run `toys ci`. Fix anything that breaks; commit only if fixes are needed.

---

## Out-of-scope / explicit non-changes

- No README changes.
- No benchmark changes (none mentioned in design).
- No `WorkerStoppedMessage` schema change — it does not need to carry fiber
  IDs since each worker resolves its own fibers before reporting.
- No changes to `make_block_arg` (caller side) — the choice between fiber and
  blocking yield is made entirely by `Server#make_block`.
- Fibers in the blocking-fallback path retain the original deadlock risk for
  re-entrant calls; this is documented (step 1.4) rather than fixed.

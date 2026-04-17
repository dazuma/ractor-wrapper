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
  `BlockReturnMessage(:fiber_id, :value)`,
  `BlockExceptionMessage(:fiber_id, :exception)`.
- Update caller side (`Wrapper#call`, `handle_yield`) to handle both
  `FiberYieldMessage` and `BlockingYieldMessage` — fiber path sends
  `BlockReturn`/`BlockException` to `@port`; blocking path sends
  `Return`/`Exception` to `message.reply_port` (unchanged).
- Rewrite `Server#make_block` to use the fiber path (no hybrid check yet — the
  basic test goes through this path correctly).
- Update sequential-mode `Server#main_loop`: spawn a `Fiber` per `CallMessage`,
  store in `@pending_fibers`, and on `BlockReturnMessage`/`BlockExceptionMessage`
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
`BlockReturnMessage`/`BlockExceptionMessage` from `@port` until
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

### 2.1 Re-entry depth exceeds thread count

**Red.** Test: `threads: 2`, depth-3 re-entrant call, completes within 2s.
Currently deadlocks (all worker threads blocked).

**Green.** Convert threaded mode to multi-queue dispatch:
- Replace `@queue` with: `@multi_queue_mutex` (Mutex), `@multi_queue_cond`
  (ConditionVariable), `@shared_queue` (Array of new `CallMessage`s),
  `@thread_queues` (Array of per-worker Arrays for fiber resume items),
  `@fiber_to_worker` (Hash{fiber_id => worker_num}, guarded by the mutex).
- Rewrite `worker_thread` (rename to `worker_loop` per the design naming) per
  design §"Worker thread loop": local `pending` hash, `dequeue_work` that
  prioritizes the worker's thread queue, then the shared queue.
- Update `main_loop` threaded dispatch: `CallMessage` → `@shared_queue`;
  `BlockReturnMessage`/`BlockExceptionMessage` → `@thread_queues[worker_num]`
  via `@fiber_to_worker` lookup. If lookup misses (fiber dead), discard.

**Commit:** `feat: Multi-queue fiber dispatch in threaded mode`

### 2.2 Graceful stop with suspended fibers (threaded)

**Red.** Test: same shape as 1.5 but with `threads: 2`.

**Green.** On `StopMessage`, enqueue `nil` sentinel to all per-worker queues.
Workers receiving `nil` re-enqueue the sentinel and continue if their local
`pending` is non-empty; once empty, they exit normally and send
`WorkerStoppedMessage`. Main loop continues routing `BlockReturn`/
`BlockException` to per-worker queues during the stopping phase. Refuse new
`CallMessage`s with `StoppedError`.

**Commit:** `feat: Drain pending fibers during graceful stop (threaded mode)`

### 2.3 Concurrent callers with re-entrant blocks

**Red.** Test: multiple Ractors simultaneously call methods-with-blocks that
re-enter the wrapper (`threads: 2`). All complete with correct results.

No production code change expected after 2.1/2.2; this is contention coverage.

**Commit:** `test: Cover concurrent re-entrant calls in threaded mode` (only
if changes were needed)

### 2.4 Worker thread crash with pending fibers

**Red.** Test: arrange for a worker to crash (e.g., wrap an object whose method
does something that explodes inside the fiber) while another fiber on the same
worker is suspended; verify the suspended fiber's caller receives
`CrashedError`.

**Green.** Worker `ensure` block calls `abort_pending_fibers(pending)` before
sending `WorkerStoppedMessage`. Each aborted fiber's own rescue chain sends an
`ExceptionMessage` to its `reply_port`.

**Commit:** `feat: Abort fibers in crashed worker thread`

### 2.5 Main-thread force-terminate after worker crash

**Red.** Test: main loop receives unexpected `WorkerStoppedMessage` while
other workers have suspended fibers; those fibers' callers should receive
`CrashedError`.

**Green.** Extend `crash_cleanup` (threaded branch) so that when forcing
termination of remaining workers, their pending fibers are also aborted (most
naturally: workers' own ensure blocks already do this when their threads exit;
verify the path triggers correctly when main signals shutdown via
`@multi_queue_cond` cascade or queue closure).

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

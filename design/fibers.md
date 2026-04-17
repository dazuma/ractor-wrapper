# Design: Fiber-Based Server Execution

## Problem

`Ractor::Wrapper` can deadlock when a wrapped object's method yields to a block
that, in turn, calls another method on the same wrapper. Reproducing example:

```ruby
wrapper = Ractor::Wrapper.new(MyObject.new)
stub = wrapper.stub

# A wrapped object whose method yields
class MyObject
  def each_item(&block)
    ["a", "b"].each { |item| yield item }
  end

  def process(item)
    item.upcase
  end
end

# This deadlocks in sequential mode
wrapper.stub.each_item do |item|
  wrapper.stub.process(item)   # <- second call into same wrapper
end
```

### Why it deadlocks

In sequential mode, the server's main loop calls `handle_method`, which calls
`make_block` to build a proxy proc. When the wrapped object yields, the proc:

1. Creates a fresh `reply_port`
2. Sends a `YieldMessage` (carrying `reply_port`) to the caller's reply port
3. **Blocks on `reply_port.receive`** waiting for the block result

While blocked in step 3, the server is not reading from `@port`. The block the
caller runs tries to send a new `CallMessage` to `@port`; that message sits
unread. The caller waits for that method to complete. The server waits for the
caller's block result. Neither can make progress — deadlock.

In threaded mode the same deadlock occurs whenever the nesting depth of yields
exceeds the number of worker threads.

## Goal

Allow a wrapper to handle an arbitrary number of simultaneously live method
calls, each of which may be suspended at any block-yield point, without
deadlocking and without requiring the user to configure a specific thread count.

## Proposed Solution: Fiber-Based Server Execution

Each method call is executed in a **Fiber**. When the method yields to a
caller-side block, the fiber suspends (`Fiber.yield`) instead of blocking the
server thread. The server's main thread continues reading from `@port` and can
accept new `CallMessage`s — including the ones generated from inside the
caller's block. When a block result returns from the caller, the appropriate
fiber is resumed.

---

## Protocol Changes

### Current message flow for a caller-side block yield

```
Caller                                    Server
  |-- CallMessage(reply_port) ----------->|
  |<- YieldMessage(args, reply_port2) ----|   (server blocks on reply_port2.receive)
  |   [runs block]                        |
  |-- ReturnMessage -----> reply_port2 -->|   (server unblocks)
  |<- ReturnMessage(result) --------------|
```

The server creates a one-shot `reply_port2` per block call and blocks on it.
This is what causes the deadlock.

### New message flow (fiber path)

In the common case, the server runs the method in a fiber and uses a
non-blocking yield:

```
Caller                                    Server
  |-- CallMessage(reply_port) ----------->|   (server starts Fiber F)
  |<- FiberYieldMessage(args, fiber_id) --|   (Fiber F calls Fiber.yield)
  |   [runs block, possibly calling stub] |   (main loop keeps reading @port)
  |-- FiberReturnMessage(fiber_id) ------>|   (goes through @port, not a temp port)
  |                              [resumes F]
  |<- ReturnMessage(result) --------------|   (Fiber F completes)
```

Key differences from the current flow:
- **`FiberYieldMessage` carries `fiber_id`** (the `object_id` of the fiber
  running the method) instead of a `reply_port`.
- **Block results flow back through `@port`** using new message types, rather
  than through a per-yield local port.
- The server never blocks on a per-block port; it only ever blocks (waits) on
  the single `@port` it already owns.

### Blocking fallback path

A hybrid detection mechanism (described in "Server Changes" below) falls back
to the current blocking approach when the block is invoked from a context other
than the method-handling fiber (e.g., from inside an Enumerator or a spawned
thread). In this case the message flow is unchanged from the current design,
using `BlockingYieldMessage` with a `reply_port`:

```
Caller                                              Server
  |-- CallMessage(reply_port) ----------------------->|
  |<- BlockingYieldMessage(args, reply_port2) --------|   (server blocks on reply_port2)
  |   [runs block]                                    |
  |-- ReturnMessage -----> reply_port2 -------------->|   (server unblocks)
  |<- ReturnMessage(result) --------------------------|
```

### Message type changes

**`YieldMessage` is replaced by two separate types:**

```ruby
FiberYieldMessage    = ::Data.define(:args, :kwargs, :fiber_id)
BlockingYieldMessage = ::Data.define(:args, :kwargs, :reply_port)
```

The caller-side `call` loop receives both types on `reply_port` and dispatches
to the appropriate response path based on the message class.

**New (block result messages for the fiber path):**

```ruby
FiberReturnMessage    = ::Data.define(:value, :fiber_id)
FiberExceptionMessage = ::Data.define(:exception, :fiber_id)
```

No other message types change.

---

## Caller-Side Changes (`Wrapper`)

### `handle_yield`

Currently sends the block result to `message.reply_port` (a temp port on the
server). The new design splits into two paths depending on the yield message
type:

- **`FiberYieldMessage`**: sends the block result to `@port` (the server's
  main port) as a `FiberReturnMessage`/`FiberExceptionMessage`, tagging with
  `fiber_id` so the server knows which fiber to resume.
- **`BlockingYieldMessage`**: sends `ReturnMessage`/`ExceptionMessage` directly
  to `message.reply_port` — unchanged from current behavior.

```ruby
def handle_yield(message, transaction, settings, method_name, &)
  # ... run block, collect result or exception ...
  case message
  when FiberYieldMessage
    @port.send(FiberReturnMessage.new(value: result, fiber_id: message.fiber_id),
               move: settings.block_results == :move)
  when BlockingYieldMessage
    message.reply_port.send(ReturnMessage.new(result),
                            move: settings.block_results == :move)
  end
  # (similarly for exceptions: FiberExceptionMessage vs ExceptionMessage)
end
```

### `Wrapper#call` loop

The caller's loop on `reply_port.receive` now handles both yield message types:

```ruby
loop do
  reply_message = reply_port.receive
  case reply_message
  when FiberYieldMessage, BlockingYieldMessage
    handle_yield(reply_message, transaction, settings, method_name, &)
  when ReturnMessage
    return reply_message.value
  when ExceptionMessage
    raise reply_message.exception
  end
end
```

### `make_block_arg`

No change. The caller still sends `CallMessage` with `reply_port`. The yield
message type arriving on that port depends on which path the server's
`make_block` chose.

---

## Server Changes (`Server`)

### `make_block`

The block proc uses a **hybrid approach**: it checks whether it is still
running in the fiber that started the method call. If so, it uses the new
fiber-based yield. If not (e.g., the wrapped object invoked the block from
inside a nested fiber such as an Enumerator, or from a spawned thread), it
falls back to the current blocking temp-port approach.

```ruby
def make_block(message)
  return message.block_arg unless message.block_arg == :send_block_message
  expected_fiber = ::Fiber.current   # the method-handling fiber
  proc do |*args, **kwargs|
    args.map! { |arg| arg.equal?(@object) ? @stub : arg }
    kwargs.transform_values! { |arg| arg.equal?(@object) ? @stub : arg }
    if ::Fiber.current.equal?(expected_fiber)
      fiber_yield_block(message, args, kwargs)
    else
      blocking_yield_block(message, args, kwargs)
    end
  end
end
```

**Fiber path** (`fiber_yield_block`) — the common case, used when the block is
called directly from the method's own fiber:

```ruby
def fiber_yield_block(message, args, kwargs)
  fiber_id = ::Fiber.current.object_id
  yield_message = FiberYieldMessage.new(args: args, kwargs: kwargs, fiber_id: fiber_id)
  message.reply_port.send(yield_message, move: message.settings.block_arguments == :move)
  reply = ::Fiber.yield          # suspend; resumed by main loop with a result message
  case reply
  when FiberExceptionMessage then raise reply.exception
  when FiberReturnMessage    then reply.value
  end
end
```

**Blocking path** (`blocking_yield_block`) — fallback for nested-fiber and
cross-thread invocations. This is essentially the current `make_block`
implementation, preserved unchanged:

```ruby
def blocking_yield_block(message, args, kwargs)
  reply_port = ::Ractor::Port.new
  reply_message = begin
    yield_message = BlockingYieldMessage.new(args: args, kwargs: kwargs, reply_port: reply_port)
    message.reply_port.send(yield_message, move: message.settings.block_arguments == :move)
    reply_port.receive
  ensure
    reply_port.close
  end
  case reply_message
  when ExceptionMessage then raise reply_message.exception
  when ReturnMessage    then reply_message.value
  end
end
```

The blocking path retains the original deadlock risk for re-entrant wrapper
calls, but only in cases that cannot benefit from fiber-based yielding (the
block is running in a fiber or thread we don't control). This preserves
existing behavior for those edge cases rather than introducing a regression.

#### Why the hybrid is necessary

A wrapped object might invoke the caller's block from inside a nested fiber
(e.g., an Enumerator's generator block). In that context, `Fiber.current` is
the Enumerator's internal fiber, not the method-handling fiber.
`Fiber.yield` would yield the wrong fiber, causing silent corruption.
Similarly, if the block is invoked from a spawned thread, `Fiber.current` is
that thread's root fiber. The `expected_fiber` check detects both cases and
routes to the safe fallback.


### Sequential mode: `main_loop` and fiber tracking

The server maintains a hash `@pending_fibers : { Integer => Fiber }` mapping
`fiber_id` to the suspended fiber.

```
main_loop:
  loop do
    message = @port.receive
    case message
    when CallMessage
      fiber = Fiber.new { handle_method(message) }
      @pending_fibers[fiber.object_id] = fiber
      fiber.resume
      @pending_fibers.delete(fiber.object_id) unless fiber.alive?
    when FiberReturnMessage, FiberExceptionMessage
      fiber = @pending_fibers[message.fiber_id]
      if fiber
        fiber.resume(message)
        @pending_fibers.delete(message.fiber_id) unless fiber.alive?
      end
    when StopMessage  -> break
    when JoinMessage  -> queue join port
    end
  end
```

After each `fiber.resume`, if the fiber's alive it has re-suspended (another
yield); if dead it completed. In either case the fiber sends its own response
before dying, so no explicit result-routing is needed after the final resume.

### Threaded mode: multi-queue dispatch and fiber affinity

**The key constraint:** Ruby fibers cannot be resumed from a different thread
than the one that last resumed them (Ruby raises `FiberError` if attempted).
This means fibers have **thread affinity** — a fiber started in worker thread N
must always be resumed in worker thread N.

#### Multi-queue design

Work is divided across two kinds of queues:

- **One shared queue** for new `CallMessage`s — any idle worker may take work
  from here.
- **N thread-specific queues** (one per worker) for fiber resume items
  (`FiberReturnMessage`/`FiberExceptionMessage`) — routed to the worker that
  owns the fiber.

The main loop's dispatch rule is simple: new jobs go to the shared queue; block
results go to the thread-specific queue of the worker that owns the fiber.

Workers **prioritize** their thread-specific queue (finishing in-progress calls
is preferred over starting new ones), then fall back to the shared queue.

#### Why not per-worker queues for everything?

An earlier design variant routed all work — including new jobs — to per-worker
queues, with the main thread tracking a `@worker_load` metric to balance
assignment. This has a serious flaw: a worker with many *suspended* fibers
(which consume no CPU) would still appear "loaded" and stop receiving new jobs,
while idle workers sit unused. The multi-queue approach avoids this entirely —
idle workers naturally compete for jobs in the shared queue regardless of how
many fibers they have suspended.

#### Waiting on two queues simultaneously

Ruby has no `select` for `Queue`. The dual-queue wait is implemented with a
shared `Mutex` and `ConditionVariable`:

```ruby
# Shared state
@multi_queue_mutex = Mutex.new
@multi_queue_cond  = ConditionVariable.new
@shared_queue      = []                          # new CallMessage jobs
@thread_queues     = Array.new(n) { [] }         # fiber resume items, per worker
```

Enqueue operations (called from main thread):

```ruby
# New job:
@multi_queue_mutex.synchronize do
  @shared_queue << call_message
  @multi_queue_cond.signal
end

# Fiber resume:
@multi_queue_mutex.synchronize do
  @thread_queues[worker_num] << resume_item
  @multi_queue_cond.signal
end
```

Worker dequeue (blocks until work is available):

```ruby
def dequeue_work(worker_num)
  @multi_queue_mutex.synchronize do
    loop do
      if (item = @thread_queues[worker_num].shift)
        # If shared work is also waiting, wake another worker for it
        @multi_queue_cond.signal unless @shared_queue.empty?
        return [:resume, item]
      end
      if (item = @shared_queue.shift)
        @multi_queue_cond.signal unless @shared_queue.empty?
        return [:new_job, item]
      end
      @multi_queue_cond.wait(@multi_queue_mutex)
    end
  end
end
```

The `@multi_queue_cond.signal unless @shared_queue.empty?` cascade ensures
that when a worker takes a thread-specific item and leaves shared work behind,
another sleeping worker wakes to claim it.

#### Data structures

```ruby
@multi_queue_mutex  # Mutex  — guards @shared_queue and @thread_queues
@multi_queue_cond   # ConditionVariable
@shared_queue       # Array — new CallMessage jobs
@thread_queues      # Array<Array> — per-worker fiber resume items
@fiber_to_worker    # Hash{ fiber_id => worker_num }; protected by @multi_queue_mutex
# @pending_fibers is a local variable in each worker_loop (no mutex needed)
```

#### Main loop dispatch (threaded mode)

```
when CallMessage
  @multi_queue_mutex.synchronize do
    @shared_queue << message
    @multi_queue_cond.signal
  end

when FiberReturnMessage | FiberExceptionMessage
  @multi_queue_mutex.synchronize do
    worker_num = @fiber_to_worker[message.fiber_id]
    if worker_num
      @thread_queues[worker_num] << {type: :fiber_resume, fiber_id: message.fiber_id,
                                     result: message}
      @multi_queue_cond.signal
    end
  end
```

#### Worker thread loop (threaded mode)

```ruby
def worker_loop(worker_num)
  pending = {}   # fiber_id => Fiber, local to this thread (no mutex needed)
  loop do
    type, item = dequeue_work(worker_num)
    if item.nil?
      if pending.empty?
        break                # no live fibers — shut down
      else
        requeue_sentinel(worker_num)  # defer shutdown; re-enqueue nil
        next
      end
    end
    case type
    when :new_job
      message = item
      fiber = Fiber.new do
        handle_method(message)
        @multi_queue_mutex.synchronize { @fiber_to_worker.delete(Fiber.current.object_id) }
      end
      fid = fiber.object_id
      @multi_queue_mutex.synchronize { @fiber_to_worker[fid] = worker_num }
      pending[fid] = fiber
      fiber.resume
      pending.delete(fid) unless fiber.alive?
    when :fiber_resume
      fid = item[:fiber_id]
      fiber = pending[fid]
      if fiber
        fiber.resume(item[:result])
        pending.delete(fid) unless fiber.alive?
      end
    end
  end
end
```

When a fiber calls `Fiber.yield` (suspending to wait for a block result), the
worker's `fiber.resume` returns immediately. The worker loops, calls
`dequeue_work`, and picks up whatever comes next — another fiber resume or a
brand-new job.

#### Priority invariant

The priority guarantee ("resume suspended fibers before starting new ones")
holds at each *wait point*, not globally. If a worker is actively running a
fiber (not waiting) when a resume item arrives for it, the resume item sits in
the thread-specific queue until the current fiber either completes or suspends
via `Fiber.yield`. This is inherent to cooperative fiber scheduling and is not
a bug.

#### `stop_workers` change

Upon receiving `StopMessage`, the main loop enqueues the shutdown sentinel
(`nil`) to all worker thread-specific queues immediately, then continues
routing `FiberReturnMessage`/`FiberExceptionMessage`s to workers so that
in-progress fibers complete normally. Each worker defers its own shutdown
(re-enqueuing the sentinel) until its local fibers have all completed. See
"Graceful Stop with Pending Fibers" below for details.

---

## Graceful Stop with Pending Fibers

When the server receives a `StopMessage`, there may be fibers suspended in
`@pending_fibers` waiting for block results. A graceful stop must **not** abort
these fibers — it should allow all in-progress method calls to complete
normally. Only new `CallMessage`s are refused.

### Sequential mode

After receiving `StopMessage`, the main loop stops accepting new `CallMessage`s
but continues processing `FiberReturnMessage`/`FiberExceptionMessage` messages
to resume pending fibers. It exits only once all fibers have completed (i.e.,
`@pending_fibers` is empty).

### Threaded mode

The main loop enters the stopping phase: it refuses new `CallMessage`s but
continues routing `FiberReturnMessage`/`FiberExceptionMessage` to the
appropriate worker thread-specific queues.

Upon receiving `StopMessage`, the main loop enqueues the shutdown sentinel
(`nil`) to **all** worker thread-specific queues immediately. Each worker,
when it pulls the sentinel from its queue, checks whether it has any live
fibers in its local `pending` hash. If it does, it **re-enqueues the sentinel**
to defer shutdown and continues processing fiber resumes. Once all fibers have
completed, the worker eventually pulls the sentinel again, finds no live
fibers, and terminates normally.

This avoids a coordination problem: the main loop blocks on `@port.receive`
and has no way to be notified when a specific worker's fibers have all
completed. By letting each worker manage its own shutdown timing, the main
loop does not need to track per-worker fiber counts for the purpose of
graceful stop.

## Forced Termination of Pending Fibers

In abnormal situations — a worker thread crash, a main server crash, or the
main server needing to force-terminate because a worker died unexpectedly —
pending fibers cannot complete normally and must be aborted.

### `abort_pending_fibers`

Use `Fiber#raise` to inject an error into each suspended fiber. The fiber
unwinds, and `handle_method`'s rescue chain sends an `ExceptionMessage` to the
caller's `reply_port` before dying.

```ruby
def abort_pending_fibers(pending)
  pending.each_value do |fiber|
    fiber.raise(CrashedError.new("Server terminated while method was blocked on block result"))
  rescue FiberError
    # fiber already dead
  end
end
```

This is called only in crash/forced-termination paths:
- In sequential mode: from `crash_cleanup` when an exception escapes the main
  loop.
- In threaded mode: in a worker thread's ensure block when the worker is
  terminating due to a crash (before sending `WorkerStoppedMessage`).
- In threaded mode: when the main thread force-terminates remaining workers
  after receiving an unexpected `WorkerStoppedMessage`.

**Orphaned block results:** When fibers are aborted, a caller whose fiber was
just killed may still send a `FiberReturnMessage` to `@port` (the caller was
mid-block and hadn't yet received the `ExceptionMessage`). The main loop's
dispatch handles this naturally: the `@fiber_to_worker` lookup returns nil for
the dead fiber, and the message is discarded. No special mechanism is needed.

---

## Crash Handling Changes

### Sequential mode

If an exception escapes `main_loop` (server crash), `@pending_fibers` may be
non-empty. `crash_cleanup` must drain and abort them. Since `@crash_exception`
is set, we can use the same `StoppedError` or a `CrashedError` approach.

### Threaded mode

**Worker thread crash:**
Currently, if a worker crashes, `WorkerStoppedMessage` triggers the main thread
to bail. Under the fiber model, a worker crash leaves behind all fibers that
were in that worker's `pending` hash.

The worker's ensure block (which sends `WorkerStoppedMessage`) should first
raise into all locally pending fibers using `abort_pending_fibers`. Each
fiber's own rescue chain in `handle_method` is responsible for sending an
error response to the caller — the same nested-rescue pattern already in place
at lines 1213-1223 of the current code. This means no outer layer needs access
to fiber reply ports; the fiber body already has the port in its closure.

No changes to `WorkerStoppedMessage` are needed — it does not need to carry
fiber IDs or reply ports, since the worker resolves all its fibers before
reporting.

**Main server crash:**
If the main server thread (sequential mode) or the main Ractor loop crashes
while fibers are pending, the existing `crash_cleanup` path must also call
`abort_pending_fibers`. As with the worker case, each fiber handles its own
caller notification via its rescue chain.

---

## Other Considerations

### Nested yields (multiple block invocations)

A method that yields multiple times (e.g., `Enumerable#each`) works correctly:
each invocation of `make_block`'s proc produces one `Fiber.yield` / resume
cycle. The fiber remains in `@pending_fibers` throughout, re-suspending each
time and being re-resumed each time a block result arrives.

### `block_environment: :wrapped`

When `block_arg` is a shareable proc (not `:send_block_message`), `make_block`
returns it directly. This path is unaffected by the fiber changes.

### Logging

With multiple fibers in flight, log output from the server can interleave
messages from different method calls. The server's `maybe_log` method currently
accepts `call_message:`, `worker_num:`, `transaction:`, and `method_name:` as
metadata keywords. A `fiber_id:` keyword should be added:

```ruby
def maybe_log(str, call_message: nil, worker_num: nil, fiber_id: nil,
              transaction: nil, method_name: nil)
  return unless @enable_logging
  transaction ||= call_message&.transaction
  method_name ||= call_message&.method_name
  metadata = [::Time.now.utc.strftime("%Y-%m-%dT%H:%M:%S.%L"), "Ractor::Wrapper:#{@name}"]
  metadata << "Worker:#{worker_num}" if worker_num
  metadata << "Fiber:#{fiber_id}" if fiber_id
  metadata << "Transaction:#{transaction}" if transaction
  metadata << "Method:#{method_name}" if method_name
  metadata = metadata.join(" ")
  $stderr.puts("[#{metadata}] #{str}")
  $stderr.flush
rescue ::StandardError
  # Swallow any errors during logging
end
```

The `fiber_id` should be passed:
- In the main loop, when dispatching `FiberReturnMessage`/
  `FiberExceptionMessage` and resuming fibers.
- In `fiber_yield_block`, when sending `FiberYieldMessage` and when resuming
  from `Fiber.yield`.
- In `abort_pending_fibers`, when raising into suspended fibers.
- In the worker loop, when starting or resuming fibers.

The caller-side `maybe_log` (on `Wrapper`) does not need `fiber_id` — it
doesn't know or care about the server's fibers. Its existing `transaction`
metadata is sufficient to correlate caller-side and server-side log entries.

### Ractor sharability

`Fiber.current.object_id` is an `Integer`, which is shareable. Fiber objects
themselves are not shareable, but they're only stored in thread-local data
structures (or within a single Ractor's server thread). No `Ractor.make_shareable`
calls are needed.

---

## Documentation (YARD) Updates

The following user-facing documentation items must be added or updated. These
are listed here so the implementor does not forget to document behavior that
users need to understand.

### Class-level documentation (`Ractor::Wrapper`)

1. **Re-entrant calls from blocks.** Document that a caller-side block may
   safely call methods on the same wrapper without deadlocking. This is the
   primary user-visible improvement. Include a short example showing the
   pattern that previously deadlocked and now works.

2. **Fiber-based execution model.** Briefly explain that the server uses fibers
   to handle method calls, and that when a method yields to a caller-side
   block, the server remains available to process other calls. Users don't need
   to understand the implementation details, but should know that this is what
   enables re-entrant calls.

### `block_environment` parameter / `configure_method`

3. **Hybrid fallback caveat.** Document that the fiber-based (non-blocking)
   yield path is used only when the wrapped object yields to the block directly
   from the method's own execution context. If the wrapped object invokes the
   block from within a **nested fiber** (e.g., inside an Enumerator's generator
   block) or from a **spawned thread**, the system falls back to a blocking
   implementation. In the blocking fallback, re-entrant calls from the block
   back into the same wrapper **will deadlock**, the same as in prior versions.

4. **Guidance on wrapped objects that yield from internal fibers/threads.**
   Advise users that if a wrapped object is known to invoke blocks from a
   nested fiber or thread, they should avoid making re-entrant wrapper calls
   from within that block, or consider restructuring the wrapped object to
   yield directly.

### `threads:` parameter

5. **Thread count and re-entrant calls.** Update to clarify that the thread
   count no longer needs to be sized to the nesting depth of re-entrant block
   calls. Previously, N levels of nesting required at least N worker threads to
   avoid deadlock. With fiber-based execution, any thread count works for
   re-entrant calls through the fiber path.

### `async_stop` / `join`

6. **Graceful stop with in-progress calls.** Document that `async_stop` allows
   all currently in-progress method calls to complete before the wrapper
   terminates, including calls that are suspended at a block-yield point
   waiting for a block result from the caller. New calls are refused
   immediately with `StoppedError`.

### Error classes

7. **`CrashedError` from pending fibers.** Document that if the server crashes
   (or a worker thread terminates unexpectedly) while a method call is
   suspended at a block-yield point, the caller receives a `CrashedError`
   rather than a normal return value. This is consistent with the existing
   crash behavior for non-fiber calls, but the user should understand that a
   crash during a block yield is possible and produces this error.

---

## Test Plan

### Testing approach for deadlock scenarios

The failure mode for a deadlock regression is a hung test, not a failed
assertion. To handle this, deadlock tests use a timeout-with-backtrace pattern:

```ruby
it "does not deadlock on re-entrant calls" do
  result = nil
  thread = Thread.new { result = stub.each_item { |x| stub.process(x) } }
  unless thread.join(2)
    backtrace = thread.backtrace&.join("\n")
    thread.kill
    flunk "Deadlocked. Thread backtrace:\n#{backtrace}"
  end
  assert_equal expected, result
end
```

A 2-second timeout is generous enough to avoid false failures on loaded CI
machines, while keeping test runs tolerable if multiple deadlock tests regress
(each adds at most 2 seconds).

### Deadlock scenario tests

These tests verify that previously-deadlocking patterns now complete. Each
requires the timeout wrapper described above.

1. **Sequential mode: basic re-entrant call.** A method yields to a block that
   calls another method on the same wrapper. This is the core issue #12
   scenario.

2. **Sequential mode: nested re-entry.** A method yields to a block that calls
   a second method, which itself yields to a block that calls a third method.
   Exercises multiple simultaneously suspended fibers.

3. **Threaded mode: re-entrant call.** Same as (1) but with `threads: N`.
   Verifies that the multi-queue dispatch correctly handles fiber resumes
   across worker threads.

4. **Threaded mode: re-entry depth exceeds thread count.** With `threads: 2`,
   trigger 3+ levels of re-entrant calls. Under the old design all threads
   would be blocked; with fibers this should complete.

### Hybrid fallback tests

These verify the blocking-path fallback for cases where the block is invoked
from a context other than the method-handling fiber.

5. **Block invoked from nested fiber (Enumerator).** A method invokes the
   caller's block from inside an Enumerator's generator. Should work for
   simple (non-re-entrant) blocks, confirming no regression from the fallback
   path. Does not require the timeout wrapper — it either works or raises.

6. **Fallback path with re-entrant call still deadlocks.** (Negative test /
   documentation test.) A method invokes the caller's block from inside an
   Enumerator, and the block tries to call back into the wrapper. This hits
   the blocking fallback and deadlocks — same as the current behavior. Test
   with a short timeout to confirm it does NOT complete, verifying the
   limitation is understood and unchanged.

### Functional tests (no deadlock risk)

These test correctness of values and error propagation through the fiber
suspend/resume path. Standard assertion-based tests, no timeout needed.

7. **Block return values pass through fiber suspend/resume.** A method yields
   several values; the block transforms each; the method collects and returns
   results. Assert all values are correct.

8. **Exception in block propagates through fiber path.** A block raises an
   exception. The method should receive it (or it should propagate to the
   caller, depending on the method's error handling).

9. **Multiple yields from one method.** An `each`-style method yields many
   times, each with a re-entrant call in the block. All iterations complete
   with correct values.

10. **Move semantics through fiber path.** Configure a method with
    `block_arguments: :move` and `block_results: :move`. Verify that moved
    objects are properly transferred through the fiber suspend/resume cycle:
    block arguments arrive at the caller, block results arrive back at the
    server, and the sender cannot access the moved objects afterward (raises
    `Ractor::MovedError`).

11. **Graceful stop with suspended fibers.** Issue `async_stop` while a fiber
    is suspended waiting for a block result. The in-progress call should
    complete normally with the correct return value — not raise `StoppedError`.
    New calls issued after the stop should raise `StoppedError`.

12. **Crash with suspended fibers.** Force a server crash while fibers are
    suspended. All pending callers should receive `CrashedError`.

13. **Concurrent callers with re-entrant blocks (threaded mode).** Multiple
    callers simultaneously invoke methods-with-blocks that re-enter the
    wrapper. All calls complete with correct results. Exercises the
    multi-queue under contention.

14. **`block_environment: :wrapped` regression.** A method configured with
    `block_environment: :wrapped` receives a shareable proc that executes
    in the server's context. Verify this path is unaffected by the fiber
    changes — the block runs directly in the fiber without any
    `FiberYieldMessage` / `Fiber.yield` involvement, and returns the correct
    result.


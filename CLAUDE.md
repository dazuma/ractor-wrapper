# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

This project uses [Toys](https://dazuma.github.io/toys) for task management (not Rake).

```bash
# Run full CI pipeline
toys ci

# Run individual components
toys test      # Tests only
toys rubocop   # Linting and code style only
toys yardoc    # Documentation only
toys build     # Build gem only

# Run a single test file
toys test test/test_wrapper.rb
```

## Architecture

The entire library lives in `lib/ractor/wrapper.rb`. The public entry point is `Ractor::Wrapper`, which wraps a non-shareable object and exposes it to other Ractors via a shareable `Stub` proxy.

### Core classes

- **`Ractor::Wrapper`** — Public API. Wraps an object and manages its lifecycle. Accepts options like `use_current_ractor:`, `threads:`, `name:`, and per-method settings via `configure_method`.
- **`Ractor::Wrapper::Stub`** — Frozen, shareable proxy passed to other Ractors. Uses `method_missing` to forward calls back to the wrapper via message passing.
- **`Ractor::Wrapper::MethodSettings`** — Frozen configuration controlling copy vs. move semantics for arguments and return values, and block handling behavior.
- **`Ractor::Wrapper::Server`** — Private backend. Receives `CallMessage` objects and dispatches them to the wrapped object, then returns results via `ReturnMessage`, `ExceptionMessage`, or one of the yield message types (`FiberYieldMessage` or `BlockingYieldMessage`).

### Two execution modes

1. **Isolated mode** (default) — the wrapped object is moved into a new Ractor. Other Ractors interact with it through the Stub. After `join`, the object can be recovered via `recover_object`.
2. **Local mode** (`use_current_ractor: true`) — the server runs as Thread(s) inside the current Ractor. The object is never moved. Used for objects that cannot be transferred between Ractors (e.g., SQLite3 connections).

### Concurrency within the server

- **Sequential** (default) — one call at a time, no worker threads.
- **Concurrent** — multiple worker threads (set via `threads:`), for thread-safe wrapped objects.

### Message protocol

All inter-Ractor communication uses frozen message structs defined in the file: `CallMessage`, `ReturnMessage`, `ExceptionMessage`, `FiberYieldMessage`, `BlockingYieldMessage`, `FiberReturnMessage`, `FiberExceptionMessage`, `StopMessage`, `JoinMessage`, `WorkerStoppedMessage`. Block calls round-trip via one of two paths:

- **Fiber-suspend path** (most cases): the server sends a `FiberYieldMessage` (carrying the `fiber_id` of the suspended method-handling fiber) to the caller Ractor. The caller executes the block and sends a `FiberReturnMessage` or `FiberExceptionMessage` back to the server's main port; the main loop looks up the fiber by id and resumes it with the reply.
- **Blocking-fallback path** (nested-fiber/spawned-thread cases): the server allocates a temporary reply port, sends a `BlockingYieldMessage` carrying that port, and blocks on it. The caller responds with a `ReturnMessage` or `ExceptionMessage` directly to that temporary port. This path can deadlock under re-entrant calls but is preserved where the fiber-suspend path is not safe.

### Lifecycle

1. Wrapper starts → Server enters **running** phase (accepts calls).
2. `async_stop` or `stop` called → Server enters **stopping** phase (rejects new calls, drains workers).
3. All workers finish → Server enters **cleanup** phase and shuts down.
4. `join` returns → In isolated mode, `recover_object` retrieves the wrapped object.

## Code Style

- Ruby 4.0+ target
- Double-quoted strings (`Style/StringLiterals: double_quotes`)
- Trailing commas in multiline arrays and hashes
- Bracket-style symbol and word arrays (`[:foo, :bar]` not `%i[foo bar]`)
- Max line length: 120
- `Style/DocumentationMethod: Enabled` — public methods require YARD docs
- Tests use Minitest spec style with assertions (not expectations)
- Top-level constants must be prefixed with `::` (e.g. `::File`, `::Regexp`, `::Gem::Version`) to avoid ambiguous resolution within nested namespaces. Relative constants defined within the current namespace should not be prefixed. Note that Kernel method calls such as `Array(x)`, `Integer(x)`, `Float(x)` look like constants but are not and do not get the prefix.

## Testing

- Minitest spec style: `describe`/`it` blocks with `assert_*` assertions (not expectations)
- Test files follow the `test_*.rb` naming convention

## General coding instructions

- Unless instructed otherwise, always use red-green test-driven development when making code changes. For each step in a coding task, first write tests and confirm they fail. Then write code to make the tests pass.
- Unless instructed otherwise, always git commit after a step is complete and the tests pass.
- Conventional Commits format required (`fix:`, `feat:`, `docs:`, etc.)
- Prefer Ruby for any one-off scripts you need to write as part of your work.

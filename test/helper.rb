# frozen_string_literal: true

require "minitest/autorun"
require "minitest/focus"
require "minitest/rg"
require "ractor/wrapper"

# Create and throw away an initial Ractor, which will cause some versions of
# Ruby to emit a warning. The intent is to trigger that warning early so it's
# out of the way and does not interfere with test output later on.
::Ractor.new {} # rubocop:disable Lint/EmptyBlock

# A JoinMessage subclass used in tests to crash the isolated server: main_loop
# accesses reply_port on the received message, and this override raises, causing
# the exception to propagate out of main_loop and trigger crash cleanup.
class CrashingJoinMessage < ::Ractor::Wrapper::JoinMessage
  def reply_port
    raise "simulated crash"
  end
end

class RemoteObject
  def echo_args(*args, **kwargs)
    "#{args}, #{kwargs}"
  end

  def object_and_id(arg)
    [arg, arg.object_id]
  end

  def run_block(*, **)
    yield(*, **)
  end

  def run_block_with_id(obj)
    yield(obj, obj.object_id)
  end

  def whoops
    raise "Whoops"
  end

  def slow_echo(arg)
    sleep(1)
    arg
  end

  def return_self
    self
  end

  def block_args_self(obj1, obj2)
    yield(obj1, self, kwobj: obj2, kwself: self)
  end

  def each_item(items, &)
    items.each(&)
  end

  # Iterates +items+ inside an Enumerator's generator block, calling the
  # caller-side block from within the generator's internal fiber. Uses
  # +next+ (rather than +each+ with a block) to force the generator to run
  # in a separate fiber. Used to exercise the hybrid fallback path: the
  # proxy proc must detect that it is no longer running in the method-
  # handling fiber and avoid Fiber.yield.
  def each_via_generator(items, &block)
    results = []
    enum = ::Enumerator.new do |y|
      items.each { |item| y << block.call(item) }
    end
    loop { results << enum.next }
    results
  end
end

# Helpers for tests that exercise scenarios that previously deadlocked.
# Include this module in a describe block to gain access to +with_timeout+.
module TimeoutHelper
  # Runs the block in a separate thread and enforces a timeout. If the thread
  # does not return within +seconds+, fails with a Minitest::Assertion that
  # includes the stuck thread's backtrace. Returns the block's value on success.
  def with_timeout(seconds = 2, &block)
    result = nil
    thread = ::Thread.new { result = block.call }
    unless thread.join(seconds)
      backtrace = thread.backtrace&.join("\n") || "(no backtrace available)"
      thread.kill
      raise ::Minitest::Assertion, "Timed out after #{seconds}s. Thread backtrace:\n#{backtrace}"
    end
    result
  end
end

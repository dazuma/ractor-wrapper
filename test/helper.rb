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
end

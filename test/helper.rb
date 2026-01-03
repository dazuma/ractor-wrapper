# frozen_string_literal: true

require "minitest/autorun"
require "minitest/focus"
require "minitest/rg"
require "ractor/wrapper"

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

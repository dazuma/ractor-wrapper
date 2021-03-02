require "minitest/autorun"
require "minitest/focus"
require "minitest/rg"
require "ractor/wrapper"

class RemoteObject
  def echo_args(*args, **kwargs)
    "#{args}, #{kwargs}"
  end

  def fail
    raise "Whoops"
  end

  def slow_echo(arg)
    sleep(1)
    arg
  end
end

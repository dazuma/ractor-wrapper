# frozen_string_literal: true

require "helper"

describe ::Ractor::Wrapper do
  let(:remote) { RemoteObject.new }

  describe "wrapper features" do
    let(:wrapper) { Ractor::Wrapper.new(remote) }

    after { wrapper.async_stop }

    it "moves a wrapped object" do
      wrapper
      assert_raises(Ractor::MovedError) do
        remote.to_s
      end
    end

    it "refuses to wrap a moved object" do
      wrapper
      assert_raises(Ractor::MovedError) do
        Ractor::Wrapper.new(remote)
      end
    end
  end

  describe "method features" do
    def wrapper(**)
      @wrapper ||= Ractor::Wrapper.new(remote, **)
    end

    after { wrapper.async_stop }

    it "passes arguments and return values" do
      result = wrapper.call(:echo_args, 1, 2, a: "b", c: "d")
      assert_equal("[1, 2], {a: \"b\", c: \"d\"}", result)
    end

    it "gets exceptions" do
      exception = assert_raises(RuntimeError) do
        wrapper.call(:whoops)
      end
      assert_equal("Whoops", exception.message)
    end

    it "yields to a local block" do
      local_var = false
      result = wrapper.call(:run_block, 1, 2, a: "b", c: "d") do |one, two, a:, c:|
        local_var = true
        "result #{one} #{two} #{a} #{c}"
      end
      assert_equal("result 1 2 b d", result)
      assert_equal(true, local_var)
    end

    it "yields to a remote block" do
      wrapper(execute_block_in_ractor: true)
      result = wrapper.call(:run_block, 1, 2, a: "b", c: "d") do |one, two, a:, c:|
        "result #{one} #{two} #{a} #{c} #{inspect}"
      end
      assert_equal("result 1 2 b d nil", result)
    end
  end

  describe "object moving and copying" do
    after { @wrapper&.async_stop }

    it "copies arguments by default" do
      @wrapper = Ractor::Wrapper.new(remote)
      str = "hello".dup
      @wrapper.call(:echo_args, str)
      str.to_s # Would fail if str was moved
    end

    it "moves arguments when move_arguments is set to true" do
      @wrapper = Ractor::Wrapper.new(remote, move_arguments: true)
      str = "hello".dup
      @wrapper.call(:echo_args, str)
      assert_raises(Ractor::MovedError) { str.to_s }
    end

    it "moves arguments when move is set to true" do
      @wrapper = Ractor::Wrapper.new(remote, move: true)
      str = "hello".dup
      @wrapper.call(:echo_args, str)
      assert_raises(Ractor::MovedError) { str.to_s }
    end

    it "honors move_arguments over move" do
      @wrapper = Ractor::Wrapper.new(remote, move: true, move_arguments: false)
      str = "hello".dup
      @wrapper.call(:echo_args, str)
      str.to_s
    end
  end

  describe "stubs" do
    let(:wrapper) { Ractor::Wrapper.new(remote) }

    after { wrapper.async_stop }

    it "converts method calls with arguments and return values" do
      result = wrapper.stub.echo_args(1, 2, a: "b", c: "d")
      assert_equal("[1, 2], {a: \"b\", c: \"d\"}", result)
    end

    it "converts exceptions" do
      exception = assert_raises(RuntimeError) do
        wrapper.stub.whoops
      end
      assert_equal("Whoops", exception.message)
    end

    it "converts respond_to" do
      assert(wrapper.stub.respond_to?(:echo_args))
      refute(wrapper.stub.respond_to?(:nonexistent_method))
    end
  end

  describe "single-thread lifecycle" do
    let(:wrapper) { Ractor::Wrapper.new(remote) }

    after { wrapper.async_stop }

    it "recovers the remote" do
      assert_equal("[], {}", remote.echo_args)
      wrapper
      assert_raises(Ractor::MovedError) { remote.echo_args }
      wrapper.async_stop
      recovered = wrapper.recover_object
      assert_equal("[], {}", recovered.echo_args)
    end

    it "calls multiple methods" do
      assert_equal("[1], {}", wrapper.call(:echo_args, 1))
      assert_equal("[2], {}", wrapper.call(:echo_args, 2))
    end

    it "serializes long-running methods" do
      r1 = Ractor.new(wrapper) do |w|
        result = w.call(:slow_echo, "hello")
        [result, Time.now.to_f]
      end
      r2 = Ractor.new(wrapper) do |w|
        result = w.call(:slow_echo, "world")
        [result, Time.now.to_f]
      end
      result1, time1 = r1.value
      result2, time2 = r2.value
      assert_equal("hello", result1)
      assert_equal("world", result2)
      assert_operator((time1 - time2).abs, :>, 0.8)
    end
  end

  describe "2-thread lifecycle" do
    let(:wrapper) { Ractor::Wrapper.new(remote, thread_count: 2) }

    after { wrapper.async_stop }

    it "recovers the remote" do
      assert_equal("[], {}", remote.echo_args)
      wrapper
      assert_raises(Ractor::MovedError) { remote.echo_args }
      wrapper.async_stop
      recovered = wrapper.recover_object
      assert_equal("[], {}", recovered.echo_args)
    end

    it "calls multiple methods" do
      assert_equal("[1], {}", wrapper.call(:echo_args, 1))
      assert_equal("[2], {}", wrapper.call(:echo_args, 2))
    end

    it "parallelizes long-running methods" do
      wrapper.call(:echo_args, 1)
      r1 = Ractor.new(wrapper) do |w|
        result = w.call(:slow_echo, "hello")
        [result, Time.now.to_f]
      end
      r2 = Ractor.new(wrapper) do |w|
        result = w.call(:slow_echo, "world")
        [result, Time.now.to_f]
      end
      result1, time1 = r1.value
      result2, time2 = r2.value
      assert_equal("hello", result1)
      assert_equal("world", result2)
      assert_operator((time1 - time2).abs, :<, 0.4)
    end
  end
end

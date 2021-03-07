require "helper"

describe ::Ractor::Wrapper do
  let(:remote) { RemoteObject.new }

  describe "method features" do
    let(:wrapper) { Ractor::Wrapper.new(remote) }

    after { wrapper.async_stop }

    it "passes arguments and return values" do
      result = wrapper.call(:echo_args, 1, 2, a: "b", c: "d")
      assert_equal("[1, 2], {:a=>\"b\", :c=>\"d\"}", result)
    end

    it "gets exceptions" do
      exception = assert_raises(RuntimeError) do
        wrapper.call(:fail)
      end
      assert_equal("Whoops", exception.message)
    end
  end

  describe "method configuration" do
    after { @wrapper&.async_stop }

    it "copies arguments by default" do
      @wrapper = Ractor::Wrapper.new(remote)
      str = "hello"
      @wrapper.call(:echo_args, str)
      str.to_s
    end

    it "moves arguments when move_arguments is set to true" do
      @wrapper = Ractor::Wrapper.new(remote, move_arguments: true)
      str = "hello"
      @wrapper.call(:echo_args, str)
      assert_raises(Ractor::MovedError) { str.to_s }
    end

    it "moves arguments when move is set to true" do
      @wrapper = Ractor::Wrapper.new(remote, move: true)
      str = "hello"
      @wrapper.call(:echo_args, str)
      assert_raises(Ractor::MovedError) { str.to_s }
    end

    it "honors move_arguments over move" do
      @wrapper = Ractor::Wrapper.new(remote, move: true, move_arguments: false)
      str = "hello"
      @wrapper.call(:echo_args, str)
      str.to_s
    end
  end

  describe "stubs" do
    let(:wrapper) { Ractor::Wrapper.new(remote) }

    after { wrapper.async_stop }

    it "converts method calls with arguments and return values" do
      result = wrapper.stub.echo_args(1, 2, a: "b", c: "d")
      assert_equal("[1, 2], {:a=>\"b\", :c=>\"d\"}", result)
    end

    it "converts exceptions" do
      exception = assert_raises(RuntimeError) do
        wrapper.stub.fail
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
      recovered = wrapper.recovered_object
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
      result1, time1 = r1.take
      result2, time2 = r2.take
      assert_equal("hello", result1)
      assert_equal("world", result2)
      assert_operator((time1 - time2).abs, :>, 0.8)
    end
  end

  describe "2-thread lifecycle" do
    let(:wrapper) { Ractor::Wrapper.new(remote, threads: 2) }

    after { wrapper.async_stop }

    it "recovers the remote" do
      assert_equal("[], {}", remote.echo_args)
      wrapper
      assert_raises(Ractor::MovedError) { remote.echo_args }
      wrapper.async_stop
      recovered = wrapper.recovered_object
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
      result1, time1 = r1.take
      result2, time2 = r2.take
      assert_equal("hello", result1)
      assert_equal("world", result2)
      assert_operator((time1 - time2).abs, :<, 0.4)
    end
  end
end

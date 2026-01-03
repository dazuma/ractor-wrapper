# frozen_string_literal: true

require "helper"

describe ::Ractor::Wrapper do
  let(:remote) { RemoteObject.new }

  describe "an isolated wrapper" do
    let(:wrapper) { Ractor::Wrapper.new(remote) }

    after { wrapper.async_stop.join }

    it "moves a wrapped object" do
      wrapper
      assert_raises(Ractor::MovedError) do
        remote.echo_args
      end
    end

    it "recovers the object" do
      wrapper.async_stop
      recovered = wrapper.recover_object
      assert_equal("[], {}", recovered.echo_args)
    end
  end

  describe "a local wrapper" do
    let(:wrapper) { Ractor::Wrapper.new(remote, use_current_ractor: true) }

    after { wrapper.async_stop.join }

    it "does not move a wrapped object" do
      wrapper
      assert_equal("[], {}", remote.echo_args)
    end

    it "refuses to recover" do
      wrapper.async_stop
      error = assert_raises(Ractor::Error) do
        wrapper.recover_object
      end
      assert_equal("cannot recover an object from a local wrapper", error.message)
    end
  end

  [
    {
      desc: "an isolated wrapper",
      opts: {use_current_ractor: false},
    },
    {
      desc: "a local wrapper",
      opts: {use_current_ractor: true},
    },
  ].each do |config|
    describe "basic behavior of #{config[:desc]}" do
      let(:base_opts) { config[:opts] }

      before { @wrapper = nil }
      after { @wrapper&.async_stop&.join }

      it "refuses to wrap a moved object" do
        port = Ractor::Port.new
        port.send(remote, move: true)
        port.receive
        port.close
        error = assert_raises(Ractor::MovedError) do
          Ractor::Wrapper.new(remote, **base_opts)
        end
        assert_equal("cannot wrap a moved object", error.message)
      end

      it "is shareable" do
        @wrapper = Ractor::Wrapper.new(remote, **base_opts)
        assert(Ractor.shareable?(@wrapper))
      end
    end

    describe "method features of #{config[:desc]}" do
      let(:base_opts) { config[:opts] }

      def wrapper(**)
        @wrapper ||= Ractor::Wrapper.new(remote, **base_opts, **)
      end

      after { wrapper.async_stop.join }

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

      it "yields to an in-place block" do
        wrapper(execute_blocks_in_place: true)
        result = wrapper.call(:run_block, 1, 2, a: "b", c: "d") do |one, two, a:, c:|
          "result #{one} #{two} #{a} #{c} #{inspect}"
        end
        assert_equal("result 1 2 b d nil", result)
      end
    end

    describe "object moving and copying in #{config[:desc]}" do
      let(:base_opts) { config[:opts] }

      def wrapper(**)
        @wrapper ||= Ractor::Wrapper.new(remote, **base_opts, **)
      end

      after { wrapper.async_stop.join }

      it "copies arguments by default" do
        str = "hello".dup
        wrapper.call(:echo_args, str)
        str.to_s # Would fail if str was moved
      end

      it "moves arguments when move_arguments is set to true" do
        wrapper(move_arguments: true)
        str = "hello".dup
        wrapper.call(:echo_args, str)
        assert_raises(Ractor::MovedError) { str.to_s }
      end

      it "moves arguments when move_data is set to true" do
        wrapper(move_data: true)
        str = "hello".dup
        wrapper.call(:echo_args, str)
        assert_raises(Ractor::MovedError) { str.to_s }
      end

      it "honors move_arguments over move_data" do
        wrapper(move_data: true, move_arguments: false)
        str = "hello".dup
        wrapper.call(:echo_args, str)
        str.to_s # Would fail if str was moved
      end

      it "copies return values by default" do
        obj, id = wrapper.call(:object_and_id, "hello".dup)
        refute_equal(obj.object_id, id)
      end

      it "moves return values when move_results is set to true" do
        wrapper(move_results: true)
        obj, id = wrapper.call(:object_and_id, "hello".dup)
        assert_equal(obj.object_id, id)
      end

      it "moves return values when move_data is set to true" do
        wrapper(move_data: true)
        obj, id = wrapper.call(:object_and_id, "hello".dup)
        assert_equal(obj.object_id, id)
      end

      it "honors move_results over move_data" do
        wrapper(move_data: true, move_results: false)
        obj, id = wrapper.call(:object_and_id, "hello".dup)
        refute_equal(obj.object_id, id)
      end

      it "copies block arguments by default" do
        arg_id, orig_id = wrapper.call(:run_block_with_id, "hello".dup) do |str, str_id|
          [str.object_id, str_id]
        end
        refute_equal(orig_id, arg_id)
      end

      it "moves block arguments when move_block_arguments is set" do
        wrapper(move_block_arguments: true)
        arg_id, orig_id = wrapper.call(:run_block_with_id, "hello".dup) do |str, str_id|
          [str.object_id, str_id]
        end
        assert_equal(orig_id, arg_id)
      end

      it "moves block arguments when move_data is set" do
        wrapper(move_data: true)
        arg_id, orig_id = wrapper.call(:run_block_with_id, "hello".dup) do |str, str_id|
          [str.object_id, str_id]
        end
        assert_equal(orig_id, arg_id)
      end

      it "honors move_block_arguments over move_data" do
        wrapper(move_data: true, move_block_arguments: false)
        arg_id, orig_id = wrapper.call(:run_block_with_id, "hello".dup) do |str, str_id|
          [str.object_id, str_id]
        end
        refute_equal(orig_id, arg_id)
      end

      it "copies block results by default" do
        wrapper(move_results: true)
        obj, id = wrapper.call(:run_block) do
          str = "hello".dup
          [str, str.object_id]
        end
        refute_equal(obj.object_id, id)
      end

      it "moves block results when move_block_results is set" do
        wrapper(move_results: true, move_block_results: true)
        obj, id = wrapper.call(:run_block) do
          str = "hello".dup
          [str, str.object_id]
        end
        assert_equal(obj.object_id, id)
      end

      it "moves block results when move_data is set" do
        wrapper(move_data: true)
        obj, id = wrapper.call(:run_block) do
          str = "hello".dup
          [str, str.object_id]
        end
        assert_equal(obj.object_id, id)
      end

      it "honors move_block_results over move_data" do
        wrapper(move_data: true, move_block_results: false)
        obj, id = wrapper.call(:run_block) do
          str = "hello".dup
          [str, str.object_id]
        end
        refute_equal(obj.object_id, id)
      end
    end
  end

  describe "stubs" do
    let(:wrapper) { Ractor::Wrapper.new(remote) }

    after { wrapper.async_stop.join }

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

    after { wrapper.async_stop.join }

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
      assert_operator((time1 - time2).abs, :>, 0.9)
    end
  end

  describe "2-thread lifecycle" do
    let(:wrapper) { Ractor::Wrapper.new(remote, threads: 2) }

    after { wrapper.async_stop.join }

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
      assert_operator((time1 - time2).abs, :<, 0.8)
    end
  end
end

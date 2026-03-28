# frozen_string_literal: true

require "helper"
require "timeout"

describe ::Ractor::Wrapper do
  let(:remote) { RemoteObject.new }

  describe "error classes" do
    it "Error is a subclass of Ractor::Error" do
      assert(::Ractor::Wrapper::Error < ::Ractor::Error)
    end

    it "CrashedError is a subclass of Error" do
      assert(::Ractor::Wrapper::CrashedError < ::Ractor::Wrapper::Error)
    end

    it "StoppedError is a subclass of Error" do
      assert(::Ractor::Wrapper::StoppedError < ::Ractor::Wrapper::Error)
    end

    it "CrashedError can be rescued as Ractor::Wrapper::Error" do
      assert_raises(::Ractor::Wrapper::Error) { raise ::Ractor::Wrapper::CrashedError, "test" }
    end

    it "StoppedError can be rescued as Ractor::Wrapper::Error" do
      assert_raises(::Ractor::Wrapper::Error) { raise ::Ractor::Wrapper::StoppedError, "test" }
    end
  end

  describe "initialization block" do
    after { @wrapper&.async_stop&.join }

    it "yields a Configuration, not the Wrapper itself" do
      yielded = nil
      @wrapper = ::Ractor::Wrapper.new(remote) { |config| yielded = config }
      assert_instance_of(::Ractor::Wrapper::Configuration, yielded)
      refute_equal(@wrapper, yielded)
    end

    it "can set name in the block" do
      @wrapper = ::Ractor::Wrapper.new(remote) { |config| config.name = "myname" }
      assert_equal("myname", @wrapper.name)
    end

    it "can set threads in the block" do
      @wrapper = ::Ractor::Wrapper.new(remote) { |config| config.threads = 2 }
      assert_equal(2, @wrapper.threads)
    end

    it "can set use_current_ractor in the block" do
      @wrapper = ::Ractor::Wrapper.new(remote) { |config| config.use_current_ractor = true }
      assert(@wrapper.use_current_ractor?)
    end

    it "block settings override kwargs" do
      @wrapper = ::Ractor::Wrapper.new(remote, threads: 2) { |config| config.threads = 0 }
      assert_equal(0, @wrapper.threads)
    end

    it "block can call configure_method" do
      @wrapper = ::Ractor::Wrapper.new(remote) { |config| config.configure_method(move_arguments: true) }
      str = "hello".dup
      @wrapper.call(:echo_args, str)
      assert_raises(::Ractor::MovedError) { str.to_s }
    end
  end

  wrapper_types = [
    {
      desc: "an isolated wrapper",
      opts: {use_current_ractor: false},
    },
    {
      desc: "a local wrapper",
      opts: {use_current_ractor: true},
    },
  ]
  threading_types = [
    {
      desc: "sequentially",
      opts: {threads: 0},
    },
    {
      desc: "with worker threads",
      opts: {threads: 2},
    },
  ]

  threading_types.each do |config|
    describe "an isolated wrapper running #{config[:desc]}" do
      let(:base_opts) { config[:opts] }
      let(:wrapper) { ::Ractor::Wrapper.new(remote, **base_opts) }

      after { wrapper.async_stop.join }

      it "moves a wrapped object" do
        wrapper
        assert_raises(::Ractor::MovedError) do
          remote.echo_args
        end
      end

      it "recovers the object" do
        assert_equal("[], {}", remote.echo_args)
        wrapper
        assert_raises(::Ractor::MovedError) do
          remote.echo_args
        end
        wrapper.async_stop
        recovered = wrapper.recover_object
        assert_equal("[], {}", recovered.echo_args)
      end
    end

    describe "a local wrapper" do
      let(:base_opts) { config[:opts] }
      let(:wrapper) { ::Ractor::Wrapper.new(remote, **base_opts, use_current_ractor: true) }

      after { wrapper.async_stop.join }

      it "does not move a wrapped object" do
        wrapper
        assert_equal("[], {}", remote.echo_args)
      end

      it "refuses to recover" do
        wrapper.async_stop
        error = assert_raises(::Ractor::Error) do
          wrapper.recover_object
        end
        assert_equal("cannot recover an object from a local wrapper", error.message)
      end
    end
  end

  wrapper_types.product(threading_types).each do |(config1, config2)|
    describe "basic behavior of #{config1[:desc]} running #{config2[:desc]}" do
      let(:base_opts) { config1[:opts].merge(config2[:opts]) }

      before { @wrapper = nil }
      after { @wrapper&.async_stop&.join }

      it "refuses to wrap a moved object" do
        port = ::Ractor::Port.new
        port.send(remote, move: true)
        port.receive
        port.close
        error = assert_raises(::Ractor::MovedError) do
          ::Ractor::Wrapper.new(remote, **base_opts)
        end
        assert_equal("cannot wrap a moved object", error.message)
      end

      it "is shareable" do
        @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
        assert(::Ractor.shareable?(@wrapper))
      end
    end

    describe "method features of #{config1[:desc]} running #{config2[:desc]}" do
      let(:base_opts) { config1[:opts].merge(config2[:opts]) }

      def wrapper(**)
        @wrapper ||= ::Ractor::Wrapper.new(remote, **base_opts, **)
      end

      after { wrapper.async_stop.join }

      it "passes arguments and return values" do
        result = wrapper.call(:echo_args, 1, 2, a: "b", c: "d")
        assert_equal("[1, 2], {a: \"b\", c: \"d\"}", result)
      end

      it "gets exceptions" do
        exception = assert_raises(::RuntimeError) do
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

    describe "object moving and copying in #{config1[:desc]} running #{config2[:desc]}" do
      let(:base_opts) { config1[:opts].merge(config2[:opts]) }

      def wrapper(**)
        @wrapper ||= ::Ractor::Wrapper.new(remote, **base_opts, **)
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
        assert_raises(::Ractor::MovedError) { str.to_s }
      end

      it "moves arguments when move_data is set to true" do
        wrapper(move_data: true)
        str = "hello".dup
        wrapper.call(:echo_args, str)
        assert_raises(::Ractor::MovedError) { str.to_s }
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

    describe "stopping behavior of #{config1[:desc]} running #{config2[:desc]}" do
      let(:base_opts) { config1[:opts].merge(config2[:opts]) }
      let(:wrapper) { ::Ractor::Wrapper.new(remote, **base_opts) }

      after { wrapper.async_stop.join }

      it "raises StoppedError when called after the wrapper has stopped" do
        wrapper.async_stop.join
        assert_raises(::Ractor::Wrapper::StoppedError) { wrapper.call(:echo_args) }
      end
    end

    describe "stubs in #{config1[:desc]} running #{config2[:desc]}" do
      let(:base_opts) { config1[:opts].merge(config2[:opts]) }
      let(:wrapper) { ::Ractor::Wrapper.new(remote, **base_opts) }

      after { wrapper.async_stop.join }

      it "converts method calls with arguments and return values" do
        result = wrapper.stub.echo_args(1, 2, a: "b", c: "d")
        assert_equal("[1, 2], {a: \"b\", c: \"d\"}", result)
      end

      it "converts exceptions" do
        exception = assert_raises(::RuntimeError) do
          wrapper.stub.whoops
        end
        assert_equal("Whoops", exception.message)
      end

      it "converts respond_to" do
        assert(wrapper.stub.respond_to?(:echo_args))
        refute(wrapper.stub.respond_to?(:nonexistent_method))
      end
    end
  end

  wrapper_types.each do |config|
    describe "non-threaded lifecycle in #{config[:desc]}" do
      let(:base_opts) { config[:opts] }
      let(:wrapper) { ::Ractor::Wrapper.new(remote, **base_opts) }

      after { wrapper.async_stop.join }

      it "raises StoppedError when a call is refused during shutdown" do
        ::Timeout.timeout(5) do
          # Start a slow call so the server is occupied, then queue stop + another call.
          # When the slow call finishes, server processes the stop, and cleanup refusees
          # the queued call via refuse_method.
          slow_thread = ::Thread.new { wrapper.call(:slow_echo, "fill") rescue nil }
          sleep 0.1 # ensure slow call is being processed
          wrapper.async_stop
          result = begin
            wrapper.call(:echo_args)
          rescue ::StandardError => e
            e
          end
          slow_thread.join
          assert_instance_of(::Ractor::Wrapper::StoppedError, result)
        end
      end

      it "serializes long-running methods" do
        r1 = ::Ractor.new(wrapper) do |w|
          result = w.call(:slow_echo, "hello")
          [result, ::Time.now.to_f]
        end
        r2 = ::Ractor.new(wrapper) do |w|
          result = w.call(:slow_echo, "world")
          [result, ::Time.now.to_f]
        end
        result1, time1 = r1.value
        result2, time2 = r2.value
        assert_equal("hello", result1)
        assert_equal("world", result2)
        assert_operator((time1 - time2).abs, :>, 0.9)
      end
    end

    describe "2-thread lifecycle in #{config[:desc]}" do
      let(:base_opts) { config[:opts] }
      let(:wrapper) { ::Ractor::Wrapper.new(remote, **base_opts, threads: 2) }

      after { wrapper.async_stop.join }

      it "parallelizes long-running methods" do
        wrapper.call(:echo_args, 1)
        r1 = ::Ractor.new(wrapper) do |w|
          result = w.call(:slow_echo, "hello")
          [result, ::Time.now.to_f]
        end
        r2 = ::Ractor.new(wrapper) do |w|
          result = w.call(:slow_echo, "world")
          [result, ::Time.now.to_f]
        end
        result1, time1 = r1.value
        result2, time2 = r2.value
        assert_equal("hello", result1)
        assert_equal("world", result2)
        assert_operator((time1 - time2).abs, :<, 0.8)
      end
    end
  end

  describe "after unexpected server termination" do
    # Force a server crash by sending a CrashingJoinMessage (defined in
    # helper.rb) to the server port. main_loop accesses reply_port on the
    # received message, which raises, propagating out of main_loop and
    # triggering the ensure-based crash cleanup.
    def crash_server(wrapper)
      wrapper.instance_variable_get(:@port).send(
        CrashingJoinMessage.new(reply_port: nil),
        move: true
      )
    end

    [
      {desc: "isolated threaded wrapper", opts: {threads: 1, enable_logging: true}},
      {desc: "local threaded wrapper", opts: {use_current_ractor: true, threads: 1}},
    ].each do |config|
      describe config[:desc] do
        it "notifies callers queued but not yet dispatched to a worker when the server crashes" do
          ::Timeout.timeout(5) do
            capture_subprocess_io do
              wrapper = ::Ractor::Wrapper.new(remote, **config[:opts])
              t1 = ::Thread.new { wrapper.call(:slow_echo, "first") rescue $! } # rubocop:disable Style/SpecialGlobalVars
              sleep 0.1 # let the single worker pick up the first call
              t2 = ::Thread.new { wrapper.call(:slow_echo, "second") rescue $! } # rubocop:disable Style/SpecialGlobalVars
              sleep 0.1 # let the second call reach the queue
              crash_server(wrapper)
              result1 = t1.value
              result2 = t2.value
              # Call 1 was already dispatched to the worker, which finishes and replies normally
              assert_equal("first", result1)
              # Call 2 was queued but not dispatched; crash cleanup should notify it
              assert_instance_of(::Ractor::Wrapper::CrashedError, result2)
            end
          end
        end
      end
    end

    [
      {desc: "isolated sequential wrapper", opts: {}},
      {desc: "isolated threaded wrapper", opts: {threads: 2}},
      {desc: "local sequential wrapper", opts: {use_current_ractor: true}},
      {desc: "local threaded wrapper", opts: {use_current_ractor: true, threads: 2}},
    ].each do |config|
      describe config[:desc] do
        it "does not hang when the server crashes with a join pending" do
          ::Timeout.timeout(5) do
            capture_subprocess_io do
              wrapper = ::Ractor::Wrapper.new(remote, **config[:opts])
              join_thread = ::Thread.new { wrapper.join }
              sleep 0.1  # let the join request reach and be queued by the server
              crash_server(wrapper)
              assert join_thread.join(5), "join should complete within 5 seconds"
              assert_same(wrapper, join_thread.value)
            end
          end
        end

        it "does not hang when join called after the server has already crashed" do
          ::Timeout.timeout(5) do
            capture_subprocess_io do
              wrapper = ::Ractor::Wrapper.new(remote, **config[:opts])
              crash_server(wrapper)
              sleep 0.1  # let crash cleanup finish and close the port
              assert_same(wrapper, wrapper.join)
            end
          end
        end

        it "raises StoppedError if a call is made post-crash" do
          ::Timeout.timeout(5) do
            capture_subprocess_io do
              wrapper = ::Ractor::Wrapper.new(remote, **config[:opts])
              crash_server(wrapper)
              sleep 0.1  # let crash cleanup finish and close the port
              assert_raises(::Ractor::Wrapper::StoppedError) do
                wrapper.call(:echo_args)
              end
            end
          end
        end
      end
    end

    [
      {desc: "isolated sequential wrapper", opts: {}},
      {desc: "isolated threaded wrapper", opts: {threads: 2}},
    ].each do |config|
      describe config[:desc] do
        it "recovers the object" do
          ::Timeout.timeout(5) do
            capture_subprocess_io do
              wrapper = ::Ractor::Wrapper.new(remote, **config[:opts])
              crash_server(wrapper)
              recovered = wrapper.recover_object
              assert_equal("[], {}", recovered.echo_args)
            end
          end
        end
      end
    end
  end
end

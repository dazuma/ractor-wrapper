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

  describe "configuration" do
    after { @wrapper&.async_stop&.join }

    it "has a known default" do
      @wrapper = ::Ractor::Wrapper.new(remote)
      assert_equal(@wrapper.object_id.to_s, @wrapper.name)
      assert_equal(false, @wrapper.enable_logging?)
      assert_equal(0, @wrapper.threads)
      assert_equal(false, @wrapper.use_current_ractor?)
      method_settings = @wrapper.method_settings(:hello)
      assert_equal(:copy, method_settings.arguments)
      assert_equal(:copy, method_settings.results)
      assert_equal(:copy, method_settings.block_arguments)
      assert_equal(:copy, method_settings.block_results)
      assert_equal(:caller, method_settings.block_environment)
    end

    describe "via keyword args" do
      it "can set name" do
        @wrapper = ::Ractor::Wrapper.new(remote, name: "myname")
        assert_equal("myname", @wrapper.name)
      end

      it "can set use_current_ractor" do
        @wrapper = ::Ractor::Wrapper.new(remote, use_current_ractor: true)
        assert_equal(true, @wrapper.use_current_ractor?)
      end

      it "can set threads" do
        @wrapper = ::Ractor::Wrapper.new(remote, threads: 3)
        assert_equal(3, @wrapper.threads)
      end

      it "can set default arguments" do
        @wrapper = ::Ractor::Wrapper.new(remote, arguments: :move)
        method_settings = @wrapper.method_settings(:hello)
        assert_equal(:move, method_settings.arguments)
      end

      it "can set default results" do
        @wrapper = ::Ractor::Wrapper.new(remote, results: :void)
        method_settings = @wrapper.method_settings(:hello)
        assert_equal(:void, method_settings.results)
      end

      it "can set default block arguments" do
        @wrapper = ::Ractor::Wrapper.new(remote, block_arguments: :move)
        method_settings = @wrapper.method_settings(:hello)
        assert_equal(:move, method_settings.block_arguments)
      end

      it "can set default block results" do
        @wrapper = ::Ractor::Wrapper.new(remote, block_results: :void)
        method_settings = @wrapper.method_settings(:hello)
        assert_equal(:void, method_settings.block_results)
      end

      it "can set default block environment" do
        @wrapper = ::Ractor::Wrapper.new(remote, block_environment: :wrapped)
        method_settings = @wrapper.method_settings(:hello)
        assert_equal(:wrapped, method_settings.block_environment)
      end

      it "validates default arguments" do
        assert_raises(::ArgumentError) do
          @wrapper = ::Ractor::Wrapper.new(remote, arguments: :toe)
        end
      end

      it "validates default results" do
        assert_raises(::ArgumentError) do
          @wrapper = ::Ractor::Wrapper.new(remote, results: :salchow)
        end
      end

      it "validates default block_arguments" do
        assert_raises(::ArgumentError) do
          @wrapper = ::Ractor::Wrapper.new(remote, block_arguments: :loop)
        end
      end

      it "validates default block_results" do
        assert_raises(::ArgumentError) do
          @wrapper = ::Ractor::Wrapper.new(remote, block_results: :flip)
        end
      end

      it "validates default block_environment" do
        assert_raises(::ArgumentError) do
          @wrapper = ::Ractor::Wrapper.new(remote, block_environment: :axel)
        end
      end
    end

    describe "via initialization block" do
      it "yields a Configuration" do
        yielded = nil
        @wrapper = ::Ractor::Wrapper.new(remote) { |config| yielded = config }
        assert_instance_of(::Ractor::Wrapper::Configuration, yielded)
      end

      it "can set name" do
        @wrapper = ::Ractor::Wrapper.new(remote) { |config| config.name = "myname" }
        assert_equal("myname", @wrapper.name)
      end

      it "can set threads" do
        @wrapper = ::Ractor::Wrapper.new(remote) { |config| config.threads = 2 }
        assert_equal(2, @wrapper.threads)
      end

      it "can set use_current_ractor" do
        @wrapper = ::Ractor::Wrapper.new(remote) { |config| config.use_current_ractor = true }
        assert_equal(true, @wrapper.use_current_ractor?)
      end

      it "overrides kwargs" do
        @wrapper = ::Ractor::Wrapper.new(remote, threads: 2) { |config| config.threads = 3 }
        assert_equal(3, @wrapper.threads)
      end

      it "can call configure_method to override default method config" do
        @wrapper = ::Ractor::Wrapper.new(remote) do |config|
          config.configure_method(results: :move)
          config.configure_method(:hello, results: :void)
        end
        hello_method_settings = @wrapper.method_settings(:hello)
        assert_equal(:void, hello_method_settings.results)
        bye_method_settings = @wrapper.method_settings(:bye)
        assert_equal(:move, bye_method_settings.results)
      end

      it "validates arguments to configure_method" do
        assert_raises(::ArgumentError) do
          @wrapper = ::Ractor::Wrapper.new(remote) do |config|
            config.configure_method(block_environment: :axel)
          end
        end
      end
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

      it "allows recovery from another ractor" do
        wrapper.async_stop
        ractor2 = ::Ractor.new(wrapper) do |wrap|
          recovered = wrap.recover_object
          recovered.echo_args
        end
        assert_equal("[], {}", ractor2.value)
      end

      it "does not allow recovery from another ractor if already recovered" do
        wrapper.async_stop
        wrapper.recover_object
        ractor2 = ::Ractor.new(wrapper) do |wrap|
          wrap.recover_object
          ""
        rescue ::StandardError => e
          e.class.name
        end
        assert_equal("Ractor::Wrapper::Error", ractor2.value)
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

      it "yields to a caller-environment block" do
        local_var = false
        result = wrapper.call(:run_block, 1, 2, a: "b", c: "d") do |one, two, a:, c:|
          local_var = true
          "result #{one} #{two} #{a} #{c}"
        end
        assert_equal("result 1 2 b d", result)
        assert_equal(true, local_var)
      end

      it "yields to a wrapped-environment block" do
        wrapper(block_environment: :wrapped)
        result = wrapper.call(:run_block, 1, 2, a: "b", c: "d") do |one, two, a:, c:|
          "result #{one} #{two} #{a} #{c} #{inspect}"
        end
        assert_equal("result 1 2 b d nil", result)
      end

      it "does not allow a wrapped-environment block to access the caller environment" do
        local_var = false
        wrapper(block_environment: :wrapped)
        assert_raises(::ArgumentError) do
          wrapper.call(:run_block, 1, 2, a: "b", c: "d") do |one, two, a:, c:|
            local_var = true
            "result #{one} #{two} #{a} #{c}"
          end
        end
        assert_equal(false, local_var)
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

      it "moves arguments when arguments is set to :move" do
        wrapper(arguments: :move)
        str = "hello".dup
        wrapper.call(:echo_args, str)
        assert_raises(::Ractor::MovedError) { str.to_s }
      end

      it "copies return values by default" do
        obj, id = wrapper.call(:object_and_id, "hello".dup)
        refute_equal(obj.object_id, id)
      end

      it "moves return values when results is set to :move" do
        wrapper(results: :move)
        obj, id = wrapper.call(:object_and_id, "hello".dup)
        assert_equal(obj.object_id, id)
      end

      it "suppresses return values when results is set to :void" do
        wrapper(results: :void)
        result = wrapper.call(:object_and_id, "hello".dup)
        assert_nil(result)
      end

      it "copies block arguments by default" do
        arg_id, orig_id = wrapper.call(:run_block_with_id, "hello".dup) do |str, str_id|
          [str.object_id, str_id]
        end
        refute_equal(orig_id, arg_id)
      end

      it "moves block arguments when block_arguments is :move" do
        wrapper(block_arguments: :move)
        arg_id, orig_id = wrapper.call(:run_block_with_id, "hello".dup) do |str, str_id|
          [str.object_id, str_id]
        end
        assert_equal(orig_id, arg_id)
      end

      it "copies block results by default" do
        wrapper(results: :move)
        obj, id = wrapper.call(:run_block) do
          str = "hello".dup
          [str, str.object_id]
        end
        refute_equal(obj.object_id, id)
      end

      it "moves block results when block_results is :move" do
        wrapper(results: :move, block_results: :move)
        obj, id = wrapper.call(:run_block) do
          str = "hello".dup
          [str, str.object_id]
        end
        assert_equal(obj.object_id, id)
      end

      it "suppresses block results when block_results is :void" do
        wrapper(results: :move, block_results: :void)
        result = wrapper.call(:run_block) do
          str = "hello".dup
          [str, str.object_id]
        end
        assert_nil(result)
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

      it "returns stubs instead of wrapped object" do
        returned_self = wrapper.stub.return_self
        result = returned_self.echo_args
        assert_equal("[], {}", result)
      end

      it "yields stubs instead of wrapped object" do
        wrapper.stub.block_args_self("hello", "ruby") do |obj1, obj2, kwobj:, kwself:|
          assert_equal("hello", obj1)
          assert_kind_of(::Ractor::Wrapper::Stub, obj2)
          assert_equal("ruby", kwobj)
          assert_kind_of(::Ractor::Wrapper::Stub, kwself)
        end
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
      {desc: "isolated threaded wrapper", opts: {threads: 1}},
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

  describe "fiber-based re-entrant block calls" do
    include TimeoutHelper

    # Bound the cleanup so a wrapper that has deadlocked under test cannot hang
    # the entire suite. Under correct behavior the join completes immediately.
    def bounded_cleanup(wrapper)
      ::Thread.new { wrapper.async_stop.join }.join(3)
    end

    [
      {desc: "isolated", opts: {}},
      {desc: "local", opts: {use_current_ractor: true}},
    ].each do |wrapper_config|
      describe "in sequential mode (#{wrapper_config[:desc]})" do
        let(:base_opts) { wrapper_config[:opts] }

        before { @wrapper = nil }
        after { bounded_cleanup(@wrapper) if @wrapper }

        it "does not deadlock when a block re-enters the wrapper" do
          @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
          stub = @wrapper.stub
          results = with_timeout(2) do
            collected = []
            stub.each_item(["a", "b"]) { |item| collected << stub.echo_args(item) }
            collected
          end
          assert_equal(['["a"], {}', '["b"], {}'], results)
        end

        it "does not deadlock when blocks re-enter the wrapper at depth 3" do
          @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
          stub = @wrapper.stub
          results = with_timeout(2) do
            collected = []
            stub.each_item([1, 2]) do |a|
              stub.each_item([10, 20]) do |b|
                stub.each_item([100]) do |c|
                  collected << stub.echo_args(a + b + c)
                end
              end
            end
            collected
          end
          assert_equal(["[111], {}", "[121], {}", "[112], {}", "[122], {}"], results)
        end

        it "invokes the block correctly when called from within an Enumerator generator" do
          @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
          stub = @wrapper.stub
          results = with_timeout(2) do
            stub.each_via_generator(["x", "y"], &:upcase)
          end
          assert_equal(["X", "Y"], results)
        end

        it "raises CrashedError when the server crashes with a fiber suspended" do
          capture_subprocess_io do
            @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
            stub = @wrapper.stub
            latch = ::Queue.new
            result_holder = []
            call_thread = ::Thread.new do
              stub.each_item(["a"]) do |item|
                latch.pop
                item.upcase
              end
            rescue ::Exception => e # rubocop:disable Lint/RescueException
              result_holder << e
            end
            with_timeout(2) { sleep 0.01 until latch.num_waiting.positive? }
            crash_port = ::Ractor::Port.new
            @wrapper.instance_variable_get(:@port).send(CrashingJoinMessage.new(crash_port))
            sleep 0.1
            latch.push(:go)
            assert(call_thread.join(2), "call thread should complete after crash")
            assert_instance_of(::Ractor::Wrapper::CrashedError, result_holder.first)
            with_timeout(2) { @wrapper.join }
            @wrapper = nil
          end
        end

        it "drains a suspended fiber when stopped during a block" do
          @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
          stub = @wrapper.stub
          latch = ::Queue.new
          result_holder = []
          call_thread = ::Thread.new do
            result_holder << stub.each_item(["a"]) do |item|
              latch.pop
              item.upcase
            end
          end
          with_timeout(2) { sleep 0.01 until latch.num_waiting.positive? }
          @wrapper.async_stop
          with_timeout(2) do
            assert_raises(::Ractor::Wrapper::StoppedError) { stub.echo_args("nope") }
          end
          latch.push(:go)
          assert(call_thread.join(2), "call thread should complete after latch released")
          assert_equal([["a"]], result_holder)
          with_timeout(2) { @wrapper.join }
          @wrapper = nil
        end

        # Documents an unchanged limitation: when a block is invoked from a
        # non-method-handling fiber (here, an Enumerator generator), the proxy
        # proc falls back to the blocking path. A re-entrant wrapper call from
        # inside that block then deadlocks the server.
        it "still deadlocks when an Enumerator-invoked block re-enters the wrapper" do
          @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
          stub = @wrapper.stub
          assert_raises(::Minitest::Assertion) do
            with_timeout(1) do
              stub.each_via_generator(["a", "b"]) { |item| stub.echo_args(item) }
            end
          end
        end
      end

      describe "in threaded mode (#{wrapper_config[:desc]})" do
        let(:base_opts) { wrapper_config[:opts].merge(threads: 2) }

        before { @wrapper = nil }
        after { bounded_cleanup(@wrapper) if @wrapper }

        it "does not deadlock when re-entry depth exceeds the worker count" do
          @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
          stub = @wrapper.stub
          results = with_timeout(2) do
            collected = []
            stub.each_item([1, 2]) do |a|
              stub.each_item([10, 20]) do |b|
                stub.each_item([100]) do |c|
                  collected << stub.echo_args(a + b + c)
                end
              end
            end
            collected
          end
          assert_equal(["[111], {}", "[121], {}", "[112], {}", "[122], {}"], results)
        end

        it "drains a suspended fiber when stopped during a block" do
          @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
          stub = @wrapper.stub
          latch = ::Queue.new
          result_holder = []
          call_thread = ::Thread.new do
            result_holder << stub.each_item(["a"]) do |item|
              latch.pop
              item.upcase
            end
          end
          with_timeout(2) { sleep 0.01 until latch.num_waiting.positive? }
          @wrapper.async_stop
          with_timeout(2) do
            assert_raises(::Ractor::Wrapper::StoppedError) { stub.echo_args("nope") }
          end
          latch.push(:go)
          assert(call_thread.join(2), "call thread should complete after latch released")
          assert_equal([["a"]], result_holder)
          with_timeout(2) { @wrapper.join }
          @wrapper = nil
        end

        it "raises CrashedError when the server crashes with fibers suspended in workers" do
          capture_subprocess_io do
            @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
            stub = @wrapper.stub
            latch = ::Queue.new
            result_holder = ::Queue.new
            threads = ::Array.new(2) do |i|
              ::Thread.new do
                stub.each_item([i]) do |item|
                  latch.pop
                  item
                end
              rescue ::Exception => e # rubocop:disable Lint/RescueException
                result_holder << e
              end
            end
            with_timeout(2) { sleep 0.01 until latch.num_waiting == 2 }
            crash_port = ::Ractor::Port.new
            @wrapper.instance_variable_get(:@port).send(CrashingJoinMessage.new(crash_port))
            sleep 0.1
            2.times { latch.push(:go) }
            threads.each { |t| assert(t.join(2), "caller thread should complete after crash") }
            errors = []
            errors << result_holder.pop until result_holder.empty?
            assert_equal(2, errors.size)
            errors.each { |e| assert_instance_of(::Ractor::Wrapper::CrashedError, e) }
            with_timeout(2) { @wrapper.join }
            @wrapper = nil
          end
        end

        it "raises CrashedError when a worker thread crashes with a fiber suspended" do
          skip "requires local mode to reach the worker thread" unless wrapper_config[:opts][:use_current_ractor]
          capture_subprocess_io do
            @wrapper = ::Ractor::Wrapper.new(remote, threads: 1, **wrapper_config[:opts])
            stub = @wrapper.stub
            latch = ::Queue.new
            result_holder = []
            call_thread = ::Thread.new do
              stub.each_item(["a"]) do |item|
                latch.pop
                item.upcase
              end
            rescue ::Exception => e # rubocop:disable Lint/RescueException
              result_holder << e
            end
            with_timeout(2) { sleep 0.01 until latch.num_waiting.positive? }
            worker = ::Thread.list.find { |t| t.name&.include?(":worker:") }
            assert(worker, "expected to find a named worker thread")
            worker.raise(::RuntimeError.new("simulated worker crash"))
            sleep 0.1
            latch.push(:go)
            assert(call_thread.join(2), "call thread should complete after worker crash")
            assert_instance_of(::Ractor::Wrapper::CrashedError, result_holder.first)
            assert_match(/simulated worker crash/, result_holder.first.message)
            assert_match(/RuntimeError/, result_holder.first.message)
            with_timeout(2) { @wrapper.join }
            @wrapper = nil
          end
        end

        it "handles concurrent callers making re-entrant block calls" do
          @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
          stub = @wrapper.stub
          caller_count = 4
          per_caller = [1, 2, 3]
          results = ::Array.new(caller_count)
          threads = ::Array.new(caller_count) do |caller_num|
            ::Thread.new do
              collected = []
              stub.each_item(per_caller) do |item|
                collected << stub.echo_args((caller_num * 100) + item)
              end
              results[caller_num] = collected
            end
          end
          with_timeout(5) { threads.each(&:join) }
          assert_equal(caller_count, results.size)
          results.each_with_index do |collected, caller_num|
            expected = per_caller.map { |item| "[#{(caller_num * 100) + item}], {}" }
            assert_equal(expected, collected, "results for caller #{caller_num}")
          end
        end
      end

      describe "functional coverage in fiber path (#{wrapper_config[:desc]})" do
        let(:base_opts) { wrapper_config[:opts] }

        before { @wrapper = nil }
        after { bounded_cleanup(@wrapper) if @wrapper }

        it "passes block return values back through fiber suspend/resume" do
          @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
          stub = @wrapper.stub
          collected = []
          stub.each_item([1, 2, 3]) { |n| collected << (n * 10) }
          assert_equal([10, 20, 30], collected)
        end

        it "propagates an exception raised in the block back through the fiber path" do
          @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
          stub = @wrapper.stub
          attempts = 0
          assert_raises(::RuntimeError, "block boom") do
            stub.each_item([1, 2, 3]) do |_n| # rubocop:disable Lint/UnreachableLoop
              attempts += 1
              raise "block boom"
            end
          end
          assert_equal(1, attempts)
        end

        it "supports many yields from one method, each with a re-entrant call" do
          @wrapper = ::Ractor::Wrapper.new(remote, **base_opts)
          stub = @wrapper.stub
          collected = []
          stub.each_item((1..5).to_a) { |n| collected << stub.echo_args(n) }
          assert_equal((1..5).map { |n| "[#{n}], {}" }, collected)
        end

        it "preserves move semantics for block arguments through the fiber path" do
          @wrapper = ::Ractor::Wrapper.new(remote, **base_opts) do |config|
            config.configure_method(:run_block_with_id, block_arguments: :move)
          end
          stub = @wrapper.stub
          arg_id, server_side_id = stub.run_block_with_id("hello".dup) do |str, str_id|
            [str.object_id, str_id]
          end
          assert_equal(server_side_id, arg_id)
        end

        it "moves block results back into the server through the fiber path" do
          @wrapper = ::Ractor::Wrapper.new(remote, **base_opts) do |config|
            config.configure_method(:run_block, block_results: :move)
          end
          stub = @wrapper.stub
          returned = "ignored"
          stub.run_block do
            returned = "result".dup
            returned
          end
          assert_raises(::Ractor::MovedError) { returned.to_s }
        end

        it "runs a wrapped-environment shareable block without going through the fiber path" do
          @wrapper = ::Ractor::Wrapper.new(remote, block_environment: :wrapped, **base_opts)
          stub = @wrapper.stub
          result = stub.run_block(1, 2, a: "b", c: "d") do |one, two, a:, c:|
            "result #{one} #{two} #{a} #{c}"
          end
          assert_equal("result 1 2 b d", result)
        end
      end
    end
  end
end

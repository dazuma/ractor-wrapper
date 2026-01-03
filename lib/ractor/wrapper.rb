# frozen_string_literal: true

##
# See ruby-doc.org for info on Ractors.
#
class Ractor
  ##
  # An experimental class that wraps a non-shareable object in an actor,
  # allowing multiple Ractors to access it concurrently.
  #
  # WARNING: This is a highly experimental library, and currently _not_
  # recommended for production use. (As of Ruby 4.0.0, the same can be said of
  # Ractors in general.)
  #
  # ## What is Ractor::Wrapper?
  #
  # For the most part, unless an object is _sharable_, which generally means
  # deeply immutable along with a few other restrictions, it cannot be accessed
  # directly from another Ractor. This makes it difficult for multiple Ractors
  # to share a resource that is stateful. Such a resource must typically itself
  # be implemented as a Ractor and accessed via message passing.
  #
  # Ractor::Wrapper makes it possible for an ordinary non-shareable object to
  # be accessed from multiple Ractors. It does this by "wrapping" the object
  # with an actor that listens for messages and invokes the object's methods in
  # a controlled single-Ractor environment. It then provides a stub object that
  # reproduces the interface of the original object, but responds to method
  # calls by sending messages to the wrapper. Ractor::Wrapper can be used to
  # implement simple actors by writing "plain" Ruby objects, or to adapt
  # existing non-shareable objects to a multi-Ractor world.
  #
  # ## Net::HTTP example
  #
  # The following example shows how to share a single Net::HTTP session object
  # among multiple Ractors.
  #
  #     require "ractor/wrapper"
  #     require "net/http"
  #
  #     # Create a Net::HTTP session. Net::HTTP sessions are not shareable,
  #     # so normally only one Ractor can access them at a time.
  #     http = Net::HTTP.new("example.com")
  #     http.start
  #
  #     # Create a wrapper around the session. This moves the session into an
  #     # internal Ractor and listens for method call requests. By default, a
  #     # wrapper serializes calls, handling one at a time, for compatibility
  #     # with non-thread-safe objects.
  #     wrapper = Ractor::Wrapper.new(http)
  #
  #     # At this point, the session object can no longer be accessed directly
  #     # because it is now owned by the wrapper's internal Ractor.
  #     #     http.get("/whoops")  # <= raises Ractor::MovedError
  #
  #     # However, you can access the session via the stub object provided by
  #     # the wrapper. This stub proxies the call to the wrapper's internal
  #     # Ractor. And it's shareable, so any number of Ractors can use it.
  #     response = wrapper.stub.get("/")
  #
  #     # Here, we start two Ractors, and pass the stub to each one. Each
  #     # Ractor can simply call methods on the stub as if it were the original
  #     # connection object. Internally, of course, the calls are proxied to
  #     # the original object via the wrapper, and execution is serialized.
  #     r1 = Ractor.new(wrapper.stub) do |stub|
  #       5.times do
  #         stub.get("/hello")
  #       end
  #       :ok
  #     end
  #     r2 = Ractor.new(wrapper.stub) do |stub|
  #       5.times do
  #         stub.get("/ruby")
  #       end
  #       :ok
  #     end
  #
  #     # Wait for the two above Ractors to finish.
  #     r1.join
  #     r2.join
  #
  #     # After you stop the wrapper, you can retrieve the underlying session
  #     # object and access it directly again.
  #     wrapper.async_stop
  #     http = wrapper.recover_object
  #     http.finish
  #
  # ## SQLite3 example
  #
  # The following example shows how to share a SQLite3 database among multiple
  # Ractors.
  #
  #     require "ractor/wrapper"
  #     require "sqlite3"
  #
  #     # Create a SQLite3 database. These objects are not shareable, so
  #     # normally only one Ractor can access them.
  #     db = SQLite3::Database.new($my_database_path)
  #
  #     # Create a wrapper around the database. A SQLite3::Database object
  #     # cannot be moved between Ractors, so we configure the wrapper to run
  #     # in the current Ractor. You can also configure it to run multiple
  #     # worker threads because the database object itself is thread-safe.
  #     wrapper = Ractor::Wrapper.new(db, use_current_ractor: true, threads: 2)
  #
  #     # At this point, the database object can still be accessed directly
  #     # because it hasn't been moved to a different Ractor.
  #     rows = db.execute("select * from numbers")
  #
  #     # You can also access the database via the stub object provided by the
  #     # wrapper.
  #     rows = wrapper.stub.execute("select * from numbers")
  #
  #     # Here, we start two Ractors, and pass the stub to each one. The
  #     # wrapper's two worker threads will handle the requests in the order
  #     # received.
  #     r1 = Ractor.new(wrapper.stub) do |db_stub|
  #       5.times do
  #         rows = db_stub.execute("select * from numbers")
  #       end
  #       :ok
  #     end
  #     r2 = Ractor.new(wrapper.stub) do |db_stub|
  #       5.times do
  #         rows = db_stub.execute("select * from numbers")
  #       end
  #       :ok
  #     end
  #
  #     # Wait for the two above Ractors to finish.
  #     r1.join
  #     r2.join
  #
  #     # After stopping the wrapper, you can call the join method to wait for
  #     # it to completely finish.
  #     wrapper.async_stop
  #     wrapper.join
  #
  #     # When running a wrapper with :use_current_ractor, you do not need to
  #     # recover the object, because it was never moved. The recover_object
  #     # method is not available.
  #     #     db2 = wrapper.recover_object  # <= raises Ractor::Error
  #
  # ## Features
  #
  # *   Provides a Ractor-shareable method interface to a non-shareable object.
  # *   Supports arbitrary method arguments and return values.
  # *   Can be configured to run in its own isolated Ractor or in a Thread in
  #     the current Ractor.
  # *   Can be configured per method whether to copy or move arguments and
  #     return values.
  # *   Blocks can be run in the calling Ractor or in the object Ractor.
  # *   Raises exceptions thrown by the method.
  # *   Can serialize method calls for non-thread-safe objects, or run methods
  #     concurrently in multiple worker threads for thread-safe objects.
  # *   Can gracefully shut down the wrapper and retrieve the original object.
  #
  # ## Caveats
  #
  # Ractor::Wrapper is subject to some limitations (and bugs) of Ractors, as of
  # Ruby 4.0.0.
  #
  # *   You can run blocks in the object Ractor only if the block does not
  #     access any data outside the block. Otherwise, the block must be run in
  #     the calling Ractor.
  # *   Certain types cannot be used as method arguments or return values
  #     because they cannot be moved between Ractors. These include threads,
  #     backtraces, and a few others.
  # *   Any exceptions raised are always copied (rather than moved) back to the
  #     calling Ractor, and the backtrace is cleared out. This is due to
  #     https://bugs.ruby-lang.org/issues/21818
  #
  class Wrapper
    ##
    # A stub that forwards calls to a wrapper.
    #
    # This object is shareable and can be passed to any Ractor.
    #
    class Stub
      ##
      # Create a stub given a wrapper.
      #
      # @param wrapper [Ractor::Wrapper]
      #
      def initialize(wrapper)
        @wrapper = wrapper
        freeze
      end

      ##
      # Forward calls to {Ractor::Wrapper#call}.
      # @private
      #
      def method_missing(name, ...)
        @wrapper.call(name, ...)
      end

      ##
      # Forward respond_to queries.
      # @private
      #
      def respond_to_missing?(name, include_all)
        @wrapper.call(:respond_to?, name, include_all)
      end
    end

    ##
    # Settings for a method call. Specifies how a method's arguments and
    # return value are communicated (i.e. copy or move semantics.)
    #
    class MethodSettings
      # @private
      def initialize(move_data: false,
                     move_arguments: nil,
                     move_results: nil,
                     move_block_arguments: nil,
                     move_block_results: nil,
                     execute_blocks_in_place: nil)
        @move_arguments = interpret_setting(move_arguments, move_data)
        @move_results = interpret_setting(move_results, move_data)
        @move_block_arguments = interpret_setting(move_block_arguments, move_data)
        @move_block_results = interpret_setting(move_block_results, move_data)
        @execute_blocks_in_place = interpret_setting(execute_blocks_in_place, false)
        freeze
      end

      ##
      # @return [Boolean] Whether to move arguments
      #
      def move_arguments?
        @move_arguments
      end

      ##
      # @return [Boolean] Whether to move return values
      #
      def move_results?
        @move_results
      end

      ##
      # @return [Boolean] Whether to move arguments to a block
      #
      def move_block_arguments?
        @move_block_arguments
      end

      ##
      # @return [Boolean] Whether to move block results
      #
      def move_block_results?
        @move_block_results
      end

      ##
      # @return [Boolean] Whether to call blocks in-place
      #
      def execute_blocks_in_place?
        @execute_blocks_in_place
      end

      private

      def interpret_setting(setting, default)
        if setting.nil?
          default ? true : false
        else
          setting ? true : false
        end
      end
    end

    ##
    # Create a wrapper around the given object.
    #
    # If you pass an optional block, the wrapper itself will be yielded to it,
    # at which time you can set additional configuration options. In
    # particular, method-specific configuration must be set in this block.
    # The configuration is frozen once the object is constructed.
    #
    # @param object [Object] The non-shareable object to wrap.
    # @param use_current_ractor [boolean] If true, the wrapper is run in a
    #     thread in the current Ractor instead of spawning a new Ractor (the
    #     default behavior). This option can be used if the wrapped object
    #     cannot be moved or must run in the main Ractor.
    # @param name [String] A name for this wrapper. Used during logging.
    # @param threads [Integer] The number of worker threads to run.
    #     Defaults to 1, which causes the worker to serialize calls into a
    #     single thread.
    # @param move_data [boolean] If true, all communication will by default
    #     move instead of copy arguments and return values. Default is false.
    #     This setting can be overridden by other `:move_*` settings.
    # @param move_arguments [boolean] If true, all arguments will be moved
    #     instead of copied by default. If not set, uses the `:move_data`
    #     setting.
    # @param move_results [boolean] If true, return values are moved instead of
    #     copied by default. If not set, uses the `:move_data` setting.
    # @param move_block_arguments [boolean] If true, arguments to blocks are
    #     moved instead of copied by default. If not set, uses the `:move_data`
    #     setting.
    # @param move_block_results [boolean] If true, result values from blocks
    #     are moved instead of copied by default. If not set, uses the
    #     `:move_data` setting.
    # @param execute_blocks_in_place [boolean] If true, blocks passed to
    #     methods are made shareable and passed into the wrapper to be executed
    #     in the wrapped environment. If false (the default), blocks are
    #     replaced by a proc that passes messages back out to the caller and
    #     executes the block in the caller's environment.
    # @param enable_logging [boolean] Set to true to enable logging. Default
    #     is false.
    #
    def initialize(object,
                   use_current_ractor: false,
                   name: nil,
                   threads: 1,
                   move_data: false,
                   move_arguments: nil,
                   move_results: nil,
                   move_block_arguments: nil,
                   move_block_results: nil,
                   execute_blocks_in_place: nil,
                   enable_logging: false)
      raise ::Ractor::MovedError, "cannot wrap a moved object" if ::Ractor::MovedObject === object

      @method_settings = {}
      self.name = name || object_id.to_s
      self.enable_logging = enable_logging
      self.threads = threads
      configure_method(move_data: move_data,
                       move_arguments: move_arguments,
                       move_results: move_results,
                       move_block_arguments: move_block_arguments,
                       move_block_results: move_block_results,
                       execute_blocks_in_place: execute_blocks_in_place)
      yield self if block_given?
      @method_settings.freeze

      if use_current_ractor
        setup_local_server(object)
      else
        setup_isolated_server(object)
      end
      @stub = Stub.new(self)

      freeze
    end

    ##
    # Set the number of threads to run in the wrapper. If the underlying object
    # is thread-safe, this allows concurrent calls to it. If the underlying
    # object is not thread-safe, you should leave this set to its default of 1,
    # which effectively causes calls to be serialized.
    #
    # This method can be called only during an initialization block.
    # All settings are frozen once the wrapper is active.
    #
    # @param value [Integer]
    #
    def threads=(value)
      value = value.to_i
      value = 1 if value < 1
      @threads = value
    end

    ##
    # Enable or disable internal debug logging.
    #
    # This method can be called only during an initialization block.
    # All settings are frozen once the wrapper is active.
    #
    # @param value [Boolean]
    #
    def enable_logging=(value)
      @enable_logging = value ? true : false
    end

    ##
    # Set the name of this wrapper. This is shown in logging, and is also used
    # as the name of the wrapping Ractor.
    #
    # This method can be called only during an initialization block.
    # All settings are frozen once the wrapper is active.
    #
    # @param value [String, nil]
    #
    def name=(value)
      @name = value ? value.to_s.freeze : nil
    end

    ##
    # Configure the move semantics for the given method (or the default
    # settings if no method name is given.) That is, determine whether
    # arguments, return values, and/or exceptions are copied or moved when
    # communicated with the wrapper. By default, all objects are copied.
    #
    # This method can be called only during an initialization block.
    # All settings are frozen once the wrapper is active.
    #
    # @param method_name [Symbol, nil] The name of the method being configured,
    #     or `nil` to set defaults for all methods not configured explicitly.
    # @param move_data [boolean] If true, communication for this method will
    #     move instead of copy arguments and return values. Default is false.
    #     This setting can be overridden by other `:move_*` settings.
    # @param move_arguments [boolean] If true, arguments for this method are
    #     moved instead of copied. If not set, uses the `:move_data` setting.
    # @param move_results [boolean] If true, return values for this method are
    #     moved instead of copied. If not set, uses the `:move_data` setting.
    # @param move_block_arguments [boolean] If true, arguments to blocks passed
    #     to this method are moved instead of copied. If not set, uses the
    #     `:move_data` setting.
    # @param move_block_results [boolean] If true, result values from blocks
    #     passed to this method are moved instead of copied. If not set, uses
    #     the `:move_data` setting.
    # @param execute_blocks_in_place [boolean] If true, blocks passed to this
    #     method are made shareable and passed into the wrapper to be executed
    #     in the wrapped environment. If false (the default), blocks are
    #     replaced by a proc that passes messages back out to the caller and
    #     executes the block in the caller's environment.
    #
    def configure_method(method_name = nil,
                         move_data: false,
                         move_arguments: nil,
                         move_results: nil,
                         move_block_arguments: nil,
                         move_block_results: nil,
                         execute_blocks_in_place: nil)
      method_name = method_name.to_sym unless method_name.nil?
      @method_settings[method_name] =
        MethodSettings.new(move_data: move_data,
                           move_arguments: move_arguments,
                           move_results: move_results,
                           move_block_arguments: move_block_arguments,
                           move_block_results: move_block_results,
                           execute_blocks_in_place: execute_blocks_in_place)
    end

    ##
    # Return the name of this wrapper.
    #
    # @return [String]
    #
    attr_reader :name

    ##
    # Determine whether this wrapper runs in the current Ractor
    #
    # @return [boolean]
    #
    def use_current_ractor?
      @ractor.nil?
    end

    ##
    # Return whether logging is enabled for this wrapper.
    #
    # @return [Boolean]
    #
    def enable_logging?
      @enable_logging
    end

    ##
    # Return the number of threads used by the wrapper.
    #
    # @return [Integer]
    #
    attr_reader :threads

    ##
    # Return the method settings for the given method name. This returns the
    # default method settings if the given method is not configured explicitly
    # by name.
    #
    # @param method_name [Symbol,nil] The method name, or `nil` to return the
    #     defaults.
    # @return [MethodSettings]
    #
    def method_settings(method_name)
      method_name = method_name.to_sym
      @method_settings[method_name] || @method_settings[nil]
    end

    ##
    # Return the wrapper stub. This is an object that responds to the same
    # methods as the wrapped object, providing an easy way to call a wrapper.
    #
    # @return [Ractor::Wrapper::Stub]
    #
    attr_reader :stub

    ##
    # A lower-level interface for calling methods through the wrapper.
    #
    # @param method_name [Symbol] The name of the method to call
    # @param args [arguments] The positional arguments
    # @param kwargs [keywords] The keyword arguments
    # @return [Object] The return value
    #
    def call(method_name, *args, **kwargs, &)
      reply_port = ::Ractor::Port.new
      transaction = ::Random.rand(7_958_661_109_946_400_884_391_936).to_s(36).freeze
      settings = method_settings(method_name)
      block_arg = make_block_arg(settings, &)
      message = CallMessage.new(method_name: method_name,
                                args: args,
                                kwargs: kwargs,
                                block_arg: block_arg,
                                transaction: transaction,
                                settings: settings,
                                reply_port: reply_port)
      maybe_log("Sending method", method_name: method_name, transaction: transaction)
      @port.send(message, move: settings.move_arguments?)
      loop do
        reply_message = reply_port.receive
        case reply_message
        when YieldMessage
          handle_yield(reply_message, transaction, settings, method_name, &)
        when ReturnMessage
          maybe_log("Received result", method_name: method_name, transaction: transaction)
          return reply_message.value
        when ExceptionMessage
          maybe_log("Received exception", method_name: method_name, transaction: transaction)
          raise reply_message.exception
        end
      end
    end

    ##
    # Request that the wrapper stop. All currently running calls will complete
    # before the wrapper actually terminates. However, any new calls will fail.
    #
    # This method is idempotent and can be called multiple times (even from
    # different ractors).
    #
    # @return [self]
    #
    def async_stop
      maybe_log("Stopping wrapper")
      @port.send(StopMessage.new.freeze)
      self
    rescue ::Ractor::ClosedError
      # Ignore to allow stops to be idempotent.
      self
    end

    ##
    # Blocks until the wrapper has fully stopped.
    #
    # @return [self]
    #
    def join
      if @ractor
        @ractor.join
      else
        reply_port = ::Ractor::Port.new
        @port.send(JoinMessage.new(reply_port))
        reply_port.receive
      end
      self
    rescue ::Ractor::ClosedError
      self
    end

    ##
    # Retrieves the original object that was wrapped. This should be called
    # only after a stop request has been issued using {#async_stop}, and may
    # block until the wrapper has fully stopped.
    #
    # This can be called only if the wrapper was *not* configured with
    # `use_current_ractor: true`. If the wrapper had that configuration, the
    # object will not be moved, and does not need to be recovered. In such a
    # case, any calls to this method will raise Ractor::Error.
    #
    # Only one ractor may call this method; any additional calls will fail with
    # a Ractor::Error.
    #
    # @return [Object] The original wrapped object
    #
    def recover_object
      raise ::Ractor::Error, "cannot recover an object from a local wrapper" unless @ractor
      @ractor.value
    end

    #### private items below ####

    ##
    # @private
    # Message sent to initialize a server.
    #
    InitMessage = ::Data.define(:object, :enable_logging, :threads)

    ##
    # @private
    # Message sent to a server to call a method
    #
    CallMessage = ::Data.define(:method_name, :args, :kwargs, :block_arg,
                                :transaction, :settings, :reply_port)

    ##
    # @private
    # Message sent to a server when a worker thread terminates
    #
    WorkerStoppedMessage = ::Data.define(:worker_num)

    ##
    # @private
    # Message sent to a server to request it to stop
    #
    StopMessage = ::Data.define

    ##
    # @private
    # Message sent to a server to request a join response
    #
    JoinMessage = ::Data.define(:reply_port)

    ##
    # @private
    # Message sent to report a return value
    #
    ReturnMessage = ::Data.define(:value)

    ##
    # @private
    # Message sent to report an exception result
    #
    ExceptionMessage = ::Data.define(:exception)

    ##
    # @private
    # Message sent from a server to request a yield block run
    #
    YieldMessage = ::Data.define(:args, :kwargs, :reply_port)

    private

    def setup_local_server(object)
      maybe_log("Starting local server")
      @ractor = nil
      @port = ::Ractor::Port.new
      ::Thread.new do
        Server.run_local(object: object,
                         port: @port,
                         name: name,
                         enable_logging: enable_logging?,
                         threads: threads)
      end
    end

    def setup_isolated_server(object)
      maybe_log("Starting isolated server")
      @ractor = ::Ractor.new(name, enable_logging?, threads, name: "wrapper:#{name}") do |name, enable_logging, threads|
        Server.run_isolated(name: name,
                            enable_logging: enable_logging,
                            threads: threads)
      end
      @port = @ractor.default_port
      @port.send(object, move: true)
    end

    def make_transaction
      ::Random.rand(7_958_661_109_946_400_884_391_936).to_s(36).freeze
    end

    def make_block_arg(settings, &)
      if !block_given?
        nil
      elsif settings.execute_blocks_in_place?
        ::Ractor.shareable_proc(&)
      else
        :send_block_message
      end
    end

    def handle_yield(message, transaction, settings, method_name)
      maybe_log("Yielding to block", method_name: method_name, transaction: transaction)
      begin
        block_result = yield(*message.args, **message.kwargs)
        maybe_log("Sending block result", method_name: method_name, transaction: transaction)
        message.reply_port.send(ReturnMessage.new(block_result), move: settings.move_block_results?)
      rescue ::Exception => e # rubocop:disable Lint/RescueException
        maybe_log("Sending block exception", method_name: method_name, transaction: transaction)
        begin
          message.reply_port.send(ExceptionMessage.new(e))
        rescue ::StandardError
          begin
            message.reply_port.send(ExceptionMessage.new(::StandardError.new(e.inspect)))
          rescue ::StandardError
            maybe_log("Failure to send block reply", method_name: method_name, transaction: transaction)
          end
        end
      end
    end

    def maybe_log(str, transaction: nil, method_name: nil)
      return unless enable_logging?
      metadata = [::Time.now.utc.strftime("%Y-%m-%dT%H:%M:%S.%L"), "Ractor::Wrapper/#{name}"]
      metadata << "Transaction/#{transaction}" if transaction
      metadata << "Method/#{method_name}" if method_name
      metadata = metadata.join(" ")
      $stderr.puts("[#{metadata}] #{str}")
      $stderr.flush
    end

    ##
    # This is the backend implementation of a wrapper. A Server runs within a
    # Ractor, and manages a shared object. It handles communication with
    # clients, translating those messages into method calls on the object. It
    # runs worker threads internally to handle actual method calls.
    #
    # See the {#run} method for an overview of the Server implementation and
    # lifecycle.
    #
    # @private
    #
    class Server
      ##
      # @private
      # Create and run a server in the current Ractor
      #
      def self.run_local(object:, port:, name:, enable_logging: false, threads: 1)
        server = new(isolated: false, object:, port:, name:, enable_logging:, threads:)
        server.run
      end

      ##
      # @private
      # Create and run a server in an isolated Ractor
      #
      def self.run_isolated(name:, enable_logging: false, threads: 1)
        port = ::Ractor.current.default_port
        server = new(isolated: true, object: nil, port:, name:, enable_logging:, threads:)
        server.run
      end

      def initialize(isolated:, object:, port:, name:, enable_logging:, threads:)
        @isolated = isolated
        @object = object
        @port = port
        @name = name
        @enable_logging = enable_logging
        @threads = threads
        @queue = ::Queue.new
        @join_requests = []
      end

      ##
      # Handle the server lifecycle, running through the following phases:
      #
      # *   **init**: Setup and spawning of worker threads.
      # *   **running**: Normal operation, until a stop request is received.
      # *   **stopping**: Waiting for worker threads to terminate.
      # *   **cleanup**: Clearing out of any lingering messages.
      #
      # The server returns the wrapped object, so one client can recover it.
      #
      def run
        receive_remote_object if @isolated
        start_threads
        running_phase
        stopping_phase
        cleanup_phase
        @object
      rescue ::StandardError => e
        maybe_log("Unexpected error: #{e.inspect}")
        @object
      end

      private

      def receive_remote_object
        maybe_log("Waiting for remote object")
        @object = @port.receive
      end

      def start_threads
        maybe_log("Spawning #{@threads} worker threads")
        (1..@threads).map do |worker_num|
          ::Thread.new { worker_thread(worker_num) }
        end
      end

      ##
      # In the **running phase**, the Server listens on the Ractor's inbox and
      # handles messages for normal operation:
      #
      # *   If it receives a `call` message, it adds it to the job queue from
      #     which a worker thread will pick it up.
      # *   If it receives a `stop` message, we proceed to the stopping phase.
      # *   If it receives a `thread_stopped` message, that indicates one of
      #     the worker threads has unexpectedly stopped. We don't expect this
      #     to happen until the stopping phase, so if we do see it here, we
      #     conclude that something has gone wrong, and we proceed to the
      #     stopping phase.
      #
      def running_phase
        loop do
          maybe_log("Waiting for message in running phase")
          message = @port.receive
          case message
          when CallMessage
            maybe_log("Received CallMessage", call_message: message)
            @queue.enq(message)
          when WorkerStoppedMessage
            maybe_log("Received unexpected WorkerStoppedMessage")
            @threads -= 1
            break
          when StopMessage
            maybe_log("Received stop")
            break
          when JoinMessage
            maybe_log("Received and queueing join request")
            @join_requests << message.reply_port
          end
        end
      end

      ##
      # In the **stopping phase**, we close the job queue, which signals to all
      # worker threads that they should finish their current task and then
      # terminate. We then wait for acknowledgement messages from all workers
      # before proceeding to the next phase. Any `call` requests received
      # during stopping are refused (i.e. we send back an error response.) Any
      # further `stop` requests are ignored.
      #
      def stopping_phase
        @queue.close
        while @threads.positive?
          maybe_log("Waiting for message in stopping phase")
          message = @port.receive
          case message
          when CallMessage
            refuse_method(message)
          when WorkerStoppedMessage
            maybe_log("Acknowledged WorkerStoppedMessage: #{message.worker_num}")
            @threads -= 1
          when StopMessage
            maybe_log("Stop received when already stopping")
          when JoinMessage
            maybe_log("Received and queueing join request")
            @join_requests << message.reply_port
          end
        end
      end

      ##
      # In the **cleanup phase**, The Server closes its inbox, and iterates
      # through one final time to ensure it has responded to all remaining
      # requests with a refusal.
      #
      def cleanup_phase
        maybe_log("Closing inbox")
        @port.close
        maybe_log("Responding to join requests")
        @join_requests.each { |port| send_join_reply(port) }
        maybe_log("Draining inbox")
        loop do
          message = begin
            @port.receive
          rescue ::Ractor::ClosedError
            maybe_log("Inbox is empty")
            nil
          end
          return if message.nil?
          case message
          when CallMessage
            refuse_method(message)
          when WorkerStoppedMessage
            maybe_log("Unexpected WorkerStoppedMessage when in cleanup")
          when StopMessage
            maybe_log("Stop received when already stopping")
          when JoinMessage
            maybe_log("Received and responding immediately to join request")
            send_join_reply(message.reply_port)
          end
        end
      end

      ##
      # A worker thread repeatedly pulls a method call requests off the job
      # queue, handles it, and sends back a response. It also removes the
      # request from the pending request list to signal that it has responded.
      # If no job is available, the thread blocks while waiting. If the queue
      # is closed, the worker will send an acknowledgement message and then
      # terminate.
      #
      def worker_thread(worker_num)
        maybe_log("Worker starting", worker_num: worker_num)
        loop do
          maybe_log("Waiting for job", worker_num: worker_num)
          message = @queue.deq
          break if message.nil?
          handle_method(message, worker_num: worker_num)
        end
      ensure
        maybe_log("Worker stopping", worker_num: worker_num)
        begin
          @port.send(WorkerStoppedMessage.new(worker_num))
        rescue ::Ractor::ClosedError
          maybe_log("Orphaned worker thread", worker_num: worker_num)
        end
      end

      ##
      # This is called within a worker thread to handle a method call request.
      # It calls the method on the wrapped object, and then sends back a
      # response to the caller. If an exception was raised, it sends back an
      # error response. It tries very hard always to send a response of some
      # kind; if an error occurs while constructing or sending a response, it
      # will catch the exception and try to send a simpler response.
      #
      def handle_method(message, worker_num: nil)
        block = make_block(message)
        maybe_log("Running method", worker_num: worker_num, call_message: message)
        begin
          result = @object.send(message.method_name, *message.args, **message.kwargs, &block)
          maybe_log("Sending return value", worker_num: worker_num, call_message: message)
          message.reply_port.send(ReturnMessage.new(result), move: message.settings.move_results?)
        rescue ::Exception => e # rubocop:disable Lint/RescueException
          maybe_log("Sending exception", worker_num: worker_num, call_message: message)
          begin
            message.reply_port.send(ExceptionMessage.new(e))
          rescue ::StandardError
            begin
              message.reply_port.send(ExceptionMessage.new(::StandardError.new(e.inspect)))
            rescue ::StandardError
              maybe_log("Failure to send method response", worker_num: worker_num, call_message: message)
            end
          end
        end
      end

      def make_block(message)
        return message.block_arg unless message.block_arg == :send_block_message
        proc do |*args, **kwargs|
          reply_port = ::Ractor::Port.new
          yield_message = YieldMessage.new(args: args, kwargs: kwargs, reply_port: reply_port)
          message.reply_port.send(yield_message, move: message.settings.move_block_arguments?)
          reply_message = reply_port.receive
          case reply_message
          when ExceptionMessage
            raise reply_message.exception
          when ReturnMessage
            reply_message.value
          end
        end
      end

      ##
      # This is called from the main Ractor thread to report to a caller that
      # the wrapper cannot handle a requested method call, likely because the
      # wrapper is shutting down.
      #
      def refuse_method(message)
        maybe_log("Refusing method call", call_message: message)
        begin
          error = ::Ractor::ClosedError.new("Wrapper is shutting down")
          message.reply_port.send(ExceptionMessage.new(error))
        rescue ::Ractor::Error
          maybe_log("Failure to send refusal message", call_message: message)
        end
      end

      def send_join_reply(port)
        port.send(nil)
      rescue ::Ractor::ClosedError
        maybe_log("Join reply port is closed")
      end

      def maybe_log(str, call_message: nil, worker_num: nil, transaction: nil, method_name: nil)
        return unless @enable_logging
        transaction ||= call_message&.transaction
        method_name ||= call_message&.method_name
        metadata = [::Time.now.utc.strftime("%Y-%m-%dT%H:%M:%S.%L"), "Ractor::Wrapper/#{@name}"]
        metadata << "Worker/#{worker_num}" if worker_num
        metadata << "Transaction/#{transaction}" if transaction
        metadata << "Method/#{method_name}" if method_name
        metadata = metadata.join(" ")
        $stderr.puts("[#{metadata}] #{str}")
        $stderr.flush
      end
    end
  end
end

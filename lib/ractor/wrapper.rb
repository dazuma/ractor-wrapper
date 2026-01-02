# frozen_string_literal: true

##
# See ruby-doc.org for info on Ractors.
#
class Ractor
  ##
  # An experimental class that wraps a non-shareable object, allowing multiple
  # Ractors to access it concurrently. This can make it possible for Ractors to
  # share a "plain" object such as a database connection.
  #
  # WARNING: This is a highly experimental library, and currently _not_
  # recommended for production use. (As of Ruby 4.0.0, the same can be said of
  # Ractors in general.)
  #
  # ## What is Ractor::Wrapper?
  #
  # Ractors for the most part cannot access objects concurrently with other
  # Ractors unless the object is _shareable_, which generally means deeply
  # immutable along with a few other restrictions. If multiple Ractors need to
  # interact with a shared resource that is stateful or otherwise not shareable
  # that resource must itself be implemented and accessed as a Ractor.
  #
  # `Ractor::Wrapper` makes it possible for such a shared resource to be
  # implemented as an object and accessed using ordinary method calls. It does
  # this by "wrapping" the object in a Ractor, and mapping method calls to
  # message passing. This may make it easier to implement such a resource with
  # a simple class rather than a full-blown Ractor with message passing, and it
  # may also be useful for adapting existing object-based resources.
  #
  # Given a shared resource object, `Ractor::Wrapper` starts a new Ractor and
  # "runs" the object within that Ractor. It provides you with a stub object
  # on which you can invoke methods. The wrapper responds to these method calls
  # by sending messages to the internal Ractor, which invokes the shared object
  # and then sends back the result. If the underlying object is thread-safe,
  # you can configure the wrapper to run multiple threads that can run methods
  # concurrently. Or, if not, the wrapper can serialize requests to the object.
  #
  # ## Example usage
  #
  # The following example shows how to share a single `Faraday::Conection`
  # object among multiple Ractors. Because `Faraday::Connection` is not itself
  # thread-safe, this example serializes all calls to it.
  #
  #     require "ractor/wrapper"
  #     require "faraday"
  #
  #     # Create a Faraday connection. Faraday connections are not shareable,
  #     # so normally only one Ractor can access them at a time.
  #     connection = Faraday.new("http://example.com")
  #
  #     # Create a wrapper around the connection. This starts up an internal
  #     # Ractor and "moves" the connection object to that Ractor.
  #     wrapper = Ractor::Wrapper.new(connection)
  #
  #     # At this point, the connection object can no longer be accessed
  #     # directly because it is now owned by the wrapper's internal Ractor.
  #     #     connection.get("/whoops")  # <= raises an error
  #
  #     # However, you can access the connection via the stub object provided
  #     # by the wrapper. This stub proxies the call to the wrapper's internal
  #     # Ractor. And it's shareable, so any number of Ractors can use it.
  #     wrapper.stub.get("/hello")
  #
  #     # Here, we start two Ractors, and pass the stub to each one. Each
  #     # Ractor can simply call methods on the stub as if it were the original
  #     # connection object. (Internally, of course, the calls are proxied back
  #     # to the wrapper.) By default, all calls are serialized. However, if
  #     # you know that the underlying object is thread-safe, you can configure
  #     # a wrapper to run calls concurrently.
  #     r1 = Ractor.new(wrapper.stub) do |conn|
  #       10.times do
  #         conn.get("/hello")
  #       end
  #       :ok
  #     end
  #     r2 = Ractor.new(wrapper.stub) do |conn|
  #       10.times do
  #         conn.get("/ruby")
  #       end
  #       :ok
  #     end
  #
  #     # Wait for the two above Ractors to finish.
  #     r1.join
  #     r2.join
  #
  #     # After you stop the wrapper, you can retrieve the underlying
  #     # connection object and access it directly again.
  #     wrapper.async_stop
  #     connection = wrapper.recover_object
  #     connection.get("/finally")
  #
  # ## Features
  #
  # *   Provides a method interface to an object running in its own Ractor.
  # *   Supports arbitrary method arguments and return values.
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
  #     because Ractor does not allow them to be moved between Ractors. These
  #     include threads, backtraces, and a few others.
  # *   Any exceptions raised are always copied back to the calling Ractor, and
  #     the backtrace is cleared out. This is due to
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
      def initialize(move: false,
                     move_arguments: nil,
                     move_result: nil,
                     execute_block_in_ractor: nil,
                     move_block_arguments: nil,
                     move_block_result: nil)
        @move_arguments = interpret_setting(move_arguments, move)
        @move_result = interpret_setting(move_result, move)
        @execute_block_in_ractor = interpret_setting(execute_block_in_ractor, false)
        @move_block_arguments = interpret_setting(move_block_arguments, move)
        @move_block_result = interpret_setting(move_block_result, move)
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
      def move_result?
        @move_result
      end

      ##
      # @return [Boolean] Whether to call blocks in-ractor
      #
      def execute_block_in_ractor?
        @execute_block_in_ractor
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
      def move_block_result?
        @move_block_result
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
    # @param name [String] A name for this wrapper. Used during logging.
    # @param logging_enabled [boolean] Set to true to enable logging. Default
    #     is false.
    # @param thread_count [Integer] The number of worker threads to run.
    #     Defaults to 1, which causes the worker to serialize calls into a
    #     single thread.
    # @param move [boolean] If true, all communication will by default move
    #     instead of copy arguments and return values. Default is false.
    # @param move_arguments [boolean] If true, all arguments will be moved
    #     instead of copied by default. If not set, uses the `:move` setting.
    # @param move_result [boolean] If true, return values will be moved instead
    #     of copied by default. If not set, uses the `:move` setting.
    #
    def initialize(object,
                   name: nil,
                   logging_enabled: false,
                   thread_count: 1,
                   move: false,
                   move_arguments: nil,
                   move_result: nil,
                   execute_block_in_ractor: nil,
                   move_block_arguments: nil,
                   move_block_result: nil)
      raise ::Ractor::MovedError, "can not wrap a moved object" if ::Ractor::MovedObject === object

      @method_settings = {}
      self.name = name || object_id.to_s
      self.logging_enabled = logging_enabled
      self.thread_count = thread_count
      configure_method(move: move,
                       move_arguments: move_arguments,
                       move_result: move_result,
                       execute_block_in_ractor: execute_block_in_ractor,
                       move_block_arguments: move_block_arguments,
                       move_block_result: move_block_result)
      yield self if block_given?
      @method_settings.freeze

      maybe_log("Starting server")
      @ractor = ::Ractor.new(self.name, name: "wrapper: #{name}") do |wrapper_name|
        Server.new(wrapper_name).run
      end
      init_message = InitMessage.new(object: object,
                                     logging_enabled: self.logging_enabled,
                                     thread_count: self.thread_count)
      @ractor.send(init_message, move: true)
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
    def thread_count=(value)
      value = value.to_i
      value = 1 if value < 1
      @thread_count = value
    end

    ##
    # Enable or disable internal debug logging.
    #
    # This method can be called only during an initialization block.
    # All settings are frozen once the wrapper is active.
    #
    # @param value [Boolean]
    #
    def logging_enabled=(value)
      @logging_enabled = value ? true : false
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
    # @param move [Boolean] Whether to move all communication. This value, if
    #     given, is used if `move_arguments`, `move_result`, or
    #     `move_exceptions` are not set.
    # @param move_arguments [Boolean] Whether to move arguments.
    # @param move_result [Boolean] Whether to move return values.
    #
    def configure_method(method_name = nil,
                         move: false,
                         move_arguments: nil,
                         move_result: nil,
                         execute_block_in_ractor: nil,
                         move_block_arguments: nil,
                         move_block_result: nil)
      method_name = method_name.to_sym unless method_name.nil?
      @method_settings[method_name] =
        MethodSettings.new(move: move,
                           move_arguments: move_arguments,
                           move_result: move_result,
                           execute_block_in_ractor: execute_block_in_ractor,
                           move_block_arguments: move_block_arguments,
                           move_block_result: move_block_result)
    end

    ##
    # Return the wrapper stub. This is an object that responds to the same
    # methods as the wrapped object, providing an easy way to call a wrapper.
    #
    # @return [Ractor::Wrapper::Stub]
    #
    attr_reader :stub

    ##
    # Return the number of threads used by the wrapper.
    #
    # @return [Integer]
    #
    attr_reader :thread_count

    ##
    # Return whether logging is enabled for this wrapper.
    #
    # @return [Boolean]
    #
    attr_reader :logging_enabled

    ##
    # Return the name of this wrapper.
    #
    # @return [String]
    #
    attr_reader :name

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
      block_arg = settings.execute_block_in_ractor? ? ::Ractor.shareable_proc(&) : :message
      message = CallMessage.new(method_name: method_name,
                                args: args,
                                kwargs: kwargs,
                                block_arg: block_arg,
                                transaction: transaction,
                                settings: settings,
                                reply_port: reply_port)
      maybe_log("Sending method", method_name: method_name, transaction: transaction)
      @ractor.send(message, move: settings.move_arguments?)
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
      @ractor.send(StopMessage.new.freeze)
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
      @ractor.join
      self
    end

    ##
    # Retrieves the original object that was wrapped. This should be called
    # only after a stop request has been issued using {#async_stop}, and may
    # block until the wrapper has fully stopped.
    #
    # Only one ractor may call this method; any additional calls will fail.
    #
    # @return [Object] The original wrapped object
    #
    def recover_object
      @ractor.value
    end

    #### private items below ####

    ##
    # @private
    # Message sent to initialize a server.
    #
    InitMessage = ::Data.define(:object, :logging_enabled, :thread_count)

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

    def handle_yield(message, transaction, settings, method_name)
      maybe_log("Yielding to block", method_name: method_name, transaction: transaction)
      begin
        block_result = yield(*message.args, **message.kwargs)
        maybe_log("Sending block result", method_name: method_name, transaction: transaction)
        message.reply_port.send(ReturnMessage.new(block_result), move: settings.move_block_result?)
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
      return unless logging_enabled
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
    # lifecycle. Server is stateful and not shareable.
    #
    # @private
    #
    class Server
      def initialize(name)
        @name = name
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
        init_phase
        running_phase
        stopping_phase
        cleanup_phase
        @object
      rescue ::StandardError => e
        maybe_log("Unexpected error: #{e.inspect}")
        @object
      end

      private

      ##
      # In the **init phase**, the Server:
      #
      # *   Receives an initial message providing the object to wrap, and
      #     server configuration such as thread count and communications
      #     settings.
      # *   Initializes the job queue.
      # *   Spawns worker threads.
      #
      def init_phase
        maybe_log("Waiting for initialization")
        init_message = ::Ractor.receive
        @object = init_message.object
        @logging_enabled = init_message.logging_enabled
        @thread_count = init_message.thread_count
        @queue = ::Queue.new
        maybe_log("Spawning #{@thread_count} worker threads")
        (1..@thread_count).map do |worker_num|
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
          maybe_log("Waiting for message")
          message = ::Ractor.receive
          case message
          when CallMessage
            maybe_log("Received CallMessage", call_message: message)
            @queue.enq(message)
          when WorkerStoppedMessage
            maybe_log("Received unexpected WorkerStoppedMessage")
            @thread_count -= 1
            break
          when StopMessage
            maybe_log("Received stop")
            break
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
        while @thread_count.positive?
          maybe_log("Refusing incoming messages while stopping")
          message = ::Ractor.receive
          case message
          when CallMessage
            begin
              refuse_method(message)
            rescue ::Ractor::ClosedError
              maybe_log("Reply port is closed", call_message: message)
            end
          when WorkerStoppedMessage
            maybe_log("Acknowledged WorkerStoppedMessage: #{message.worker_num}")
            @thread_count -= 1
          when StopMessage
            maybe_log("Stop received when already stopping")
          end
        end
      end

      ##
      # In the **cleanup phase**, The Server closes its inbox, and iterates
      # through one final time to ensure it has responded to all remaining
      # requests with a refusal.
      #
      def cleanup_phase
        ::Ractor.current.close
        maybe_log("Checking message queue for cleanup")
        loop do
          message = ::Ractor.receive
          case message
          when CallMessage
            begin
              refuse_method(message)
            rescue ::Ractor::ClosedError
              maybe_log("Reply port is closed", call_message: message)
            end
          when WorkerStoppedMessage
            maybe_log("Unexpected WorkerStoppedMessage when in cleanup")
          when StopMessage
            maybe_log("Stop received when already stopping")
          end
        end
      rescue ::Ractor::ClosedError
        maybe_log("Message queue is empty")
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
          ::Ractor.current.send(WorkerStoppedMessage.new(worker_num))
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
        block = message.block_arg
        block = make_proxy_block(message.reply_port, message.settings) if block == :message
        maybe_log("Running method", worker_num: worker_num, call_message: message)
        begin
          result = @object.send(message.method_name, *message.args, **message.kwargs, &block)
          maybe_log("Sending return value", worker_num: worker_num, call_message: message)
          message.reply_port.send(ReturnMessage.new(result), move: message.settings.move_result?)
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

      def make_proxy_block(port, settings)
        proc do |*args, **kwargs|
          reply_port = ::Ractor::Port.new
          yield_message = YieldMessage.new(args: args, kwargs: kwargs, reply_port: reply_port)
          port.send(yield_message, move: settings.move_block_arguments?)
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
        rescue ::StandardError
          maybe_log("Failure to send refusal message", call_message: message)
        end
      end

      def maybe_log(str, call_message: nil, worker_num: nil, transaction: nil, method_name: nil)
        return unless @logging_enabled
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

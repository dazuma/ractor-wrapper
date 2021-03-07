##
# See ruby-doc.org for info on Ractors.
#
class Ractor
  ##
  # An experimental class that wraps a non-shareable object, allowing multiple
  # Ractors to access it concurrently.
  #
  # WARNING: This is a highly experimental library, and currently _not_
  # recommended for production use. (As of Ruby 3.0.0, the same can be said of
  # Ractors in general.)
  #
  # ## What is Ractor::Wrapper?
  #
  # Ractors for the most part cannot access objects concurrently with other
  # Ractors unless the object is _shareable_ (that is, deeply immutable along
  # with a few other restrictions.) If multiple Ractors need to interact with a
  # shared resource that is stateful or otherwise not Ractor-shareable, that
  # resource must itself be implemented and accessed as a Ractor.
  #
  # `Ractor::Wrapper` makes it possible for such a shared resource to be
  # implemented as an object and accessed using ordinary method calls. It does
  # this by "wrapping" the object in a Ractor, and mapping method calls to
  # message passing. This may make it easier to implement such a resource with
  # a simple class rather than a full-blown Ractor with message passing, and it
  # may also useful for adapting existing legacy object-based implementations.
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
  #     require "faraday"
  #
  #     # Create a Faraday connection and a wrapper for it.
  #     connection = Faraday.new "http://example.com"
  #     wrapper = Ractor::Wrapper.new(connection)
  #
  #     # At this point, the connection object cannot be accessed directly
  #     # because it has been "moved" to the wrapper's internal Ractor.
  #     #     connection.get("/whoops")  # <= raises an error
  #
  #     # However, any number of Ractors can now access it through the wrapper.
  #     # By default, access to the object is serialized; methods will not be
  #     # invoked concurrently.
  #     r1 = Ractor.new(wrapper) do |w|
  #       10.times do
  #         w.stub.get("/hello")
  #       end
  #       :ok
  #     end
  #     r2 = Ractor.new(wrapper) do |w|
  #       10.times do
  #         w.stub.get("/ruby")
  #       end
  #       :ok
  #     end
  #
  #     # Wait for the two above Ractors to finish.
  #     r1.take
  #     r2.take
  #
  #     # After you stop the wrapper, you can retrieve the underlying
  #     # connection object and access it directly again.
  #     wrapper.async_stop
  #     connection = wrapper.recover_object
  #     connection.get("/finally")
  #
  # ## Features
  #
  # *   Provides a method interface to an object running in a different Ractor.
  # *   Supports arbitrary method arguments and return values.
  # *   Supports exceptions thrown by the method.
  # *   Can be configured to copy or move arguments, return values, and
  #     exceptions, per method.
  # *   Can serialize method calls for non-concurrency-safe objects, or run
  #     methods concurrently in multiple worker threads for thread-safe objects.
  # *   Can gracefully shut down the wrapper and retrieve the original object.
  #
  # ## Caveats
  #
  # Ractor::Wrapper is subject to some limitations (and bugs) of Ractors, as of
  # Ruby 3.0.0.
  #
  # *   You cannot pass blocks to wrapped methods.
  # *   Certain types cannot be used as method arguments or return values
  #     because Ractor does not allow them to be moved between Ractors. These
  #     include threads, procs, backtraces, and a few others.
  # *   You can call wrapper methods from multiple Ractors concurrently, but
  #     you cannot call them from multiple Threads within a single Ractor.
  #     (This is due to https://bugs.ruby-lang.org/issues/17624)
  # *   If you close the incoming port on a Ractor, it will no longer be able
  #     to call out via a wrapper. If you close its incoming port while a call
  #     is currently pending, that call may hang. (This is due to
  #     https://bugs.ruby-lang.org/issues/17617)
  #
  class Wrapper
    ##
    # Create a wrapper around the given object.
    #
    # If you pass an optional block, the wrapper itself will be yielded to it
    # at which time you can set additional configuration options. (The
    # configuration is frozen once the object is constructed.)
    #
    # @param object [Object] The non-shareable object to wrap.
    # @param threads [Integer] The number of worker threads to run.
    #     Defaults to 1, which causes the worker to serialize calls.
    #
    def initialize(object,
                   threads: 1,
                   move: false,
                   move_arguments: nil,
                   move_return: nil,
                   logging: false,
                   name: nil)
      @method_settings = {}
      self.threads = threads
      self.logging = logging
      self.name = name
      configure_method(move: move, move_arguments: move_arguments, move_return: move_return)
      yield self if block_given?
      @method_settings.freeze

      maybe_log("Starting server")
      @ractor = ::Ractor.new(name: name) { Server.new.run }
      opts = {
        object: object,
        threads: @threads,
        method_settings: @method_settings,
        name: @name,
        logging: @logging,
      }
      @ractor.send(opts, move: true)

      maybe_log("Server ready")
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
    def logging=(value)
      @logging = value ? true : false
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
    #     given, is used if `move_arguments`, `move_return`, or
    #     `move_exceptions` are not set.
    # @param move_arguments [Boolean] Whether to move arguments.
    # @param move_return [Boolean] Whether to move return values.
    #
    def configure_method(method_name = nil,
                         move: false,
                         move_arguments: nil,
                         move_return: nil)
      method_name = method_name.to_sym unless method_name.nil?
      @method_settings[method_name] =
        MethodSettings.new(move: move, move_arguments: move_arguments, move_return: move_return)
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
    attr_reader :threads

    ##
    # Return whether logging is enabled for this wrapper.
    #
    # @return [Boolean]
    #
    attr_reader :logging

    ##
    # Return the name of this wrapper.
    #
    # @return [String, nil]
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
    # A lower-level interface for calling the wrapper.
    #
    # @param method_name [Symbol] The name of the method to call
    # @param args [arguments] The positional arguments
    # @param kwargs [keywords] The keyword arguments
    # @return [Object] The return value
    #
    def call(method_name, *args, **kwargs)
      request = Message.new(:call, data: [method_name, args, kwargs])
      transaction = request.transaction
      move = method_settings(method_name).move_arguments?
      maybe_log("Sending method #{method_name} (move=#{move}, transaction=#{transaction})")
      @ractor.send(request, move: move)
      reply = ::Ractor.receive_if { |msg| msg.is_a?(Message) && msg.transaction == transaction }
      case reply.type
      when :result
        maybe_log("Received result for method #{method_name} (transaction=#{transaction})")
        reply.data
      when :error
        maybe_log("Received exception for method #{method_name} (transaction=#{transaction})")
        raise reply.data
      end
    end

    ##
    # Request that the wrapper stop. All currently running calls will complete
    # before the wrapper actually terminates. However, any new calls will fail.
    #
    # This metnod is idempotent and can be called multiple times (even from
    # different ractors).
    #
    # @return [self]
    #
    def async_stop
      maybe_log("Stopping #{name}")
      @ractor.send(Message.new(:stop))
      self
    rescue ::Ractor::ClosedError
      # Ignore to allow stops to be idempotent.
      self
    end

    ##
    # Return the original object that was wrapped. The object is returned after
    # the wrapper finishes stopping. Only one ractor may call this method; any
    # additional calls will fail.
    #
    # @return [Object] The original wrapped object
    #
    def recovered_object
      @ractor.take
    end

    private

    def maybe_log(str)
      return unless logging
      time = ::Time.now.utc.strftime("%Y-%m-%dT%H:%M:%S.%L")
      $stderr.puts("[#{time} Ractor::Wrapper/#{name}]: #{str}")
      $stderr.flush
    end

    ##
    # A stub that forwards calls to a wrapper.
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
      def method_missing(name, *args, **kwargs)
        @wrapper.call(name, *args, **kwargs)
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
                     move_return: nil)
        @move_arguments = interpret_setting(move_arguments, move)
        @move_return = interpret_setting(move_return, move)
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
      def move_return?
        @move_return
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
    # The class of all messages passed between a client Ractor and a wrapper.
    # This helps the wrapper distinguish these messages from any other messages
    # that might be received by a client Ractor.
    #
    # Any Ractor that calls a wrapper may receive messages of this type when
    # the call is in progress. If a Ractor interacts with its incoming message
    # queue concurrently while a wrapped call is in progress, it must ignore
    # these messages (i.e. by by using `receive_if`) in order not to interfere
    # with the wrapper. (Similarly, the wrapper will use `receive_if` to
    # receive only messages of this type, so it does not interfere with your
    # Ractor's functionality.)
    #
    class Message
      # @private
      def initialize(type, data: nil, transaction: nil)
        @sender = ::Ractor.current
        @type = type
        @data = data
        @transaction = transaction || new_transaction
        freeze
      end

      # @private
      attr_reader :type

      # @private
      attr_reader :sender

      # @private
      attr_reader :transaction

      # @private
      attr_reader :data

      private

      def new_transaction
        ::Random.rand(7958661109946400884391936).to_s(36).freeze
      end
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
      # Handle the server lifecycle, running through the following phases:
      #
      # *   **init**: Setup and spawning of worker threads.
      # *   **running**: Normal operation, until a stop request is received.
      # *   **stopping**: Waiting for worker threads to terminate.
      # *   **cleanup**: Clearing out of any lingering meessages.
      #
      # The server returns the wrapped object, allowing one client Ractor to
      # take it.
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
      # *   Initializes the job queue and the pending request list.
      # *   Spawns worker threads.
      #
      def init_phase
        opts = ::Ractor.receive
        @object = opts[:object]
        @logging = opts[:logging]
        @name = opts[:name]
        @method_settings = opts[:method_settings]
        @thread_count = opts[:threads]
        @queue = ::Queue.new
        @mutex = ::Mutex.new
        @current_calls = {}
        maybe_log("Spawning #{@thread_count} threads")
        (1..@thread_count).map do |worker_num|
          ::Thread.new { worker_thread(worker_num) }
        end
        maybe_log("Server initialized")
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
        maybe_worker_log(worker_num, "Starting")
        loop do
          maybe_worker_log(worker_num, "Waiting for job")
          request = @queue.deq
          break if request.nil?
          handle_method(worker_num, request)
          unregister_call(request.transaction)
        end
      ensure
        maybe_worker_log(worker_num, "Stopping")
        ::Ractor.current.send(Message.new(:thread_stopped, data: worker_num), move: true)
      end

      ##
      # In the **running phase**, the Server listens on the Ractor's inbox and
      # handles messages for normal operation:
      #
      # *   If it receives a `call` request, it adds it to the job queue from
      #     which a worker thread will pick it up. It also adds the request to
      #     a list of pending requests.
      # *   If it receives a `stop` request, we proceed to the stopping phase.
      # *   If it receives a `thread_stopped` message, that indicates one of
      #     the worker threads has unexpectedly stopped. We don't expect this
      #     to happen until the stopping phase, so if we do see it here, we
      #     conclude that something has gone wrong, and we proceed to the
      #     stopping phase.
      #
      def running_phase
        loop do
          maybe_log("Waiting for message")
          request = ::Ractor.receive
          next unless request.is_a?(Message)
          case request.type
          when :call
            @queue.enq(request)
            register_call(request)
            maybe_log("Queued method #{request.data.first} (transaction=#{request.transaction})")
          when :thread_stopped
            maybe_log("Thread unexpectedly stopped: #{request.data}")
            @thread_count -= 1
            break
          when :stop
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
          maybe_log("Waiting for message while stopping")
          message = ::Ractor.receive
          next unless request.is_a?(Message)
          case message.type
          when :call
            refuse_method(message)
          when :thread_stopped
            @thread_count -= 1
          end
        end
      end

      ##
      # In the **cleanup phase**, The Server closes its inbox, and iterates
      # through one final time to ensure it has responded to all remaining
      # requests with a refusal. It also makes another pass through the pending
      # requests; if there are any left, it probably means a worker thread died
      # without responding to it preoprly, so we send back an error message.
      #
      def cleanup_phase
        ::Ractor.current.close_incoming
        maybe_log("Checking message queue for cleanup")
        loop do
          message = ::Ractor.receive
          refuse_method(message) if message.is_a?(Message) && message.type == :call
        end
        maybe_log("Checking current calls for cleanup")
        @current_calls.each_value do |request|
          refuse_method(request)
        end
      rescue ::Ractor::ClosedError
        maybe_log("Message queue is empty")
      end

      ##
      # This is called within a worker thread to handle a method call request.
      # It calls the method on the wrapped object, and then sends back a
      # response to the caller. If an exception was raised, it sends back an
      # error response. It tries very hard always to send a response of some
      # kind; if an error occurs while constructing or sending a response, it
      # will catch the exception and try to send a simpler response.
      #
      def handle_method(worker_num, request)
        method_name, args, kwargs = request.data
        transaction = request.transaction
        sender = request.sender
        maybe_worker_log(worker_num, "Running method #{method_name} (transaction=#{transaction})")
        begin
          result = @object.send(method_name, *args, **kwargs)
          maybe_worker_log(worker_num, "Sending result (transaction=#{transaction})")
          sender.send(Message.new(:result, data: result, transaction: transaction),
                      move: (@method_settings[method_name] || @method_settings[nil]).move_return?)
        rescue ::Exception => e # rubocop:disable Lint/RescueException
          maybe_worker_log(worker_num, "Sending exception (transaction=#{transaction})")
          begin
            sender.send(Message.new(:error, data: e, transaction: transaction))
          rescue ::StandardError
            safe_error = begin
              ::StandardError.new(e.inspect)
            rescue ::StandardError
              ::StandardError.new("Unknown error")
            end
            sender.send(Message.new(:error, data: safe_error, transaction: transaction))
          end
        end
      end

      ##
      # This is called from the main Ractor thread to report to a caller that
      # the wrapper cannot handle a requested method call, likely because the
      # wrapper is shutting down.
      #
      def refuse_method(request)
        maybe_log("Refusing method call (transaction=#{message.transaction})")
        error = ::Ractor::ClosedError.new
        request.sender.send(Message.new(:error, data: error, transaction: message.transaction))
      end

      def register_call(request)
        @mutex.synchronize do
          @current_calls[request.transaction] = request
        end
      end

      def unregister_call(transaction)
        @mutex.synchronize do
          @current_calls.delete(transaction)
        end
      end

      def maybe_log(str)
        return unless @logging
        time = ::Time.now.utc.strftime("%Y-%m-%dT%H:%M:%S.%L")
        $stderr.puts("[#{time} Ractor::Wrapper/#{@name} Server]: #{str}")
        $stderr.flush
      end

      def maybe_worker_log(worker_num, str)
        return unless @logging
        time = ::Time.now.utc.strftime("%Y-%m-%dT%H:%M:%S.%L")
        $stderr.puts("[#{time} Ractor::Wrapper/#{@name} Worker/#{worker_num}]: #{str}")
        $stderr.flush
      end
    end
  end
end

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
    # @param threads [Integer,nil] The number of worker threads to run.
    #     Defaults to `nil`, which causes the worker to serialize calls.
    #
    def initialize(object,
                   threads: nil,
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
    # object is not thread-safe, you should leave this set to `nil`, which will
    # cause calls to be serialized. Setting the thread count to 1 will actually
    # spawn a single thread, although this is effectively the same as no
    # threading since a single thread will serialize calls.
    #
    # This method can be called only during an initialization block.
    # All settings are frozen once the wrapper is active.
    #
    # @param value [Integer,nil]
    #
    def threads=(value)
      if value
        value = value.to_i
        value = 1 if value < 1
        @threads = value
      else
        @threads = nil
      end
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
    # Return the number of threads used by the wrapper, or `nil` for no
    # no threading.
    #
    # @return [Integer, nil]
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
    # Settings for a method
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

    # @private
    class Message
      def initialize(type, data: nil, transaction: nil)
        @sender = ::Ractor.current
        @type = type
        @data = data
        @transaction = transaction || new_transaction
        freeze
      end

      attr_reader :type
      attr_reader :sender
      attr_reader :transaction
      attr_reader :data

      private

      def new_transaction
        ::Random.rand(7958661109946400884391936).to_s(36).freeze
      end
    end

    # @private
    class Server
      def run
        opts = ::Ractor.receive
        @object = opts[:object]
        @logging = opts[:logging]
        @name = opts[:name]
        @method_settings = opts[:method_settings]
        maybe_log("Server started")

        queue = start_threads(opts[:threads])
        running_phase(queue)
        stopping_phase if queue
        cleanup_phase

        @object
      rescue ::StandardError => e
        maybe_log("Unexpected error: #{e.inspect}")
        @object
      end

      private

      def start_threads(thread_count)
        return nil unless thread_count
        queue = ::Queue.new
        maybe_log("Spawning #{thread_count} threads")
        threads = (1..thread_count).map do |worker_num|
          ::Thread.new { worker_thread(worker_num, queue) }
        end
        ::Thread.new { monitor_thread(threads) }
        queue
      end

      def worker_thread(worker_num, queue)
        maybe_worker_log(worker_num, "Starting")
        loop do
          maybe_worker_log(worker_num, "Waiting for job")
          request = queue.deq
          if request.nil?
            break
          end
          handle_method(worker_num, request)
        end
        maybe_worker_log(worker_num, "Stopping")
      end

      def monitor_thread(workers)
        workers.each(&:join)
        maybe_log("All workers finished")
        ::Ractor.current.send(Message.new(:threads_stopped))
      end

      def running_phase(queue)
        loop do
          maybe_log("Waiting for message")
          request = ::Ractor.receive_if { |msg| msg.is_a?(Message) }
          case request.type
          when :call
            if queue
              queue.enq(request)
              maybe_log("Queued method #{request.data.first} (transaction=#{request.transaction})")
            else
              handle_method(0, request)
            end
          when :stop
            maybe_log("Received stop")
            queue&.close
            break
          end
        end
      end

      def stopping_phase
        loop do
          maybe_log("Waiting for message")
          message = ::Ractor.receive_if { |msg| msg.is_a?(Message) }
          case message.type
          when :call
            refuse_method(message)
          when :threads_stopped
            break
          end
        end
      end

      def cleanup_phase
        ::Ractor.current.close_incoming
        loop do
          maybe_log("Checking queue for cleanup")
          message = ::Ractor.receive
          refuse_method(message) if message.is_a?(Message) && message.type == :call
        end
      rescue ::Ractor::ClosedError
        maybe_log("Queue is empty")
      end

      def handle_method(worker_num, request)
        method_name, args, kwargs = request.data
        transaction = request.transaction
        sender = request.sender
        method_settings = @method_settings[method_name] || @method_settings[nil]
        maybe_worker_log(worker_num, "Running method #{method_name} (transaction=#{transaction})")
        begin
          result = @object.send(method_name, *args, **kwargs)
          maybe_worker_log(worker_num, "Sending result (transaction=#{transaction})")
          sender.send(Message.new(:result, data: result, transaction: transaction),
                      move: method_settings.move_return?)
        rescue ::Exception => e # rubocop:disable Lint/RescueException
          maybe_worker_log(worker_num, "Sending exception (transaction=#{transaction})")
          sender.send(Message.new(:error, data: e, transaction: transaction))
        end
      end

      def refuse_method(request)
        maybe_log("Refusing method call (transaction=#{message.transaction})")
        error = ::Ractor::ClosedError.new
        request.sender.send(Message.new(:error, data: error, transaction: message.transaction))
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

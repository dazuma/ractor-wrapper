# frozen_string_literal: true

##
# See https://docs.ruby-lang.org/en/4.0/language/ractor_md.html for info on
# Ractors.
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
  #     # in the current Ractor. We can also configure it to run multiple
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
  #     # wrapper's worker threads will handle the requests concurrently.
  #     r1 = Ractor.new(wrapper.stub) do |stub|
  #       5.times do
  #         stub.execute("select * from numbers")
  #       end
  #       :ok
  #     end
  #     r2 = Ractor.new(wrapper.stub) do |stub|
  #       5.times do
  #         stub.execute("select * from numbers")
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
  #     #     db2 = wrapper.recover_object  # <= raises Ractor::Wrapper::Error
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
  # *   Blocks running in the calling Ractor can re-enter the wrapper, calling
  #     other methods on it without deadlocking.
  # *   Raises exceptions thrown by the method.
  # *   Can serialize method calls for non-thread-safe objects, or run methods
  #     concurrently in multiple worker threads for thread-safe objects.
  # *   Can gracefully shut down the wrapper and retrieve the original object.
  #
  # ## Caveats
  #
  # *   Certain types cannot be used as method arguments or return values
  #     because they cannot be moved between Ractors. As of Ruby 4.0.0, these
  #     include threads, backtraces, procs, and a few others.
  # *   As of Ruby 4.0.0, any exceptions raised are always copied (rather than
  #     moved) back to the calling Ractor, and the backtrace is cleared out.
  #     This is due to https://bugs.ruby-lang.org/issues/21818
  # *   Blocks can be run "in place" (i.e. in the wrapped object context) only
  #     if the block does not access any data outside the block. Otherwise, the
  #     block must be run in caller's context.
  # *   Blocks configured to run in the caller's context can only be run while
  #     a method is executing. They cannot be "saved" as a proc to be run
  #     later unless they are configured to run "in place". In particular,
  #     using blocks as a syntax to define callbacks can generally not be done
  #     through a wrapper.
  # *   Re-entrant calls from a block are not safe if the wrapped method
  #     invoked the block from a nested Fiber (such as inside an Enumerator)
  #     or from a spawned Thread. Such re-entrant calls may deadlock.
  #
  class Wrapper
    ##
    # Base class for errors raised by {Ractor::Wrapper}.
    #
    class Error < ::Ractor::Error; end

    ##
    # Raised when a {Ractor::Wrapper} server has crashed unexpectedly. May
    # also be raised in the calling Ractor when an in-flight method call is
    # suspended at a block-yield point and the server (or its worker thread)
    # crashes before the block result can be delivered.
    #
    class CrashedError < Error; end

    ##
    # Raised when calling a method on a {Ractor::Wrapper} whose server has
    # stopped and is no longer accepting calls.
    #
    class StoppedError < Error; end

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
    # Configuration for a {Ractor::Wrapper}. An instance of this class is
    # yielded by {Ractor::Wrapper#initialize} if a block is provided. Any
    # settings made to the Configuration before the block returns take
    # effect when the Wrapper is constructed.
    #
    class Configuration
      ##
      # Set the name of the wrapper. This is shown in logging and is also
      # used as the name of the wrapping Ractor.
      #
      # @param value [String, nil]
      #
      def name=(value)
        @name = value ? value.to_s.freeze : nil
      end

      ##
      # Enable or disable internal debug logging.
      #
      # @param value [Boolean]
      #
      def enable_logging=(value)
        @enable_logging = value ? true : false
      end

      ##
      # Set the number of worker threads. If the underlying object is
      # thread-safe, a value of 2 or more allows concurrent calls. Leave at
      # the default of 0 to handle calls sequentially without worker threads.
      #
      # The number of worker threads only needs to reflect the desired
      # concurrency of independent calls. It does not need to be sized to the
      # depth of re-entrant block calls, because suspended methods do not
      # occupy a worker thread while waiting for a block to complete.
      #
      # @param value [Integer]
      #
      def threads=(value)
        value = value.to_i
        value = 0 if value.negative?
        @threads = value
      end

      ##
      # If set to true, the wrapper server runs as Thread(s) inside the
      # current Ractor rather than spawning a new isolated Ractor. Use this
      # for objects that cannot be moved between Ractors.
      #
      # @param value [Boolean]
      #
      def use_current_ractor=(value)
        @use_current_ractor = value ? true : false
      end

      ##
      # Configure how argument and return values are communicated for the given
      # method.
      #
      # In general, the following values are recognized for the data-moving
      # settings:
      #
      # * `:copy` - Method arguments or return values that are not shareable,
      #   are *deep copied* when communicated between the caller and the object.
      # * `:move` - Method arguments or return values that are not shareable,
      #   are *moved* when communicated between the caller and the object. This
      #   means they are no longer available to the source; that is, the caller
      #   can no longer access objects that were moved to method arguments, and
      #   the wrapped object can no longer access objects that were used as
      #   return values.
      # * `:void` - This option is available for return values and block
      #   results. It disables return values for the given method, and is
      #   intended to avoid copying or moving objects that are not intended to
      #   be return values. The recipient will receive `nil`.
      #
      # The following settings are recognized for the `block_environment`
      # setting:
      #
      # * `:caller` - Blocks are executed in the caller's context. This means
      #   the wrapper sends a message back to the caller to execute the block
      #   in its original context. This means the block will have access to its
      #   lexical scope and any other data available to the calling Ractor.
      #   Such blocks may safely re-enter the wrapper to invoke other methods
      #   on it, *unless* the wrapped method invoked the block from a nested
      #   Fiber (such as inside an Enumerator) or a spawned Thread, in which
      #   case re-entrant calls from the block may deadlock. If you need to
      #   invoke the block from a nested Fiber or a spawned Thread and the
      #   block does not need re-entrancy, prefer the `:wrapped` setting.
      # * `:wrapped` - Blocks are executed directly in the wrapped object's
      #   context. This does not require any communication, but it means the
      #   block is removed from the caller's environment and does not have
      #   access to the caller's lexical scope or Ractor-accessible data.
      #
      # All settings are optional. If not provided, they will fall back to a
      # default. If you are configuring a particular method, by specifying the
      # `method_name` argument, any unspecified setting will fall back to the
      # method default settings (which you can set by omitting the method name.)
      # If you are configuring the method default settings, by omitting the
      # `method_name` argument, unspecified settings will fall back to `:copy`
      # for the data movement settings, and `:caller` for the
      # `block_environment` setting.
      #
      # @param method_name [Symbol,nil] The name of the method being configured,
      #     or `nil` to set defaults for all methods not configured explicitly.
      # @param arguments [:move,:copy] How to communicate method arguments.
      # @param results [:move,:copy,:void] How to communicate method return
      #     values.
      # @param block_arguments [:move,:copy] How to communicate block arguments.
      # @param block_results [:move,:copy,:void] How to communicate block
      #     result values.
      # @param block_environment [:caller,:wrapped] How to execute blocks, and
      #     what scope blocks have access to.
      #
      def configure_method(method_name = nil,
                           arguments: nil,
                           results: nil,
                           block_arguments: nil,
                           block_results: nil,
                           block_environment: nil)
        method_name = method_name.to_sym unless method_name.nil?
        @method_settings[method_name] =
          MethodSettings.new(arguments: arguments,
                             results: results,
                             block_arguments: block_arguments,
                             block_results: block_results,
                             block_environment: block_environment)
        self
      end

      ##
      # @private
      # Return the name of the wrapper.
      #
      # @return [String, nil]
      #
      attr_reader :name

      ##
      # @private
      # Return whether logging is enabled.
      #
      # @return [Boolean]
      #
      attr_reader :enable_logging

      ##
      # @private
      # Return the number of worker threads.
      #
      # @return [Integer]
      #
      attr_reader :threads

      ##
      # @private
      # Return whether the wrapper runs in the current Ractor.
      #
      # @return [Boolean]
      #
      attr_reader :use_current_ractor

      ##
      # @private
      # Resolve the method settings by filling in the defaults for all fields
      # not explicitly set, and return the final settings keyed by method name.
      # The `nil` key will contain defaults for method names not explicitly
      # configured. This hash will be frozen and shareable.
      #
      # @return [Hash{(Symbol,nil)=>MethodSettings}]
      #
      def final_method_settings
        fallback = MethodSettings.new(arguments: :copy, results: :copy,
                                      block_arguments: :copy, block_results: :copy,
                                      block_environment: :caller)
        defaults = MethodSettings.with_fallback(@method_settings[nil], fallback)
        results = {nil => defaults}
        @method_settings.each do |name, settings|
          next if name.nil?
          results[name] = MethodSettings.with_fallback(settings, defaults)
        end
        results.freeze
      end

      ##
      # @private
      # Create an empty configuration.
      #
      def initialize
        @method_settings = {}
        configure_method(arguments: nil,
                         results: nil,
                         block_arguments: nil,
                         block_results: nil,
                         block_environment: nil)
      end
    end

    ##
    # Settings for a method call. Specifies how a method's arguments and
    # return value are communicated (i.e. copy or move semantics.)
    #
    class MethodSettings
      # @private
      def initialize(arguments: nil,
                     results: nil,
                     block_arguments: nil,
                     block_results: nil,
                     block_environment: nil)
        unless [nil, :copy, :move].include?(arguments)
          raise ::ArgumentError, "Unknown `arguments`: #{arguments.inspect} (must be :copy or :move)"
        end
        unless [nil, :copy, :move, :void].include?(results)
          raise ::ArgumentError, "Unknown `results`: #{results.inspect} (must be :copy, :move, or :void)"
        end
        unless [nil, :copy, :move].include?(block_arguments)
          raise ::ArgumentError, "Unknown `block_arguments`: #{block_arguments.inspect} (must be :copy or :move)"
        end
        unless [nil, :copy, :move, :void].include?(block_results)
          raise ::ArgumentError, "Unknown `block_results`: #{block_results.inspect} (must be :copy, :move, or :void)"
        end
        unless [nil, :caller, :wrapped].include?(block_environment)
          raise ::ArgumentError,
                "Unknown `block_environment`: #{block_environment.inspect} (must be :caller or :wrapped)"
        end
        @arguments = arguments
        @results = results
        @block_arguments = block_arguments
        @block_results = block_results
        @block_environment = block_environment
        freeze
      end

      ##
      # @return [:copy,:move] How to communicate method arguments
      # @return [nil] if not set (will not happen in final settings)
      #
      attr_reader :arguments

      ##
      # @return [:copy,:move,:void] How to communicate method return values
      # @return [nil] if not set (will not happen in final settings)
      #
      attr_reader :results

      ##
      # @return [:copy,:move] How to communicate arguments to a block
      # @return [nil] if not set (will not happen in final settings)
      #
      attr_reader :block_arguments

      ##
      # @return [:copy,:move,:void] How to communicate block results
      # @return [nil] if not set (will not happen in final settings)
      #
      attr_reader :block_results

      ##
      # @return [:caller,:wrapped] What environment blocks execute in
      # @return [nil] if not set (will not happen in final settings)
      #
      attr_reader :block_environment

      # @private
      def self.with_fallback(settings, fallback)
        new(
          arguments: settings.arguments || fallback.arguments,
          results: settings.results || fallback.results,
          block_arguments: settings.block_arguments || fallback.block_arguments,
          block_results: settings.block_results || fallback.block_results,
          block_environment: settings.block_environment || fallback.block_environment
        )
      end
    end

    ##
    # Create a wrapper around the given object.
    #
    # If you pass an optional block, a {Ractor::Wrapper::Configuration} object
    # will be yielded to it, allowing additional configuration before the wrapper
    # starts. In particular, per-method configuration must be set in this block.
    # Block-provided settings override keyword arguments.
    #
    # See {Configuration} for more information about the method communication
    # and block settings.
    #
    # @param object [Object] The non-shareable object to wrap.
    # @param use_current_ractor [boolean] If true, the wrapper is run in a
    #     thread in the current Ractor instead of spawning a new Ractor (the
    #     default behavior). This option can be used if the wrapped object
    #     cannot be moved or must run in the main Ractor. Can also be set via
    #     the configuration block.
    # @param name [String] A name for this wrapper. Used during logging. Can
    #     also be set via the configuration block. Defaults to the object_id.
    # @param threads [Integer] The number of worker threads to run.
    #     Defaults to 0, which causes the wrapper to run sequentially without
    #     spawning workers. Sized to the desired concurrency of independent
    #     calls; does not need to account for re-entrant block calls, since
    #     suspended methods do not occupy a worker thread while waiting for a
    #     block to complete. Can also be set via the configuration block.
    # @param arguments [:move,:copy] How to communicate method arguments by
    #     default. If not specified, defaults to `:copy`.
    # @param results [:move,:copy,:void] How to communicate method return
    #     values by default. If not specified, defaults to `:copy`.
    # @param block_arguments [:move,:copy] How to communicate block arguments
    #     by default. If not specified, defaults to `:copy`.
    # @param block_results [:move,:copy,:void] How to communicate block result
    #     values by default. If not specified, defaults to `:copy`.
    # @param block_environment [:caller,:wrapped] How to execute blocks, and
    #     what scope blocks have access to. If not specified, defaults to
    #     `:caller`.
    # @param enable_logging [boolean] Set to true to enable logging. Default
    #     is false. Can also be set via the configuration block.
    # @yield [config] An optional configuration block.
    # @yieldparam config [Ractor::Wrapper::Configuration]
    #
    def initialize(object,
                   use_current_ractor: false,
                   name: nil,
                   threads: 0,
                   arguments: nil,
                   results: nil,
                   block_arguments: nil,
                   block_results: nil,
                   block_environment: nil,
                   enable_logging: false)
      raise ::Ractor::MovedError, "cannot wrap a moved object" if ::Ractor::MovedObject === object

      config = Configuration.new
      config.name = name || object_id.to_s
      config.enable_logging = enable_logging
      config.threads = threads
      config.use_current_ractor = use_current_ractor
      config.configure_method(arguments: arguments,
                              results: results,
                              block_arguments: block_arguments,
                              block_results: block_results,
                              block_environment: block_environment)
      yield config if block_given?

      @name = config.name
      @enable_logging = config.enable_logging
      @threads = config.threads
      @method_settings = config.final_method_settings
      @stub = Stub.new(self)

      if config.use_current_ractor
        setup_local_server(object)
      else
        setup_isolated_server(object)
      end
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
    # Return the number of worker threads used by the wrapper.
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
      (method_name && @method_settings[method_name.to_sym]) || @method_settings[nil]
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
      transaction = make_transaction
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
      begin
        @port.send(message, move: settings.arguments == :move)
      rescue ::Ractor::ClosedError
        raise StoppedError, "Wrapper has stopped"
      end
      loop do
        reply_message = reply_port.receive
        case reply_message
        when FiberYieldMessage, BlockingYieldMessage
          handle_yield(reply_message, transaction, settings, method_name, &)
        when ReturnMessage
          maybe_log("Received result", method_name: method_name, transaction: transaction)
          return reply_message.value
        when ExceptionMessage
          maybe_log("Received exception", method_name: method_name, transaction: transaction)
          raise reply_message.exception
        end
      end
    ensure
      reply_port.close
    end

    ##
    # Request that the wrapper stop. All currently running calls will complete
    # before the wrapper actually terminates, including calls that are
    # suspended waiting for a re-entrant block to return. New calls submitted
    # after the stop request will fail with {StoppedError}.
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
    # Unlike `Thread#join` and `Ractor#join`, if a Wrapper crashes, the
    # exception generally does *not* get raised out of `Wrapper#join`. Instead,
    # it just returns self in the same way as normal termination.
    #
    # @return [self]
    #
    def join
      if @ractor
        @ractor.join
      else
        reply_port = ::Ractor::Port.new
        begin
          @port.send(JoinMessage.new(reply_port))
          reply_port.receive
        rescue ::Ractor::ClosedError
          # Assume the wrapper has stopped if the port is not sendable
        ensure
          reply_port.close
        end
      end
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
    # a Ractor::Wrapper::Error.
    #
    # @return [Object] The original wrapped object
    #
    def recover_object
      raise Error, "cannot recover an object from a local wrapper" unless @ractor
      begin
        @ractor.value
      rescue ::Ractor::Error => e
        raise ::Ractor::Wrapper::Error, e.message, cause: e
      end
    end

    #### private items below ####

    ##
    # @private
    # Message sent to initialize a server.
    #
    InitMessage = ::Data.define(:object, :stub)

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
    # Message sent from a server in response to a join request.
    #
    JoinReplyMessage = ::Data.define

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
    # Message sent from a server to request a yield block run, in the
    # blocking-fallback path. The server allocates a temporary reply_port and
    # blocks waiting for a response on it. Used when the wrapped object yields
    # from a context where Fiber.yield is not safe (e.g., inside a nested
    # fiber such as an Enumerator's generator, or in a spawned thread).
    #
    BlockingYieldMessage = ::Data.define(:args, :kwargs, :reply_port)

    ##
    # @private
    # Message sent from a server to request a yield block run, in the
    # fiber-suspend path. The server suspends its method-handling fiber and
    # is resumed when a FiberReturnMessage or FiberExceptionMessage tagged
    # with the same fiber_id arrives back on the server's main port.
    #
    FiberYieldMessage = ::Data.define(:args, :kwargs, :fiber_id)

    ##
    # @private
    # Message sent from a caller back to a server, carrying the result of a
    # block invoked via the fiber-suspend path. The fiber_id identifies which
    # suspended fiber on the server should be resumed with this value.
    #
    FiberReturnMessage = ::Data.define(:value, :fiber_id)

    ##
    # @private
    # Message sent from a caller back to a server, carrying an exception
    # raised by a block invoked via the fiber-suspend path. The fiber_id
    # identifies which suspended fiber on the server should be resumed and
    # have this exception raised inside it.
    #
    FiberExceptionMessage = ::Data.define(:exception, :fiber_id)

    private

    ##
    # Start a server in the current Ractor.
    # Passes the object directly to the server.
    #
    def setup_local_server(object)
      maybe_log("Starting local server")
      @ractor = nil
      @port = ::Ractor::Port.new
      freeze
      wrapper_id = object_id
      ::Thread.new do
        ::Thread.current.name = "ractor-wrapper:server:#{wrapper_id}"
        Server.run_local(object: object,
                         stub: @stub,
                         port: @port,
                         name: name,
                         enable_logging: enable_logging?,
                         threads: threads)
      end
    end

    ##
    # Start a server in an isolated Ractor.
    # This must send the object separately since it must be moved into the
    # server's Ractor.
    #
    def setup_isolated_server(object)
      maybe_log("Starting isolated server")
      @ractor = ::Ractor.new(name, enable_logging?, threads, name: "wrapper:#{name}") do |name, enable_logging, threads|
        Server.run_isolated(name: name,
                            enable_logging: enable_logging,
                            threads: threads)
      end
      @port = @ractor.default_port
      freeze
      init_message = InitMessage.new(object: object, stub: @stub)
      @port.send(init_message, move: true)
    end

    ##
    # Create a transaction ID, used for logging
    #
    def make_transaction
      ::Random.rand(7_958_661_109_946_400_884_391_936).to_s(36).rjust(16, "0").freeze
    end

    ##
    # Create the shareable object representing a block in a method call
    #
    def make_block_arg(settings, &)
      if !block_given?
        nil
      elsif settings.block_environment == :wrapped
        ::Ractor.shareable_proc(&)
      else
        :send_block_message
      end
    end

    ##
    # Handle a call to a block directed to run in the caller environment.
    # Dispatches the block result or exception based on which yield-message
    # variant arrived: a FiberYieldMessage routes the response back to the
    # server's main port (so the suspended fiber can be resumed), while a
    # BlockingYieldMessage routes it to the temporary reply_port the server
    # is blocked on.
    #
    def handle_yield(message, transaction, settings, method_name)
      maybe_log("Yielding to block", method_name: method_name, transaction: transaction)
      begin
        block_result = yield(*message.args, **message.kwargs)
        block_result = nil if settings.block_results == :void
        maybe_log("Sending block result", method_name: method_name, transaction: transaction)
        send_block_result(message, block_result, settings)
      rescue ::Exception => e # rubocop:disable Lint/RescueException
        maybe_log("Sending block exception", method_name: method_name, transaction: transaction)
        begin
          send_block_exception(message, e)
        rescue ::StandardError
          begin
            send_block_exception(message, ::StandardError.new(e.inspect))
          rescue ::StandardError
            maybe_log("Failure to send block reply", method_name: method_name, transaction: transaction)
          end
        end
      end
    end

    ##
    # Send a block return value to the appropriate destination based on the
    # yield-message variant.
    #
    def send_block_result(message, value, settings)
      case message
      when FiberYieldMessage
        @port.send(FiberReturnMessage.new(value: value, fiber_id: message.fiber_id),
                   move: settings.block_results == :move)
      when BlockingYieldMessage
        message.reply_port.send(ReturnMessage.new(value), move: settings.block_results == :move)
      end
    end

    ##
    # Send a block exception to the appropriate destination based on the
    # yield-message variant.
    #
    def send_block_exception(message, exception)
      case message
      when FiberYieldMessage
        @port.send(FiberExceptionMessage.new(exception: exception, fiber_id: message.fiber_id))
      when BlockingYieldMessage
        message.reply_port.send(ExceptionMessage.new(exception))
      end
    end

    ##
    # Prints out a log message
    #
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
    # @private
    #
    # Server is the backend implementation of a wrapper. It listens for method
    # call requests on a port, and calls the wrapped object in a controlled
    # environment.
    #
    # It can run:
    #
    # * Either hosted by an external Ractor or isolated in a dedicated Ractor
    # * Either sequentially or concurrently using worker threads.
    #
    class Server
      ##
      # @private
      #
      # Multi-queue work dispatcher for the threaded server. Routes new
      # `CallMessage`s through a shared queue (any worker may pick them up)
      # and routes fiber resumes (`FiberReturnMessage` / `FiberExceptionMessage`)
      # through per-worker queues so a suspended fiber is always resumed by
      # the same worker thread that started it (Ruby fibers cannot be resumed
      # from a different thread than their last resumer).
      #
      # All public methods are thread-safe. The internal mutex guards the
      # shared queue, all per-worker queues, the fiber→worker map, and the
      # closed/notified state. `dequeue` blocks on a single shared
      # `ConditionVariable`; producers `broadcast` rather than `signal` so
      # workers waiting on per-worker queues are not starved by
      # shared-queue activity.
      #
      class Dispatcher
        CLOSED = [:closed, nil].freeze
        TERMINATE = [:terminate, nil].freeze

        ##
        # @param num_workers [Integer] number of per-worker queues to allocate.
        #   Workers are addressed by integers in the range `[0, num_workers)`.
        #
        def initialize(num_workers)
          @mutex = ::Mutex.new
          @cond = ::ConditionVariable.new
          @shared_queue = []
          @worker_queues = ::Array.new(num_workers) { [] }
          @fiber_to_worker = {}
          @closed = false
          @crashed = false
          @closed_notified = ::Array.new(num_workers, false)
        end

        ##
        # Push a new `CallMessage` onto the shared queue.
        # @return [Boolean] `true` normally, `false` if `close` has been called.
        #
        def enqueue_call(message)
          @mutex.synchronize do
            return false if @closed
            @shared_queue.push(message)
            @cond.broadcast
            true
          end
        end

        ##
        # Push a fiber-resume message (`FiberReturnMessage` /
        # `FiberExceptionMessage`) onto the queue of the worker that owns the
        # fiber identified by `message.fiber_id`.
        # @return [Boolean] `true` if dispatched, `false` if the fiber_id is
        #   not registered (e.g. fiber already finished or was aborted).
        #
        def enqueue_fiber_resume(message)
          @mutex.synchronize do
            worker_num = @fiber_to_worker[message.fiber_id]
            return false unless worker_num
            @worker_queues[worker_num].push(message)
            @cond.broadcast
            true
          end
        end

        ##
        # Block until the worker has work. Priority order:
        #
        # 1. Per-worker queue (always — these are resumes for fibers this
        #    worker owns; they must be drained even after close).
        # 2. Shared queue, but only if `accept_calls` is true and the
        #    dispatcher is not yet closed.
        # 3. The `CLOSED` sentinel (`[:closed, nil]`), returned exactly once
        #    per worker, the first time the worker would otherwise have
        #    blocked after `close` was called. Used to wake the worker so it
        #    can transition to a draining state. Subsequent calls behave
        #    normally and may block again.
        #
        # If `crash_close` has been called, returns `TERMINATE`
        # (`[:terminate, nil]`) immediately whenever the per-worker queue is
        # empty — signaling the worker to exit even if it has pending fibers.
        # The per-worker queue is still drained first so any in-flight resumes
        # complete normally.
        #
        # @return [Array(Symbol, Object)] one of `[:resume, msg]`,
        #   `[:call, msg]`, `CLOSED`, or `TERMINATE`.
        #
        def dequeue(worker_num, accept_calls:)
          @mutex.synchronize do
            loop do
              if (msg = @worker_queues[worker_num].shift)
                return [:resume, msg]
              end
              return TERMINATE if @crashed
              if accept_calls && !@closed && (msg = @shared_queue.shift)
                return [:call, msg]
              end
              if @closed && !@closed_notified[worker_num]
                @closed_notified[worker_num] = true
                return CLOSED
              end
              @cond.wait(@mutex)
            end
          end
        end

        ##
        # Atomically associate `fiber_id` with `worker_num` so subsequent
        # `enqueue_fiber_resume` calls land on the right worker queue.
        #
        def register_fiber(fiber_id, worker_num)
          @mutex.synchronize { @fiber_to_worker[fiber_id] = worker_num }
        end

        ##
        # Remove the fiber→worker mapping. Idempotent.
        #
        def unregister_fiber(fiber_id)
          @mutex.synchronize { @fiber_to_worker.delete(fiber_id) }
        end

        ##
        # Mark closed and wake all blocked workers. Drains the shared queue
        # and returns its previous contents so the caller can refuse them
        # (with `StoppedError`) on behalf of their pending callers. Idempotent
        # — repeated calls return `[]`.
        # @return [Array] the messages that were in the shared queue.
        #
        def close
          @mutex.synchronize do
            return [] if @closed
            @closed = true
            drained = @shared_queue.dup
            @shared_queue.clear
            @cond.broadcast
            drained
          end
        end

        ##
        # Mark closed AND crashed: future `dequeue` calls return `TERMINATE`
        # whenever the per-worker queue is empty (rather than blocking),
        # signaling workers to exit immediately so their `ensure` blocks can
        # abort any pending fibers. Used on server crash, where no further
        # fiber-resume messages will arrive. Idempotent (sets crashed even if
        # already closed). Returns the drained shared queue.
        # @return [Array] the messages that were in the shared queue.
        #
        def crash_close
          @mutex.synchronize do
            already_closed = @closed
            @closed = true
            @crashed = true
            drained = already_closed ? [] : @shared_queue.dup
            @shared_queue.clear
            @cond.broadcast
            drained
          end
        end
      end

      ##
      # @private
      # Create and run a server hosted in the current Ractor
      #
      def self.run_local(object:, stub:, port:, name:, enable_logging: false, threads: 0)
        server = new(isolated: false, object:, stub:, port:, name:, enable_logging:, threads:)
        server.run
      end

      ##
      # @private
      # Create and run a server in an isolated Ractor
      #
      def self.run_isolated(name:, enable_logging: false, threads: 0)
        port = ::Ractor.current.default_port
        server = new(isolated: true, object: nil, stub: nil, port:, name:, enable_logging:, threads:)
        server.run
      end

      # @private
      def initialize(isolated:, object:, stub:, port:, name:, enable_logging:, threads:)
        @isolated = isolated
        @object = object
        @stub = stub
        @port = port
        @name = name
        @enable_logging = enable_logging
        @threads_requested = threads.positive? ? threads : false
        @join_requests = []
        # Sequential mode only: maps fiber_id (Integer) => Fiber for
        # method-handling fibers that have suspended (via Fiber.yield) waiting
        # for a block result. Used to route incoming
        # FiberReturnMessage/FiberExceptionMessage back to the right fiber.
        # Threaded mode tracks pending fibers per-worker (in `worker_loop`'s
        # local `pending` hash) and routes via `Dispatcher`.
        @pending_fibers = {}
      end

      ##
      # @private
      # Handle the server lifecycle.
      # Returns the wrapped object, so it can be recovered if the server is run
      # in a Ractor.
      #
      def run
        receive_remote_object if @isolated
        start_workers if @threads_requested
        main_loop
        stop_workers if @threads_requested
        cleanup
        @object
      rescue ::Exception => e # rubocop:disable Lint/RescueException
        @crash_exception = e
        @object
      ensure
        crash_cleanup if @crash_exception
      end

      private

      ##
      # Receive the moved remote object. Called if the server is run in a
      # separate Ractor.
      #
      def receive_remote_object
        maybe_log("Waiting for initialization")
        init_message = @port.receive
        @object = init_message.object
        @stub = init_message.stub
      end

      ##
      # Start the worker threads. Each thread picks up work via the
      # `Dispatcher`, which routes new `CallMessage`s through a shared queue
      # and routes fiber-resume messages to the specific worker that owns the
      # suspended fiber. Called only if worker threading is enabled.
      #
      def start_workers
        maybe_log("Spawning #{@threads_requested} worker threads")
        @dispatcher = Dispatcher.new(@threads_requested)
        @active_workers = {}
        (0...@threads_requested).each do |worker_num|
          @active_workers[worker_num] = ::Thread.new do
            ::Thread.current.name = "ractor-wrapper:#{@name}:worker:#{worker_num}"
            worker_loop(worker_num)
          end
        end
      end

      ##
      # This is the main loop, listening on the inbox and handling messages for
      # normal operation:
      #
      # *   If it receives a CallMessage, it either runs the method in a
      #     fiber (sequential mode) or hands it to the `Dispatcher`'s shared
      #     queue (threaded mode). In both modes the method body executes
      #     inside a `Fiber` so it can suspend (via `Fiber.yield`) when its
      #     caller-side block needs to make a re-entrant call back into this
      #     wrapper.
      # *   If it receives a FiberReturnMessage or FiberExceptionMessage, it
      #     resumes the suspended fiber. In sequential mode the fiber lives
      #     in `@pending_fibers` and is resumed inline. In threaded mode the
      #     `Dispatcher` routes the message to the per-worker queue of the
      #     fiber's owning worker.
      # *   If it receives a StopMessage, it exits the main loop and proceeds
      #     to the termination logic.
      # *   If it receives a JoinMessage, it adds it to the list of join ports
      #     to notify once the wrapper completes.
      # *   If it receives a WorkerStoppedMessage, that indicates a worker
      #     thread has unexpectedly stopped. We conclude something has gone
      #     wrong with a worker, and we bail, stopping the remaining workers
      #     and proceeding to termination logic.
      #
      def main_loop
        loop do
          maybe_log("Waiting for message in running phase")
          message = @port.receive
          case message
          when CallMessage then dispatch_call(message)
          when FiberReturnMessage, FiberExceptionMessage then dispatch_fiber_resume(message)
          when JoinMessage then @join_requests << message.reply_port
          when WorkerStoppedMessage
            maybe_log("Received unexpected WorkerStoppedMessage")
            @active_workers.delete(message.worker_num) if @threads_requested
            break
          when StopMessage
            maybe_log("Received stop")
            drain_pending_fibers unless @threads_requested
            break
          end
        end
      end

      ##
      # Dispatch a `CallMessage` received by the main loop. In sequential
      # mode the method is started inline as a new fiber; in threaded mode
      # it is handed to the dispatcher's shared queue for any worker to pick
      # up.
      #
      def dispatch_call(message)
        maybe_log("Received CallMessage", call_message: message)
        if @threads_requested
          @dispatcher.enqueue_call(message)
        else
          start_method_fiber(message)
        end
      end

      ##
      # Route a fiber-resume message (`FiberReturnMessage` /
      # `FiberExceptionMessage`) to its owning fiber. In sequential mode this
      # resumes the fiber inline; in threaded mode the dispatcher routes it to
      # the per-worker queue of the worker that started the fiber. Logs and
      # discards if the fiber is no longer registered (likely already finished
      # or aborted by a crashed worker).
      #
      def dispatch_fiber_resume(message)
        maybe_log("Routing fiber resume", fiber_id: message.fiber_id)
        if @threads_requested
          return if @dispatcher.enqueue_fiber_resume(message)
          maybe_log("Discarding orphan fiber resume", fiber_id: message.fiber_id)
        else
          resume_method_fiber(message)
        end
      end

      ##
      # Sequential-mode stopping phase. After receiving a StopMessage, continue
      # accepting fiber-result messages (and join requests) so that any
      # currently-suspended method-handling fiber can complete. New
      # `CallMessage`s are refused with `StoppedError`. Returns once
      # `@pending_fibers` is empty.
      #
      def drain_pending_fibers
        until @pending_fibers.empty?
          maybe_log("Waiting for pending fibers to complete")
          message = @port.receive
          case message
          when CallMessage
            refuse_method(message)
          when FiberReturnMessage, FiberExceptionMessage
            resume_method_fiber(message)
          when StopMessage
            maybe_log("Stop received when already stopping")
          when JoinMessage
            maybe_log("Received and queueing join request")
            @join_requests << message.reply_port
          else
            maybe_log("Unexpected message when draining pending fibers: #{message.class.name}")
          end
        end
      end

      ##
      # Spawn a fiber to handle a CallMessage in sequential mode. If the
      # method-handling fiber suspends via Fiber.yield (because its caller-side
      # block re-entered this wrapper), the fiber is registered in
      # `@pending_fibers` so that the matching block-return message can later
      # resume it.
      #
      def start_method_fiber(message)
        fiber = ::Fiber.new { handle_method(message) }
        fiber_id = fiber.object_id
        @pending_fibers[fiber_id] = fiber
        maybe_log("Starting method fiber", call_message: message, fiber_id: fiber_id)
        fiber.resume
        @pending_fibers.delete(fiber_id) unless fiber.alive?
      end

      ##
      # Resume a previously-suspended method-handling fiber, delivering the
      # block-result message as the return value of its Fiber.yield call.
      # Silently ignores unknown fiber_ids (the fiber may have been aborted).
      #
      def resume_method_fiber(message)
        fiber = @pending_fibers[message.fiber_id]
        return unless fiber
        maybe_log("Resuming method fiber", fiber_id: message.fiber_id)
        fiber.resume(message)
        @pending_fibers.delete(message.fiber_id) unless fiber.alive?
      end

      ##
      # This signals workers to stop by closing the dispatcher, and then
      # waits for all workers to report in that they have stopped. It is
      # called only if worker threading is enabled.
      #
      # Closing the dispatcher drains any never-dispatched `CallMessage`s
      # from its shared queue; those are refused immediately so the
      # corresponding callers do not block forever. The dispatcher also
      # delivers a one-shot closed signal to each worker so they can
      # transition to a draining state.
      #
      # Responds to messages to indicate the wrapper is stopping and no longer
      # accepting new method requests:
      #
      # *   If it receives a CallMessage, it sends back a refusal exception.
      # *   If it receives a FiberReturnMessage or FiberExceptionMessage, it
      #     forwards it to the dispatcher so the owning worker can resume its
      #     suspended fiber and complete the in-flight call.
      # *   If it receives a StopMessage, it does nothing (i.e. the stop
      #     operation is idempotent).
      # *   If it receives a JoinMessage, it adds it to the list of join ports
      #     to notify once the wrapper completes. At this point the wrapper is
      #     not yet considered complete because workers are still processing
      #     earlier method calls.
      # *   If it receives a WorkerStoppedMessage, it updates its count of
      #     running workers.
      #
      # This phase continues until all workers have signaled that they have
      # stopped.
      #
      def stop_workers
        drained = @dispatcher.close
        drained.each { |message| refuse_method(message) }
        until @active_workers.empty?
          maybe_log("Waiting for message in stopping phase")
          message = @port.receive
          case message
          when CallMessage
            refuse_method(message)
          when FiberReturnMessage, FiberExceptionMessage
            dispatch_fiber_resume(message)
          when WorkerStoppedMessage
            maybe_log("Acknowledged WorkerStoppedMessage: #{message.worker_num}")
            @active_workers.delete(message.worker_num)
          when StopMessage
            maybe_log("Stop received when already stopping")
          when JoinMessage
            maybe_log("Received and queueing join request")
            @join_requests << message.reply_port
          end
        end
      end

      ##
      # This is called when the Server is ready to terminate completely.
      # It closes the inbox and responds to any remaining contents.
      #
      def cleanup
        maybe_log("Closing inbox")
        @port.close
        maybe_log("Draining inbox")
        loop do
          message = begin
            @port.receive
          rescue ::Ractor::ClosedError
            maybe_log("Inbox is empty")
            nil
          end
          break if message.nil?
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
        maybe_log("Responding to join requests")
        @join_requests.each { |port| send_join_reply(port) }
      end

      ##
      # Called from the ensure block in run when an unexpected exception
      # terminated the server. Drains pending requests that are not otherwise
      # being handled, responding to all pending callers and join requesters,
      # and also joins any worker threads.
      #
      def crash_cleanup
        maybe_log("Running crash cleanup after: #{@crash_exception.message} (#{@crash_exception.class})")
        error = CrashedError.new("Server crashed: #{@crash_exception.message} (#{@crash_exception.class})")
        # `@dispatcher` should not be nil in threaded mode, but we're
        # checking anyway just in case a crash happened during setup
        drain_dispatcher_after_crash(@dispatcher, error) if @threads_requested && @dispatcher
        abort_pending_fibers(@pending_fibers, error) unless @threads_requested
        drain_inbox_after_crash(@port, error)
        # `@active_workers` should not be nil in threaded mode, but we're
        # checking anyway just in case a crash happened during setup
        join_workers_after_crash(@active_workers) if @threads_requested && @active_workers
        @join_requests.each { |port| send_join_reply(port) }
      rescue ::Exception => e # rubocop:disable Lint/RescueException
        maybe_log("Suppressed exception during crash_cleanup: #{e.message} (#{e.class})")
      end

      ##
      # After a crash, raises +error+ inside each suspended method-handling
      # fiber. The exception emerges from the fiber's `Fiber.yield` call and is
      # caught by `handle_method`'s rescue chain, which sends an
      # `ExceptionMessage` to the fiber's reply_port so the caller observes a
      # `CrashedError`.
      #
      def abort_pending_fibers(pending_fibers, error)
        pending_fibers.each_pair do |fiber_id, fiber|
          maybe_log("Aborting suspended fiber", fiber_id: fiber_id)
          fiber.raise(error)
        rescue ::Exception => e # rubocop:disable Lint/RescueException
          maybe_log("Suppressed exception during abort_pending_fibers: #{e.message} (#{e.class})",
                    fiber_id: fiber_id)
        end
        pending_fibers.clear
      end

      ##
      # Closes the dispatcher after a crash, then sends an error response
      # to the callers of any `CallMessage`s that were still queued in the
      # shared queue (and therefore had not yet been dispatched to a worker).
      # Workers themselves clean up their own in-flight fibers via their
      # ensure blocks.
      #
      def drain_dispatcher_after_crash(dispatcher, error)
        dispatcher.crash_close.each do |message|
          message.reply_port.send(ExceptionMessage.new(error))
        rescue ::Ractor::Error
          maybe_log("Failed to send crash error to queued caller", call_message: message)
        end
      rescue ::Exception => e # rubocop:disable Lint/RescueException
        maybe_log("Suppressed exception during drain_dispatcher_after_crash: " \
                  "#{e.message} (#{e.class})")
      end

      ##
      # Drains any remaining inbox messages after a crash, sending errors to
      # pending callers and responding to any join requests.
      #
      def drain_inbox_after_crash(port, error)
        begin
          port.close
        rescue ::Ractor::Error
          # Port was already closed (maybe because it was the cause of the crash)
        end
        loop do
          message = begin
            port.receive
          rescue ::Ractor::Error
            nil
          end
          break if message.nil?
          case message
          when CallMessage
            begin
              message.reply_port.send(ExceptionMessage.new(error))
            rescue ::Ractor::Error
              maybe_log("Failed to send crash error to caller", call_message: message)
            end
          when JoinMessage
            send_join_reply(message.reply_port)
          when WorkerStoppedMessage, StopMessage, FiberReturnMessage, FiberExceptionMessage
            # Ignore
          end
        end
      rescue ::Exception => e # rubocop:disable Lint/RescueException
        maybe_log("Suppressed exception during drain_inbox_after_crash: #{e.message} (#{e.class})")
      end

      ##
      # Wait until all workers have stopped after a crash
      #
      def join_workers_after_crash(workers)
        workers.each_value do |thread|
          thread.join
        rescue ::Exception => e # rubocop:disable Lint/RescueException
          maybe_log("Suppressed exception during join_workers_after_crash: #{e.message} (#{e.class})")
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
      def worker_loop(worker_num)
        maybe_log("Worker starting", worker_num: worker_num)
        pending = {}
        crash_exception = nil
        begin
          run_worker_dispatch_loop(worker_num, pending)
        rescue ::Exception => e # rubocop:disable Lint/RescueException
          crash_exception = e
          raise
        ensure
          cleanup_worker(worker_num, pending, crash_exception)
        end
      end

      ##
      # The dispatch loop body for a worker thread. Loops on
      # `@dispatcher.dequeue` and routes each result to the appropriate
      # handler. Exits when the dispatcher signals termination, or when a
      # graceful close has been observed and the local pending hash is empty.
      #
      def run_worker_dispatch_loop(worker_num, pending)
        stopping = false
        loop do
          maybe_log("Waiting for work", worker_num: worker_num)
          kind, message = @dispatcher.dequeue(worker_num, accept_calls: !stopping)
          case kind
          when :call then start_worker_fiber(message, pending, worker_num)
          when :resume then resume_worker_fiber(message, pending)
          when :closed then stopping = true
          when :terminate then return
          end
          return if stopping && pending.empty?
        end
      end

      ##
      # Worker-thread cleanup: aborts any pending fibers (so their callers
      # observe `CrashedError`) and reports the worker's stop to the main
      # loop. Always runs in the worker's ensure block — both for normal exit
      # and for crash exit.
      #
      def cleanup_worker(worker_num, pending, crash_exception = nil)
        maybe_log("Worker stopping", worker_num: worker_num)
        if pending && !pending.empty?
          message =
            if crash_exception
              "Worker #{worker_num} crashed: #{crash_exception.message} (#{crash_exception.class})"
            else
              "Worker #{worker_num} terminated"
            end
          error = CrashedError.new(message)
          pending.each_key { |fiber_id| @dispatcher.unregister_fiber(fiber_id) }
          abort_pending_fibers(pending, error)
        end
        begin
          @port.send(WorkerStoppedMessage.new(worker_num))
        rescue ::Ractor::ClosedError
          maybe_log("Worker unable to report stop, possibly due to server crash", worker_num: worker_num)
        end
      end

      ##
      # Start a fiber for a new `CallMessage` on this worker. Registers the
      # fiber with the dispatcher so future fiber-resume messages route here.
      # If the fiber completes synchronously (no `Fiber.yield`), it is
      # immediately removed from the local `pending` hash and unregistered.
      #
      def start_worker_fiber(message, pending, worker_num)
        fiber = ::Fiber.new { handle_method(message, worker_num: worker_num) }
        fiber_id = fiber.object_id
        pending[fiber_id] = fiber
        @dispatcher.register_fiber(fiber_id, worker_num)
        maybe_log("Starting worker fiber", call_message: message, worker_num: worker_num, fiber_id: fiber_id)
        fiber.resume
        return if fiber.alive?
        pending.delete(fiber_id)
        @dispatcher.unregister_fiber(fiber_id)
      end

      ##
      # Resume a previously-suspended fiber with a fiber-result message. If
      # the fiber is no longer alive (e.g. aborted), the message is silently
      # discarded. On completion the fiber is removed from `pending` and
      # unregistered from the dispatcher.
      #
      def resume_worker_fiber(message, pending)
        fiber = pending[message.fiber_id]
        return unless fiber
        maybe_log("Resuming worker fiber", fiber_id: message.fiber_id)
        fiber.resume(message)
        return if fiber.alive?
        pending.delete(message.fiber_id)
        @dispatcher.unregister_fiber(message.fiber_id)
      end

      ##
      # This is called to handle a method call request.
      # It calls the method on the wrapped object, and then sends back a
      # response to the caller. If an exception was raised, it sends back an
      # error response. It tries very hard always to send a response of some
      # kind; if an error occurs while constructing or sending a response, it
      # will catch the exception and try to send a simpler response. If a block
      # was passed to the method, it is also handled here.
      #
      def handle_method(message, worker_num: nil)
        block = make_block(message)
        maybe_log("Running method", worker_num: worker_num, call_message: message)
        result = @object.__send__(message.method_name, *message.args, **message.kwargs, &block)
        result = @stub if result.equal?(@object)
        result = nil if message.settings.results == :void
        maybe_log("Sending return value", worker_num: worker_num, call_message: message)
        message.reply_port.send(ReturnMessage.new(result), move: message.settings.results == :move)
      rescue ::Exception => e # rubocop:disable Lint/RescueException
        maybe_log("Sending exception", worker_num: worker_num, call_message: message)
        begin
          message.reply_port.send(ExceptionMessage.new(e))
        rescue ::Exception # rubocop:disable Lint/RescueException
          begin
            message.reply_port.send(ExceptionMessage.new(::RuntimeError.new(e.inspect)))
          rescue ::Exception # rubocop:disable Lint/RescueException
            maybe_log("Failure to send method response", worker_num: worker_num, call_message: message)
          end
        end
      end

      ##
      # Creates a block appropriate to the block specification received with
      # the method call message. This could return:
      #
      # *   nil if there was no block
      # *   the proc itself, if a shareable proc was received
      # *   otherwise a proc that sends a message back to the caller, along
      #     with the block arguments, to run the block in the caller's
      #     environment
      #
      # The returned proc uses the fiber-suspend path (Fiber.yield) so the
      # server can continue processing other messages, including re-entrant
      # calls from inside the block. In threaded mode, fiber resumes are
      # routed back to the same worker that started the fiber via the
      # `Dispatcher`'s per-worker queues (Ruby fibers cannot migrate between
      # threads).
      #
      # Hybrid fallback: if the proc is invoked from a different fiber than
      # the method-handling fiber (e.g. from inside an Enumerator generator
      # or a spawned fiber), the fiber path would call Fiber.yield on the
      # wrong fiber. In that case the proc falls back to the blocking path.
      #
      def make_block(message)
        return message.block_arg unless message.block_arg == :send_block_message
        expected_fiber = ::Fiber.current
        proc do |*args, **kwargs|
          args.map! { |arg| arg.equal?(@object) ? @stub : arg }
          kwargs.transform_values! { |arg| arg.equal?(@object) ? @stub : arg }
          if ::Fiber.current.equal?(expected_fiber)
            fiber_yield_block(message, args, kwargs)
          else
            blocking_yield_block(message, args, kwargs)
          end
        end
      end

      ##
      # Yield to a caller-side block via the fiber-suspend path. The current
      # fiber's id is sent in the FiberYieldMessage so the caller knows which
      # FiberReturnMessage/FiberExceptionMessage to send back. The fiber then
      # suspends; main_loop will resume it with the reply message when one
      # arrives on @port.
      #
      def fiber_yield_block(message, args, kwargs)
        fiber_id = ::Fiber.current.object_id
        yield_message = FiberYieldMessage.new(args: args, kwargs: kwargs, fiber_id: fiber_id)
        maybe_log("Yielding to caller-side block", call_message: message, fiber_id: fiber_id)
        message.reply_port.send(yield_message, move: message.settings.block_arguments == :move)
        reply = ::Fiber.yield
        maybe_log("Resumed after block reply", call_message: message, fiber_id: fiber_id)
        case reply
        when FiberExceptionMessage
          raise reply.exception
        when FiberReturnMessage
          reply.value
        end
      end

      ##
      # Yield to a caller-side block via the blocking-fallback path: allocate
      # a temporary reply_port and block waiting for a response on it. Used
      # when the fiber-suspend path is not available (nested-fiber and
      # spawned-thread invocations).
      #
      def blocking_yield_block(message, args, kwargs)
        reply_port = ::Ractor::Port.new
        reply_message = begin
          yield_message = BlockingYieldMessage.new(args: args, kwargs: kwargs, reply_port: reply_port)
          message.reply_port.send(yield_message, move: message.settings.block_arguments == :move)
          reply_port.receive
        ensure
          reply_port.close
        end
        case reply_message
        when ExceptionMessage
          raise reply_message.exception
        when ReturnMessage
          reply_message.value
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
          error = StoppedError.new("Wrapper is shutting down")
          message.reply_port.send(ExceptionMessage.new(error))
        rescue ::Ractor::Error
          maybe_log("Failed to send refusal message", call_message: message)
        end
      end

      ##
      # This attempts to send a signal that a wrapper join has completed.
      #
      def send_join_reply(port)
        port.send(JoinReplyMessage.new.freeze)
      rescue ::Ractor::ClosedError
        maybe_log("Join reply port is closed")
      end

      ##
      # Print out a log message
      #
      def maybe_log(str, call_message: nil, worker_num: nil, fiber_id: nil,
                    transaction: nil, method_name: nil)
        return unless @enable_logging
        transaction ||= call_message&.transaction
        method_name ||= call_message&.method_name
        metadata = [::Time.now.utc.strftime("%Y-%m-%dT%H:%M:%S.%L"), "Ractor::Wrapper:#{@name}"]
        metadata << "Worker:#{worker_num}" if worker_num
        metadata << "Fiber:#{fiber_id}" if fiber_id
        metadata << "Transaction:#{transaction}" if transaction
        metadata << "Method:#{method_name}" if method_name
        metadata = metadata.join(" ")
        $stderr.puts("[#{metadata}] #{str}")
        $stderr.flush
      rescue ::StandardError
        # Swallow any errors during logging
      end
    end
  end
end

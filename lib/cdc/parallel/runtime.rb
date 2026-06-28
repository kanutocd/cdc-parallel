# frozen_string_literal: true

module CDC
  module Parallel
    # High-level Ractor runtime facade for `cdc-core` processors.
    #
    # `Runtime` is the primary public entry point for applications that want to
    # execute normalized CDC work items with `cdc-parallel`. It wires together a
    # {ProcessorPool}, a {TransactionPool}, and a {Router} so callers can submit
    # either a single `CDC::Core::ChangeEvent` or a
    # `CDC::Core::TransactionEnvelope` through one object.
    #
    # Use this class when you want the default cdc-parallel behavior:
    #
    # * validate that the processor declared `ractor_safe!`
    # * boot a fixed set of worker Ractors
    # * route events and transaction envelopes to the right pool
    # * return `CDC::Core::ProcessorResult` objects
    # * shut down all worker resources together
    #
    # @example Processing a change event
    #   runtime = CDC::Parallel::Runtime.new(
    #     processor: AnalyticsProcessor.new,
    #     size: 4,
    #     timeout: 5
    #   )
    #
    #   result = runtime.process(change_event)
    #   result.success? #=> true
    #
    #   runtime.shutdown
    #
    # @example Processing a transaction envelope
    #   result = runtime.process_transaction(transaction)
    #
    # @note `Runtime` is an execution facade, not a source adapter. It expects
    #   work that has already been normalized into `cdc-core` primitives.
    # @see CDC::Parallel::ProcessorPool
    # @see CDC::Parallel::TransactionPool
    # @see CDC::Parallel::Router
    # @api public
    class Runtime
      # Create a runtime with event and transaction pools.
      #
      # @param processor [CDC::Core::Processor]
      #   Ractor-safe processor used for both event and transaction processing.
      # @param size [Integer]
      #   Number of worker Ractors per internal pool.
      # @param timeout [Numeric, nil]
      #   Optional timeout in seconds for result collection and shutdown waits.
      # @param supervision [Boolean]
      #   Whether worker Ractors should be respawned after unexpected death.
      # @param max_respawns [Integer]
      #   Maximum crash count inside `respawn_window` before a worker slot enters
      #   cooldown.
      # @param respawn_window [Numeric]
      #   Time window, in seconds, used by the crash-loop circuit breaker.
      # @param respawn_cooldown [Numeric]
      #   Cooldown, in seconds, before a crash-looping slot is revived again.
      # @raise [UnsafeProcessorError]
      #   Raised when the processor class has not declared `ractor_safe!`.
      # @raise [ArgumentError]
      #   Raised when size or timeout is invalid.
      # @return [void]
      def initialize(
        processor:,
        size: Etc.nprocessors,
        timeout: nil,
        supervision: true,
        max_respawns: 3,
        respawn_window: 60,
        respawn_cooldown: 5
      )
        @processor = processor
        @processor.start

        pool_options = build_pool_options(
          processor:, size:, timeout:, supervision:,
          max_respawns:, respawn_window:, respawn_cooldown:
        )

        @processor_pool = ProcessorPool.new(**pool_options)
        @transaction_pool = TransactionPool.new(**pool_options)
        @router = Router.new(processor_pool: @processor_pool, transaction_pool: @transaction_pool)
        @shutdown = false
      end

      # Process a supported normalized CDC work item.
      #
      # Supported items are `CDC::Core::ChangeEvent` and
      # `CDC::Core::TransactionEnvelope`. Unsupported objects raise
      # {UnsupportedWorkItemError} from the router.
      #
      # @param item [CDC::Core::ChangeEvent, CDC::Core::TransactionEnvelope]
      #   Normalized CDC work item.
      # @raise [ShutdownError]
      #   Raised when called after {#shutdown}.
      # @raise [UnsupportedWorkItemError]
      #   Raised for objects that are not supported CDC work item shapes.
      # @return [CDC::Core::ProcessorResult]
      def process(item)
        raise ShutdownError, "runtime has been shut down" if @shutdown

        @router.process(item)
      end

      # Process a transaction envelope.
      #
      # This method is a readability alias for transaction-oriented call sites.
      # It delegates to {#process}, so it has the same validation, shutdown, and
      # result behavior.
      #
      # @param transaction [CDC::Core::TransactionEnvelope]
      # @return [CDC::Core::ProcessorResult]
      def process_transaction(transaction)
        process(transaction)
      end

      # Shut down all runtime resources.
      #
      # Shutdown is idempotent and cascades to the internal event and transaction
      # pools. The processor's `flush` and `stop` lifecycle hooks are called once
      # after all pools have been drained. After shutdown, {#process} raises
      # {ShutdownError}.
      #
      # @return [void]
      def shutdown
        return if @shutdown

        @shutdown = true
        @processor_pool.shutdown
        @transaction_pool.shutdown
        @processor.flush
        @processor.stop
      end

      private

      # Build keyword arguments shared by the event and transaction pools.
      #
      # The runtime owns the processor lifecycle, so internal pools receive
      # `manage_lifecycle: false` to avoid duplicate `start`, `flush`, or `stop`
      # calls for the same processor instance.
      #
      # @param processor [CDC::Core::Processor]
      # @param size [Integer]
      # @param timeout [Numeric, nil]
      # @param supervision [Boolean]
      # @param max_respawns [Integer]
      # @param respawn_window [Numeric]
      # @param respawn_cooldown [Numeric]
      # @return [Hash]
      # @api private
      def build_pool_options(
        processor:,
        size:,
        timeout:,
        supervision:,
        max_respawns:,
        respawn_window:,
        respawn_cooldown:
      )
        {
          processor:,
          size:,
          timeout:,
          supervision:,
          max_respawns:,
          respawn_window:,
          respawn_cooldown:,
          manage_lifecycle: false
        }
      end
    end
  end
end

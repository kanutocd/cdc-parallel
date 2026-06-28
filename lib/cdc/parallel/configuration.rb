# frozen_string_literal: true

module CDC
  module Parallel
    # :nodoc:
    ConfigurationData = Data.define(:size, :timeout)
    private_constant :ConfigurationData

    # Immutable configuration shared by cdc-parallel runtime objects.
    #
    # `Configuration` validates worker sizing and timeout values at construction
    # time, freezes the resulting data object through `Data.define`, and makes
    # the instance shareable so it is safe to retain around Ractor-oriented
    # runtime objects.
    #
    # @example Default configuration
    #   config = CDC::Parallel::Configuration.new
    #   config.size    #=> Etc.nprocessors
    #   config.timeout #=> nil
    #
    # @example Explicit worker count and timeout
    #   config = CDC::Parallel::Configuration.new(size: 4, timeout: 5)
    #
    # @!attribute [r] size
    #   @return [Integer] Number of worker Ractors to boot.
    # @!attribute [r] timeout
    #   @return [Numeric, nil] Optional wait timeout in seconds.
    # @api public
    class Configuration < ConfigurationData
      # Create a validated runtime configuration.
      #
      # @param size [Integer]
      #   Worker count. Must be greater than zero.
      # @param timeout [Numeric, nil]
      #   Optional timeout in seconds. Must be greater than zero when provided.
      # @raise [ArgumentError]
      #   Raised when `size` or `timeout` is invalid.
      # @return [void]
      def initialize(size: Etc.nprocessors, timeout: nil)
        raise ArgumentError, "size must be an Integer" unless size.is_a?(Integer)
        raise ArgumentError, "size must be greater than zero" unless size.positive?
        raise ArgumentError, "timeout must be numeric" unless timeout.nil? || timeout.is_a?(Numeric)
        raise ArgumentError, "timeout must be greater than zero" if timeout && !timeout.positive?

        super
        ::Ractor.make_shareable(self)
      end
    end
  end
end

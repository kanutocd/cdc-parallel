# frozen_string_literal: true

module CDC
  module Parallel
    # Immutable configuration for Ractor runtimes.
    #
    # @!attribute size
    #   @return [Integer] worker count.
    # @!attribute timeout
    #   @return [Float, nil] optional wait timeout in seconds.
    class Configuration < Data.define(:size, :timeout)
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

# frozen_string_literal: true

module CDC
  module Parallel
    # Immutable configuration for Ractor runtimes.
    #
    # @!attribute size
    #   @return [Integer] worker count.
    # @!attribute timeout
    #   @return [Float, nil] optional wait timeout in seconds.
    Configuration = Data.define(:size, :timeout) do
      # @param size [Integer] worker count.
      # @param timeout [Float, nil] optional timeout.
      # @return [void]
      def initialize(size: Etc.nprocessors, timeout: nil)
        raise ArgumentError, "size must be an Integer" unless size.is_a?(Integer)
        raise ArgumentError, "size must be greater than zero" unless size.positive?

        super(size:, timeout:)
        ::Ractor.make_shareable(self)
      end
    end
  end
end

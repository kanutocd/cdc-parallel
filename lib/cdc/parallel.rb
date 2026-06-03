# frozen_string_literal: true

require "etc"
require "timeout"

require_relative "parallel/version"
require_relative "parallel/errors"
require_relative "parallel/configuration"
require_relative "parallel/result_collector"
require_relative "parallel/processor_pool"
require_relative "parallel/transaction_pool"
require_relative "parallel/router"
require_relative "parallel/runtime"

module CDC
  # Optional parallel Change Data Capture runtime for cdc-core processors.
  module Parallel
  end
end

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

# Namespace for Change Data Capture ecosystem components.
#
# The `cdc-parallel` gem adds the {CDC::Parallel} runtime namespace under
# this shared ecosystem root.
#
# @api public
module CDC
  # Optional parallel Change Data Capture runtime for `cdc-core` processors.
  #
  # `CDC::Parallel` is the CPU-bound execution layer of the CDC Ecosystem. It
  # consumes work items that have already been normalized into `cdc-core`
  # vocabulary objects and executes Ractor-safe processors across pre-warmed
  # worker Ractors.
  #
  # The namespace intentionally avoids `CDC::Ractor` so it does not collide with
  # Ruby's core `::Ractor` constant.
  #
  # ## Boundary
  #
  # `cdc-parallel` does not connect to PostgreSQL, parse `pgoutput`, decode
  # values, normalize source payloads, or persist sink data. Those concerns live
  # upstream in `pgoutput-*` libraries, source adapters, and downstream sinks.
  #
  # ## Main entry points
  #
  # * {CDC::Parallel::Runtime} - high-level facade for events and transactions
  # * {CDC::Parallel::ProcessorPool} - low-level Ractor worker pool
  # * {CDC::Parallel::TransactionPool} - transaction-envelope wrapper
  # * {CDC::Parallel::Router} - work item router
  #
  # @example
  #   runtime = CDC::Parallel::Runtime.new(
  #     processor: AnalyticsProcessor.new,
  #     size: 4
  #   )
  #
  #   result = runtime.process(change_event)
  #   runtime.shutdown
  #
  # @api public
  module Parallel
  end
end

# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/processors"
require_relative "../support/events"

class RouterTest < Minitest::Test
  include EventFixtures

  def test_rejects_unknown_item
    processor = SafeProcessor.new
    processor_pool = CDC::Parallel::ProcessorPool.new(processor:, size: 1)
    transaction_pool = CDC::Parallel::TransactionPool.new(processor:, size: 1)
    router = CDC::Parallel::Router.new(processor_pool:, transaction_pool:)

    assert_raises(CDC::Parallel::UnsupportedWorkItemError) { router.process(Object.new) }
  ensure
    processor_pool&.shutdown
    transaction_pool&.shutdown
  end
end

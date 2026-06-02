# frozen_string_literal: true

module EventFixtures
  def change_event
    CDC::Core::ChangeEvent.new(
      operation: :update,
      schema: "public",
      table: "users",
      old_values: { "name" => "Alice" },
      new_values: { "name" => "Bob" },
      transaction_id: 123
    )
  end

  def transaction
    CDC::Core::TransactionEnvelope.new(
      transaction_id: 123,
      events: [change_event],
      commit_lsn: "0/16B6C50",
      committed_at: Time.utc(2026, 5, 31)
    )
  end
end

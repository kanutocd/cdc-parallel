# frozen_string_literal: true

require_relative "../test_helper"

class ConfigurationTest < Minitest::Test
  def test_accepts_valid_configuration
    config = CDC::Parallel::Configuration.new(size: 2, timeout: 1.0)

    assert_equal 2, config.size
    assert_equal 1.0, config.timeout
    assert Ractor.shareable?(config)
  end

  def test_rejects_invalid_size
    assert_raises(ArgumentError) { CDC::Parallel::Configuration.new(size: 0) }
    assert_raises(ArgumentError) { CDC::Parallel::Configuration.new(size: "x") }
  end
end

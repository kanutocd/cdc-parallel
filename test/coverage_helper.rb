# frozen_string_literal: true

if ENV.fetch("COVERAGE", "false").to_s == "true"
  require "simplecov"

  SimpleCov.command_name("Minitest #{ENV.fetch('TEST_GROUP', 'all')}")

  SimpleCov.start do
    enable_coverage :branch
    add_filter "/test/"
    add_filter "/sig/"
  end
end

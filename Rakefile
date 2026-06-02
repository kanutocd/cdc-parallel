# frozen_string_literal: true

require "bundler/gem_tasks"
require "rubocop/rake_task"
require "yard"

RuboCop::RakeTask.new(:rubocop) do |task|
  task.options = ["--parallel"]
end

TEST_GROUPS = {
  unit: "test/unit/**/*_test.rb",
  integration: "test/integration/**/*_test.rb",
  behavior: "test/behavior/**/*_test.rb",
  performance: "test/performance/**/*_test.rb"
}.freeze

GROUPED_TESTS = %i[unit integration behavior].freeze

def run_test_files(pattern)
  test_files = Dir[pattern].sort
  abort "No test files matched #{pattern}" if test_files.empty?

  requires = test_files.map { |file| "require_relative #{file.inspect}" }.join("; ")

  sh [
    RbConfig.ruby,
    "-r./test/coverage_helper",
    "-Ilib:test",
    "-w",
    "-e",
    requires.inspect
  ].join(" ")
end

desc "Run unit, integration, and behavior tests"
task test: GROUPED_TESTS.map { |group| "test:#{group}" }

namespace :test do
  TEST_GROUPS.each do |name, pattern|
    desc "Run #{name} tests"
    task name do
      ENV["TEST_GROUP"] = name.to_s
      ENV["PERFORMANCE"] = "true" if name == :performance && !ENV.key?("PERFORMANCE")
      run_test_files(pattern)
    end
  end

  desc "Run all test groups, including performance tests"
  task all: TEST_GROUPS.keys.map { |group| "test:#{group}" }
end

# so both `bundle exec rake yard` and `bundle exec yard doc` fetch options from ./.yardopts
YARD::Rake::YardocTask.new(:yard)

task default: %i[test rubocop yard]

namespace :rbs do
  desc "Generate RBS signatures"
  task :gen do
    sh "bundle exec rbs prototype rb --out-dir=sig --base-dir=lib lib"
  end

  desc "Validate RBS signatures"
  task :validate do
    sh "bundle exec steep check"
  end
end

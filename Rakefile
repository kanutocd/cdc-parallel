# frozen_string_literal: true

require "bundler/gem_tasks"
require "rubocop/rake_task"
require "yard"

RuboCop::RakeTask.new(:rubocop) do |task|
  task.options = ["--cache", "false"]
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
task :test do
  if ENV.fetch("COVERAGE", "false").to_s == "true"
    ENV["TEST_GROUP"] = "all"
    run_test_files("test/{unit,integration,behavior}/**/*_test.rb")
  else
    GROUPED_TESTS.each { |group| Rake::Task["test:#{group}"].invoke }
  end
end

namespace :test do
  TEST_GROUPS.each do |name, pattern|
    desc "Run #{name} tests"
    task name do
      ENV["TEST_GROUP"] = name.to_s
      performance_tests = name == :performance && !ENV.key?("CDC_PARALLEL_PERFORMANCE_TESTS")
      ENV["CDC_PARALLEL_PERFORMANCE_TESTS"] = "1" if performance_tests
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
  desc "Remove all non-shimmed sig files"
  task :clean do
    sh "rm -rf ./sig/cdc_parallel.rbs ./sig/cdc"
  end

  desc "Generate RBS signatures"
  task :generate do
    sh "bundle exec rbs prototype rb --out-dir=sig --base-dir=lib lib"
  end

  desc "Validate RBS signatures"
  task :validate do
    sh "bundle exec steep check"
  end
end

namespace :benchmark do
  desc "Run the processor pool benchmark locally"
  task :processor_pool do
    sh "bundle exec ruby benchmark/processor_pool_benchmark.rb"
  end

  desc "Build the reusable benchmark Docker image"
  task :docker_build do
    sh "docker build -f docker/benchmark/Dockerfile -t cdc-parallel-benchmark ."
  end

  desc "Run the benchmark Docker image"
  task :docker_run do
    sh "docker run --rm cdc-parallel-benchmark"
  end
end

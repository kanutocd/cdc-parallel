# frozen_string_literal: true

require_relative "lib/cdc/parallel/version"

Gem::Specification.new do |spec|
  spec.name = "cdc-parallel"
  spec.version = CDC::Parallel::VERSION
  spec.authors = ["Ken C. Demanawa"]
  spec.email = ["kenneth.c.demanawa@gmail.com"]
  spec.summary = "Optional parallel Change Data Capture (CDC) runtime for cdc-core."
  spec.description = <<~TEXT
    cdc-parallel provides optional Ractor-backed parallel execution for
    cdc-core. It accelerates PostgreSQL Change Data Capture (CDC) event
    processing while preserving the cdc-core programming model.
  TEXT
  spec.homepage = "https://kanutocd.github.io/cdc-parallel/"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 4.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/kanutocd/cdc-parallel"
  spec.metadata["changelog_uri"] = "#{spec.metadata["source_code_uri"]}/blob/main/CHANGELOG.md"
  spec.metadata["documentation_uri"] = spec.homepage
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir[
    "lib/**/*.rb",
    "sig/**/*.rbs",
    "examples/**/*.rb",
    "README.md",
    "CHANGELOG.md",
    "LICENSE.txt"
  ]

  spec.add_dependency "cdc-core", "~> 0.1"
  spec.add_dependency "ractor-pool", "~> 0.4.0"
end

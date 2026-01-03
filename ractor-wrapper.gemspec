# frozen_string_literal: true

lib = ::File.expand_path("lib", __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "ractor/wrapper/version"

::Gem::Specification.new do |spec|
  spec.name = "ractor-wrapper"
  spec.version = ::Ractor::Wrapper::VERSION
  spec.authors = ["Daniel Azuma"]
  spec.email = ["dazuma@gmail.com"]

  spec.summary = "A Ractor wrapper for a non-shareable object."
  spec.description =
    "An experimental class that wraps a non-shareable object in a Ractor," \
    " allowing multiple client Ractors to access it concurrently."
  spec.license = "MIT"
  spec.homepage = "https://github.com/dazuma/ractor-wrapper"

  spec.files = ::Dir.glob("lib/**/*.rb") + ::Dir.glob("*.md") + [".yardopts"]
  spec.require_paths = ["lib"]

  spec.required_ruby_version = ">= 4.0"

  spec.metadata["bug_tracker_uri"] = "https://github.com/dazuma/ractor-wrapper/issues"
  spec.metadata["changelog_uri"] = "https://rubydoc.info/gems/ractor-wrapper/#{::Ractor::Wrapper::VERSION}/file/CHANGELOG.md"
  spec.metadata["documentation_uri"] = "https://rubydoc.info/gems/ractor-wrapper/#{::Ractor::Wrapper::VERSION}"
  spec.metadata["homepage_uri"] = "https://github.com/dazuma/ractor-wrapper"
end

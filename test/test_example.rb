# frozen_string_literal: true

require "helper"

describe ::Ractor::Wrapper do
  let(:ruby_block_finder) { /\n```ruby\n((?:(?:[^`][^\n]*)?\n)+)```\n/m }

  it "runs the Net::HTTP README example" do
    readme_content = ::File.read(::File.join(::File.dirname(__dir__), "README.md"))
    script = readme_content.scan(ruby_block_finder)[0][0]
    script = "#{script}\nresponse\n"
    response = eval(script) # rubocop:disable Security/Eval
    assert_kind_of(Net::HTTPOK, response)
  end

  it "runs the SQLite3 README example" do
    $my_database_path = File.join(__dir__, "data", "numbers.db")
    readme_content = ::File.read(::File.join(::File.dirname(__dir__), "README.md"))
    script = readme_content.scan(ruby_block_finder)[1][0]
    script = "#{script}\nrows\n"
    rows = eval(script) # rubocop:disable Security/Eval
    assert_equal([["one", 1], ["two", 2]], rows)
  end
end

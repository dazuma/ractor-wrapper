# frozen_string_literal: true

require "helper"

describe ::Ractor::Wrapper do
  let(:ruby_block_finder) { /\n```ruby\n#([^\n]+)\n((?:(?:[^`][^\n]*)?\n)+)```\n/m }
  let(:readme_path) { ::File.join(::File.dirname(__dir__), "README.md") }
  let(:readme_content) { ::File.read(readme_path) }

  def eval_script(name, expr)
    script = readme_content.scan(ruby_block_finder).find { |entry| entry[0].strip == name }&.[](1)
    flunk("Unable to find example: #{name.inspect}") unless script
    final_script = "#{script}\n#{expr}\n"
    eval(final_script) # rubocop:disable Security/Eval
  end

  it "runs the Net::HTTP README example" do
    response = eval_script("Net::HTTP example", "response")
    assert_kind_of(Net::HTTPOK, response)
  end

  it "runs the SQLite3 README example" do
    $my_database_path = File.join(__dir__, "data", "numbers.db")
    rows = eval_script("SQLite3 example", "rows")
    assert_equal([["one", 1], ["two", 2]], rows)
  end

  it "runs the actor example" do
    sum = eval_script("Simple actor example", "sum")
    assert_equal(5, sum)
  end
end

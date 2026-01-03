# frozen_string_literal: true

require "helper"

describe ::Ractor::Wrapper do
  it "runs the README example" do
    # Faraday cannot be run outside the main Ractor
    skip
    readme_content = ::File.read(::File.join(::File.dirname(__dir__), "README.md"))
    script = /\n```ruby\n(.*\n)```\n/m.match(readme_content)[1]
    eval(script) # rubocop:disable Security/Eval

    require "net/http"
    connection = Faraday.new("http://example.com")
    wrapper = Ractor::Wrapper.new(connection)
    begin
      wrapper.stub.get("/hello")
    ensure
      wrapper.async_stop
    end
  end

  it "wraps a SQLite3 database" do
    # SQLite3::Database is not movable
    skip
    require "sqlite3"
    path = File.join(__dir__, "data", "numbers.db")
    db = SQLite3::Database.new(path)
    wrapper = Ractor::Wrapper.new(db)
    begin
      rows = wrapper.stub.execute("select * from numbers")
      assert_equal([["one", 1], ["two", 2]], rows)
    ensure
      wrapper.async_stop
    end
  end
end

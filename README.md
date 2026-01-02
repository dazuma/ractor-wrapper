# Ractor::Wrapper

`Ractor::Wrapper` is an experimental class that wraps a non-shareable object,
allowing multiple Ractors to access it concurrently. This can make it possible
for Ractors to share a "plain" object such as a database connection.

**WARNING:** This is a highly experimental library, and currently _not_
recommended for production use. (As of Ruby 4.0.0, the same can be said of
Ractors in general.)

## Quick start

Install ractor-wrapper as a gem, or include it in your bundle.

    gem install ractor-wrapper

Require it in your code:

    require "ractor/wrapper"

You can then create wrappers for objects. See the example below.

`Ractor::Wrapper` requires Ruby 4.0.0 or later.

## About Ractor::Wrapper

Ractors for the most part cannot access objects concurrently with other Ractors
unless the object is _shareable_, which generally means deeply immutable along
with a few other restrictions. If multiple Ractors need to interact with a
shared resource that is stateful or otherwise not shareable that resource must
itself be implemented and accessed as a Ractor.

`Ractor::Wrapper` makes it possible for such a shared resource to be
implemented as an object and accessed using ordinary method calls. It does this
by "wrapping" the object in a Ractor, and mapping method calls to message
passing. This may make it easier to implement such a resource with a simple
class rather than a full-blown Ractor with message passing, and it may also be
useful for adapting existing object-based resources.

Given a shared resource object, `Ractor::Wrapper` starts a new Ractor and
"runs" the object within that Ractor. It provides you with a stub object on
which you can invoke methods. The wrapper responds to these method calls by
sending messages to the internal Ractor, which invokes the shared object and
then sends back the result. If the underlying object is thread-safe, you can
configure the wrapper to run multiple threads that can run methods
concurrently. Or, if not, the wrapper can serialize requests to the object.

### Example usage

The following example shows how to share a single `Faraday::Conection`
object among multiple Ractors. Because `Faraday::Connection` is not itself
thread-safe, this example serializes all calls to it.

```ruby
require "ractor/wrapper"
require "faraday"

# Create a Faraday connection. Faraday connections are not shareable,
# so normally only one Ractor can access them at a time.
connection = Faraday.new("http://example.com")

# Create a wrapper around the connection. This starts up an internal
# Ractor and "moves" the connection object to that Ractor.
wrapper = Ractor::Wrapper.new(connection)

# At this point, the connection object can no longer be accessed
# directly because it is now owned by the wrapper's internal Ractor.
#     connection.get("/whoops")  # <= raises an error

# However, you can access the connection via the stub object provided
# by the wrapper. This stub proxies the call to the wrapper's internal
# Ractor. And it's shareable, so any number of Ractors can use it.
wrapper.stub.get("/hello")

# Here, we start two Ractors, and pass the stub to each one. Each
# Ractor can simply call methods on the stub as if it were the original
# connection object. (Internally, of course, the calls are proxied back
# to the wrapper.) By default, all calls are serialized. However, if
# you know that the underlying object is thread-safe, you can configure
# a wrapper to run calls concurrently.
r1 = Ractor.new(wrapper.stub) do |conn|
  10.times do
    conn.get("/hello")
  end
  :ok
end
r2 = Ractor.new(wrapper.stub) do |conn|
  10.times do
    conn.get("/ruby")
  end
  :ok
end

# Wait for the two above Ractors to finish.
r1.join
r2.join

# After you stop the wrapper, you can retrieve the underlying
# connection object and access it directly again.
wrapper.async_stop
connection = wrapper.recover_object
connection.get("/finally")
```

### Features

*   Provides a method interface to an object running in its own Ractor.
*   Supports arbitrary method arguments and return values.
*   Can be configured per method whether to copy or move arguments and
    return values.
*   Blocks can be run in the calling Ractor or in the object Ractor.
*   Raises exceptions thrown by the method.
*   Can serialize method calls for non-thread-safe objects, or run methods
    concurrently in multiple worker threads for thread-safe objects.
*   Can gracefully shut down the wrapper and retrieve the original object.

### Caveats

Ractor::Wrapper is subject to some limitations (and bugs) of Ractors, as of
Ruby 4.0.0.

*   You can run blocks in the object's Ractor only if the block does not
    access any data outside the block. Otherwise, the block must be run in
    the calling Ractor.
*   Certain types cannot be used as method arguments or return values
    because Ractor does not allow them to be moved between Ractors. These
    include threads, backtraces, and a few others.
*   Any exceptions raised are always copied back to the calling Ractor, and
    the backtrace is cleared out. This is due to
    https://bugs.ruby-lang.org/issues/21818

## Contributing

Development is done in GitHub at https://github.com/dazuma/ractor-wrapper.

*   To file issues: https://github.com/dazuma/ractor-wrapper/issues.
*   For questions and discussion, please do not file an issue. Instead, use the
    discussions feature: https://github.com/dazuma/ractor-wrapper/discussions.
*   Pull requests are welcome, but the library is highly experimental at this
    stage, and I recommend discussing features or design changes first before
    implementing.

The library uses [toys](https://dazuma.github.io/toys) for testing and CI. To
run the test suite, `gem install toys` and then run `toys ci`. You can also run
unit tests, rubocop, and builds independently.

## License

Copyright 2021-2026 Daniel Azuma

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

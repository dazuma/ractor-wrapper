# Ractor::Wrapper

`Ractor::Wrapper` is an experimental class that wraps a non-shareable object,
allowing multiple Ractors to access it concurrently. This can make it possible
for multiple ractors to share an object such as a database connection.

## Quick start

Install ractor-wrapper as a gem, or include it in your bundle.

    gem install ractor-wrapper

Require it in your code:

    require "ractor/wrapper"

You can then create wrappers for objects. See the example below.

`Ractor::Wrapper` requires Ruby 3.0.0 or later.

WARNING: This is a highly experimental library, and currently _not_ recommended
for production use. (As of Ruby 3.0.0, the same can be said of Ractors in
general.)

## About Ractor::Wrapper

Ractors for the most part cannot access objects concurrently with other
Ractors unless the object is _shareable_ (that is, deeply immutable along
with a few other restrictions.) If multiple Ractors need to interact with a
shared resource that is stateful or otherwise not Ractor-shareable, that
resource must itself be implemented and accessed as a Ractor.

`Ractor::Wrapper` makes it possible for such a shared resource to be
implemented as an ordinary object and accessed using ordinary method calls. It
does this by "wrapping" the object in a Ractor, and mapping method calls to
message passing. This may make it easier to implement such a resource with
a simple class rather than a full-blown Ractor with message passing, and it
may also useful for adapting existing legacy object-based implementations.

Given a shared resource object, `Ractor::Wrapper` starts a new Ractor and
"runs" the object within that Ractor. It provides you with a stub object
on which you can invoke methods. The wrapper responds to these method calls
by sending messages to the internal Ractor, which invokes the shared object
and then sends back the result. If the underlying object is thread-safe,
you can configure the wrapper to run multiple threads that can run methods
concurrently. Or, if not, the wrapper can serialize requests to the object.

### Example usage

The following example shows how to share a single `Faraday::Conection`
object among multiple Ractors. Because `Faraday::Connection` is not itself
thread-safe, this example serializes all calls to it.

```ruby
require "faraday"
require "ractor/wrapper"

# Create a Faraday connection and a wrapper for it.
connection = Faraday.new "http://example.com"
wrapper = Ractor::Wrapper.new(connection)

# At this point, the connection ojbect cannot be accessed directly
# because it has been "moved" to the wrapper's internal Ractor.
#     connection.get("/whoops")  # <= raises an error

# However, any number of Ractors can now access it through the wrapper.
# By default, access to the object is serialized; methods will not be
# invoked concurrently. (To allow concurrent access, set up threads when
# creating the wrapper.)
r1 = Ractor.new(wrapper) do |w|
  10.times do
    w.stub.get("/hello")
  end
  :ok
end
r2 = Ractor.new(wrapper) do |w|
  10.times do
    w.stub.get("/ruby")
  end
  :ok
end

# Wait for the two above Ractors to finish.
r1.take
r2.take

# After you stop the wrapper, you can retrieve the underlying
# connection object and access it directly again.
wrapper.async_stop
connection = wrapper.recover_object
connection.get("/finally")
```

### Features

*   Provides a method interface to an object running in a different Ractor.
*   Supports arbitrary method arguments and return values.
*   Supports exceptions thrown by the method.
*   Can be configured to copy or move arguments, return values, and
    exceptions, per method.
*   Can serialize method calls for non-concurrency-safe objects, or run
    methods concurrently in multiple worker threads for thread-safe objects.
*   Can gracefully shut down the wrapper and retrieve the original object.

### Caveats

Ractor::Wrapper is subject to some limitations (and bugs) of Ractors, as of
Ruby 3.0.0.

*   You cannot pass blocks to wrapped methods.
*   Certain types cannot be used as method arguments or return values
    because Ractor does not allow them to be moved between Ractors. These
    include threads, procs, backtraces, and a few others.
*   You can call wrapper methods from multiple Ractors concurrently, but
    you cannot call them from multiple Threads within a single Ractor.
    (This is due to https://bugs.ruby-lang.org/issues/17624)
*   If you close the incoming port on a Ractor, it will no longer be able
    to call out via a wrapper. If you close its incoming port while a call
    is currently pending, that call may hang. (This is due to
    https://bugs.ruby-lang.org/issues/17617)

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

Copyright 2021 Daniel Azuma

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

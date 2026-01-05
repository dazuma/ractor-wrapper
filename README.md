# Ractor::Wrapper

Ractor::Wrapper is an experimental class that wraps a non-shareable object in
an actor, allowing multiple Ractors to access it concurrently.

**WARNING:** This is a highly experimental library, and currently _not_
recommended for production use. (As of Ruby 4.0.0, the same can be said of
Ractors in general.)

## Quick start

Install ractor-wrapper as a gem, or include it in your bundle.

    gem install ractor-wrapper

Require it in your code:

    require "ractor/wrapper"

You can then create wrappers for objects. See the example below.

Ractor::Wrapper requires Ruby 4.0.0 or later.

## What is Ractor::Wrapper?

For the most part, unless an object is _sharable_, which generally means
deeply immutable along with a few other restrictions, it cannot be accessed
directly from another Ractor. This makes it difficult for multiple Ractors
to share a resource that is stateful. Such a resource must typically itself
be implemented as a Ractor and accessed via message passing.

Ractor::Wrapper makes it possible for an ordinary non-shareable object to
be accessed from multiple Ractors. It does this by "wrapping" the object
with an actor that listens for messages and invokes the object's methods in
a controlled single-Ractor environment. It then provides a stub object that
reproduces the interface of the original object, but responds to method
calls by sending messages to the wrapper. Ractor::Wrapper can be used to
implement simple actors by writing "plain" Ruby objects, or to adapt
existing non-shareable objects to a multi-Ractor world.

### Net::HTTP example

The following example shows how to share a single Net::HTTP session object
among multiple Ractors.

```ruby
require "ractor/wrapper"
require "net/http"

# Create a Net::HTTP session. Net::HTTP sessions are not shareable,
# so normally only one Ractor can access them at a time.
http = Net::HTTP.new("example.com")
http.start

# Create a wrapper around the session. This moves the session into an
# internal Ractor and listens for method call requests. By default, a
# wrapper serializes calls, handling one at a time, for compatibility
# with non-thread-safe objects.
wrapper = Ractor::Wrapper.new(http)

# At this point, the session object can no longer be accessed directly
# because it is now owned by the wrapper's internal Ractor.
#     http.get("/whoops")  # <= raises Ractor::MovedError

# However, you can access the session via the stub object provided by
# the wrapper. This stub proxies the call to the wrapper's internal
# Ractor. And it's shareable, so any number of Ractors can use it.
response = wrapper.stub.get("/")

# Here, we start two Ractors, and pass the stub to each one. Each
# Ractor can simply call methods on the stub as if it were the original
# connection object. Internally, of course, the calls are proxied to
# the original object via the wrapper, and execution is serialized.
r1 = Ractor.new(wrapper.stub) do |stub|
  5.times do
    stub.get("/hello")
  end
  :ok
end
r2 = Ractor.new(wrapper.stub) do |stub|
  5.times do
    stub.get("/ruby")
  end
  :ok
end

# Wait for the two above Ractors to finish.
r1.join
r2.join

# After you stop the wrapper, you can retrieve the underlying session
# object and access it directly again.
wrapper.async_stop
http = wrapper.recover_object
http.finish
```

### SQLite3 example

The following example shows how to share a SQLite3 database among multiple
Ractors.

```ruby
require "ractor/wrapper"
require "sqlite3"

# Create a SQLite3 database. These objects are not shareable, so
# normally only one Ractor can access them.
db = SQLite3::Database.new($my_database_path)

# Create a wrapper around the database. A SQLite3::Database object
# cannot be moved between Ractors, so we configure the wrapper to run
# in the current Ractor. You can also configure it to run multiple
# worker threads because the database object itself is thread-safe.
wrapper = Ractor::Wrapper.new(db, use_current_ractor: true, threads: 2)

# At this point, the database object can still be accessed directly
# because it hasn't been moved to a different Ractor.
rows = db.execute("select * from numbers")

# You can also access the database via the stub object provided by the
# wrapper.
rows = wrapper.stub.execute("select * from numbers")

# Here, we start two Ractors, and pass the stub to each one. The
# wrapper's two worker threads will handle the requests in the order
# received.
r1 = Ractor.new(wrapper.stub) do |stub|
  5.times do
    stub.execute("select * from numbers")
  end
  :ok
end
r2 = Ractor.new(wrapper.stub) do |stub|
  5.times do
    stub.execute("select * from numbers")
  end
  :ok
end

# Wait for the two above Ractors to finish.
r1.join
r2.join

# After stopping the wrapper, you can call the join method to wait for
# it to completely finish.
wrapper.async_stop
wrapper.join

# When running a wrapper with :use_current_ractor, you do not need to
# recover the object, because it was never moved. The recover_object
# method is not available.
#     db2 = wrapper.recover_object  # <= raises Ractor::Error
```

### Features

*   Provides a Ractor-shareable method interface to a non-shareable object.
*   Supports arbitrary method arguments and return values.
*   Can be configured to run in its own isolated Ractor or in a Thread in
    the current Ractor.
*   Can be configured per method whether to copy or move arguments and
    return values.
*   Blocks can be run in the calling Ractor or in the object Ractor.
*   Raises exceptions thrown by the method.
*   Can serialize method calls for non-thread-safe objects, or run methods
    concurrently in multiple worker threads for thread-safe objects.
*   Can gracefully shut down the wrapper and retrieve the original object.

### Caveats

*   Certain types cannot be used as method arguments or return values
    because they cannot be moved between Ractors. As of Ruby 4.0.0, these
    include threads, backtraces, procs, and a few others.
*   As of Ruby 4.0.0, any exceptions raised are always copied (rather than
    moved) back to the calling Ractor, and the backtrace is cleared out.
    This is due to https://bugs.ruby-lang.org/issues/21818
*   Blocks can be run "in place" (i.e. in the wrapped object context) only
    if the block does not access any data outside the block. Otherwise, the
    block must be run in caller's context.
*   Blocks configured to run in the caller's context can only be run while
    a method is executing. They cannot be "saved" as a proc to be run
    later unless they are configured to run "in place". In particular,
    using blocks as a syntax to define callbacks can generally not be done
    through a wrapper.

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
unit tests, rubocop, and build tests independently.

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

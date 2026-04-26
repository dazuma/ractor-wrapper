# Ractor::Wrapper

`Ractor::Wrapper` is an experimental class that wraps a non-shareable object in
an actor, allowing multiple Ractors to access it concurrently.

**WARNING:** This is an experimental library, and currently _not_ recommended
for production use. (As of Ruby 4.0, the same can still be said of Ractors in
general.)

## Quick start

Install ractor-wrapper as a gem, or include it in your bundle.

```sh
gem install ractor-wrapper
```

Require it in your code:

```ruby
require "ractor/wrapper"
```

You can then create wrappers for objects. See the example below.

`Ractor::Wrapper` requires Ruby 4.0.0 or later.

## What is `Ractor::Wrapper`?

For the most part, unless an object is _shareable_, which generally means
deeply immutable along with a few other restrictions, it cannot be accessed
directly from a Ractor other than the one in which it was constructed. This
makes it difficult for multiple Ractors to share a resource that is stateful,
such as a database connection.

    +----Main-Ractor----+       +-Another-Ractor-+
    |                   |       |                |
    |      client1      |       |                |
    |         |         |       |                |
    |         | ok      |       |                |
    |         v         |       |                |
    |     my_db_conn <------X------ client2      |
    |                   | fails |                |
    +-------------------+       +----------------+

`Ractor::Wrapper` makes it possible for an ordinary non-shareable object to
be accessed from multiple Ractors. It does this by "wrapping" the object with
a shareable proxy.

    +--Main-Ractor--+    +-Wrapper-Ractor-+    +-Another-Ractor-+
    |               |    |                |    |                |
    |    client1    |    |                |    |    client2     |
    |       |       |    |                |    |       |        |
    |       v       |    |                |    |       v        |
    |     +----------------------------------------------+      |
    |     |             SHAREABLE    WRAPPER             |      |
    |     +----------------------------------------------+      |
    |               |    |        |       |    |                |
    |               |    |        v       |    |                |
    |               |    |   my_db_conn   |    |                |
    +---------------+    +----------------+    +----------------+

The wrapper provides a shareable stub object that reproduces the method
interface of the original object, so, with a few caveats, the wrapper is almost
fully transparent. Behind the scenes, the wrapper "runs" the wrapped object in
a controlled single-Ractor environment, and uses port messaging to communicate
method calls, arguments, and return values between Ractors.

`Ractor::Wrapper` can be used to adapt non-shareable objects to a multi-Ractor
world. It can also be used to implement a simple actor by writing a "plain"
Ruby object and wrapping it with a Ractor.

## Examples

Below are some illustrative examples showing how to use `Ractor::Wrapper`.

### Net::HTTP example

The following example shows how to share a single Net::HTTP session object
among multiple Ractors.

```ruby
# Net::HTTP example

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
# SQLite3 example

require "ractor/wrapper"
require "sqlite3"

# Create a SQLite3 database. These objects are not shareable, so
# normally only one Ractor can access them.
db = SQLite3::Database.new($my_database_path)

# Create a wrapper around the database. A SQLite3::Database object
# cannot be moved between Ractors, so we configure the wrapper to run
# in the current Ractor instead of an internal Ractor. We can also
# configure it to run multiple worker threads because the database
# object itself is thread-safe.
wrapper = Ractor::Wrapper.new(db, use_current_ractor: true, threads: 2)

# At this point, the database object can still be accessed directly
# from the current Ractor because it hasn't been moved.
rows = db.execute("select * from numbers")

# You can also access the database via the stub object provided by the
# wrapper.
rows = wrapper.stub.execute("select * from numbers")

# Here, we start two Ractors, and pass the stub to each one. The
# wrapper's worker threads will handle the requests concurrently.
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
#     db2 = wrapper.recover_object  # <= raises Ractor::Wrapper::Error
```

### Simple actor example

The following example demonstrates how to use `Ractor::Wrapper` to implement an
actor as a plain Ruby object. Focus on writing functionality as methods, and
let `Ractor::Wrapper` handle all the messaging logic.

```ruby
# Simple actor example

require "ractor/wrapper"

class SimpleCalculator
  class EmptyStackError < StandardError
  end

  def initialize
    @stack = []
  end

  def push(number)
    @stack.push(number)
    nil
  end

  def pop
    raise EmptyStackError if @stack.empty?
    @stack.pop
  end

  def add
    push(pop + pop)
    nil
  end
end

# Create an actor based on SimpleCalculator
calc_actor = Ractor::Wrapper.new(SimpleCalculator.new)

# You can now send messages by calling methods
calc_stub = calc_actor.stub
calc_stub.push(2)
calc_stub.push(3)
calc_stub.add
sum = calc_stub.pop

# Stop the actor by calling async_stop
calc_actor.async_stop
# Wait for the actor to shut down
calc_actor.join
```

## Configuring a wrapper

`Ractor::Wrapper` supports a fair amount of configuration, which may be needed
in order to ensure good behavior of the wrapped object. You can configure many
aspects of `Ractor::Wrapper` by passing keyword arguments to its constructor.
Alternatively, you can pass a block to the constructor; the constructor will
yield a configuration interface to your block, letting you configure the
wrapper's behavior in detail.

The various configuration options are described below.

### Current Ractor mode

Normally a wrapper will spawn a new Ractor and move the wrapped object into
that Ractor. We call this default mode the "isolated Ractor" mode. Isolated
Ractor lets the object function as an actor that can be called uniformly from
any Ractor.

However, some objects cannot be moved to a different Ractor. This in particular
can include certain C-based I/O objects such as database connections.
Additionally, there are other objects that can live only in the main Ractor. If
the object to be wrapped cannot be moved to its own Ractor, configure it with
`use_current_ractor`, which will run the wrapper in a Thread in the calling
Ractor rather than trying to move it to its own Ractor. The SQLite3 example
above demonstrates wrapping an object that cannot be moved to its own Ractor.

### Sequential vs concurrent execution

By default, wrappers run sequentially in a single Thread. The wrapper will
handle only a single method call at a time, and any other concurrent requests
are queued and blocked until their turn. This is the behavior of the classic
actor model, and in particular is appropriate for wrapped objects that are not
thread-safe.

You can, however, configure a wrapper with concurrent access. This will spin up
a configurable number of worker threads within the wrapper, to handle
potentially concurrent method calls. You should set this configuration only if
you are certain the wrapped object can handle concurrent access.

### Data communication options

When you call a method on a wrapper, and you pass arguments and receive a
return value, or you pass a block that can receive arguments and return a
value, those objects are communicated to and from the wrapper via Ractor ports.
As such, if they are not shareable, they may be *copied* or *moved*. By
default, values are copied in order to minimize interference with surrounding
code, but a wrapper can be configured to move objects instead.

This configuration is done per-method, using the `configure_method` call in the
configuration block. You can, for particular method names, specify whether each
type of value: arguments, return values, block arguments, and block return
values, are copied or moved. For any given method, you must configure all
arguments to be handled the same way, but different methods can have different
configurations. You can also provide a default configuration that will apply to
all method names that are not explicitly configured.

Return values (and block return values) have a third configuration option:
*void*. This option disables communication of return values, sending `nil`
instead of what was actually returned from the method. This is intended for
methods that do not *semantically* need to return anything, but because of
their implementation they actually do return some internal object. You can use
the *void* option to prevent those methods from wasting resources copying a
return object unnecessarily, or worse, moving an object that shouldn't be moved.

### Block execution environment

If a block is passed to a method, it is handled in one of two ways. By default,
if/when the method yields to the block, the wrapper will send a message *back*
to the caller, and the block will be executed in the caller's environment. In
most cases, this is what you want; your block may access information from its
lexical environment, and that environment would not be available to the wrapped
object. However, this extra communication can add overhead.

As an alternative, you can configure, per-method, blocks to be executed in the
context of the *wrapped object*. Effectively, the block itself is *moved* into
the wrapped object's Ractor/context, and called directly. This will work only
if the block does not access any information from its lexical context, or
anything that cannot be accessed from a different Ractor. A block must truly be
self-contained in order to use this option.

As with data communication options, configuring block execution environment is
done using the `configure_method` call in the configuration block. You can set
the environment either to `:caller` or `:wrapped`, and you can do so for an
individual method or provide a default to apply to all methods not explicitly
configured.

## Additional features

### Wrapper shutdown

If you are done with a wrapper, you should shut it down by calling `async_stop`.
This method will initiate a graceful shutdown of the wrapper, finishing any
pending method calls, and putting the wrapper in a state where it will refuse
new calls. Any additional method calls will cause a
`Ractor::Wrapper::StoppedError` to be raised.

`Ractor::Wrapper` also provides a `join` method that can be called to wait for
the wrapper to complete its shutdown.

### Wrapped object access

The general intent is that once you've wrapped an object, all access should go
through the wrapper. In the default "isolated Ractor" mode, the wrapped object
is in fact *moved* to a different Ractor, so the Ractor system will prevent you
from accessing it directly. In "current Ractor" mode, the wrapped object is not
moved, so you technically could continue to access it directly from its
original Ractor. But beware: the wrapper runs a thread and will be making calls
to the object from that thread, which may cause you problems if the object is
not thread-safe.

In "isolated Ractor" mode, after you shut down the wrapper, you can recover the
original object by calling `recover_object`. Only one Ractor can call this
method; the object will be moved into the requesting Ractor, and any other
Ractor that subsequently requests the object will get an exception instead.

In "current Ractor" mode, the object will never have been moved to a different
Ractor, so any pre-existing references (in the original Ractor) will still be
valid. In this case, `recover_object` is not necessary and will raise an
exception if called.

### Error handling

`Ractor::Wrapper` provides fairly robust handling of errors. If a method call
raises an exception, the exception will be passed back to the caller and raised
there. In the unlikely event that the wrapper itself crashes, it goes through a
very thorough clean-up process and makes every effort to shut down gracefully,
notifying any pending method calls that the wrapper has crashed by raising
`Ractor::Wrapper::CrashedError`.

### Automatic stub conversion

One special case handled by the wrapper is methods that return `self`. This is
a common pattern in Ruby and is used to allow "chaining" interfaces. However,
you generally cannot return `self` from a wrapped object because, depending on
the communication configuration, you'll either get a *copy* of `self`, or
you'll *move* the object out of the wrapper, thus breaking the wrapper. Thus,
`Ractor::Wrapper` explicitly detects when methods return `self`, and instead
replaces it with the wrapper's stub object. The stub is shareable, and designed
to have the same usage as the original object, so this should work for most use
cases.

## Known issues

Ractors are in general somewhat "bolted-on" to Ruby, and there are a lot of
caveats to their use. This also applies to `Ractor::Wrapper`, which itself is
essentially a workaround to the fact that Ruby has a lot of use cases that
simply don't play well in a Ractor world. Here we'll discuss some of the
caveats and known issues with `Ractor::Wrapper`.

### Data communication issues

As of Ruby 4.0, most objects have been retrofitted to play reasonably with
Ractors. Some objects are shareable across Ractors, and most others can be
moved from one Ractor to another. However, there are a few objects that,
because of their semantics or details about their implementation, cannot be
moved and are confined to their creating Ractor (or in some cases, only the
main Ractor.) These may include objects such as threads, procs, backtraces, and
certain C-based objects.

One particular case of note is exception objects, which one might expect to be
shareable, but are not. Furthermore, they cannot be moved, and even copying an
exception has issues (in particular the backtrace of a copy gets cleared out).
See https://bugs.ruby-lang.org/issues/21818 for more info. When a method raises
an exception, `Ractor::Wrapper` communicates that exception via copying, which
means that currently backtraces will not be present.

### Blocks

Ruby blocks pose particular challenges for `Ractor::Wrapper` because of their
semantics and some of their common usage patterns. We've already seen above
that `Ractor::Wrapper` can run them either in the caller's context or in the
wrapped object's context, which may limit what the block can do. Additionally,
the following restrictions apply to blocks:

Blocks configured to run in the caller's context can be run only while the
method is executing; i.e. they can only be "yielded" to. The wrapped object
cannot "save" the block as a proc to be run later, unless the block is
configured to run in the "wrapped object's" context. This is simply because we
have access to the caller only while the caller is making a method call. After
the call is done, we no longer have access to that context, and there's no
guarantee that the caller or its Ractor even exists anymore. In particular,
this means that the common Ruby idiom of using blocks to define callbacks (that
run in the context of the code defining the callback) can generally not be done
through a wrapper.

In Ruby, it is legal (although not considered very good practice) to do a
non-local `return` from inside a block. Assuming the block isn't being defined
via a lambda, this causes a return from the method *surrounding* the call that
includes the block. However, `Ractor::Wrapper` cannot reproduce this behavior.
Attempting to `return` within a block that was passed to `Ractor::Wrapper` will
result in an exception.

### Block re-entrancy in a separate Thread/Fiber

One final known corner case has to do with block re-entrancy, i.e. calling a
method *from within a block passed to another call to the same object*. This
would mean that there are two "active" method calls to the object at once: one
made while another is "suspended" because it has yielded to the block.

```ruby
stub.method_with_block do
  stub.another_method
end
```

In most cases, the wrapper handles this case properly, even in sequential mode
when it has no concurrency. Internally, it uses fibers to track each method
call, and it yields the fiber when yielding to a block. However, if the
"internal" method call itself is done in a separate Fiber or Thread, this would
break the internal fiber tracking. In such a case, the wrapper falls back to a
"blocking" model where it depends on concurrency to handle the simultaneous
method calls. If run in sequential mode, or if not enough worker threads are
available, this can deadlock.

```ruby
# Can deadlock if the wrapper was created in sequential mode
stub.method_with_block do
  t = Thread.new do
    stub.another_method
  end
  t.join
end
```

To avoid this issue, do not call methods on the same wrapper from within a
separate Thread or Fiber spawned inside a block passed to that wrapper.

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

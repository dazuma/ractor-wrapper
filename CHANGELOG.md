# Release History

### v0.4.0 / 2026-03-30

This release includes two major changes: it greatly improves robustness in the case of server crashes, and it reworks the method call configuration interface. This involves several breaking changes, and I expect the interface will continue to be a bit unstable for now as I'm working through use cases and edge cases. The README has also been expanded to include more information on the configuration options and the known issues.

* ADDED: Uses a separate `Ractor::Wrapper::Configuration` class for block-based initialization. Removed the configuration mutation methods from `Ractor::Wrapper` itself.
* BREAKING CHANGE: The method configuration interface now uses symbolic settings values instead of booleans for more flexibility
* ADDED: Support for suppressing return values for methods and blocks that unintentionally return something they shouldn't
* BREAKING CHANGE: Raises `Ractor::Wrapper::StoppedError` instead of `Ractor::ClosedError` if a method is called via the wrapper after the wrapper has stopped
* BREAKING CHANGE: `Wrapper#join` now returns normally rather than raising, if an isolated wrapper terminated due to a crash
* BREAKING FIX: `Wrapper#join` no longer hangs if a local wrapper crashes, but returns to indicate that the wrapper has stopped (albeit non-normally)
* FIXED: Internal cleanup is more robust if a crash occurs in the wrapper
* FIXED: Method calls raise `Ractor::Wrapper::CrashedError` instead of hanging if the wrapper crashes during handling
* FIXED: Prevented port leaks if a method call send or a block yield send fails
* FIXED: Methods that return or yield self return/yield the stub instead
* FIXED: The `recover_object` method now raises `Ractor::Wrapper::Error` if recovery failed
* DOCS: Updates to README

### v0.3.0 / 2026-01-05

This is a major update, and the library, while still experimental, is finally somewhat usable. The examples in the README now actually work!

Earlier versions were severely hampered by limitations of the Ractor implementation in Ruby 3. Many of these were fixed in Ruby 4.0, and Ractor::Wrapper has been updated to take advantage of it. This new version requires Ruby 4.0.0 or later, and includes a number of enhancements:

* Support for running a wrapper in the current Ractor, useful for wrapping objects that cannot be moved, or that must run in the main Ractor. (By default, wrapped objects are still moved into an isolated Ractor to maximize cleanliness and concurrency.)
* Support for running a wrapper sequentially without worker threads. This is now the default behavior, which does not spawn any extra threads in the wrapper. (Earlier behavior would spawn exactly one worker thread by default.)
* Limited support for passing blocks to a wrapped object. You can cause a block to run "in place" within the wrapper, as long as the block can be made shareable (i.e. does not access any outside data), or have the block run in the caller's context with the cost of some additional communication. You can also configure that communication to move or copy data.
* Provided Ractor::Wrapper#join for waiting for a wrapper to complete without asking for the wrapped object back.
* Some of the configuration parameters have been renamed.

Some caveats remain, so please consult the README for details. This library should still be considered experimental, and not suitable for production use. I reserve the right to make breaking changes at any time.

### v0.2.0 / 2021-03-08

* BREAKING CHANGE: The wrapper now copies (instead of moves) arguments and return values by default.
* It is now possible to control, per method, whether arguments and return values are copied or moved.
* Fixed: The respond_to? method did not work correctly for stubs.
* Improved: The wrapper server lifecycle is a bit more robust against worker crashes.

### v0.1.0 / 2021-03-02

* Initial release. HIGHLY EXPERIMENTAL.

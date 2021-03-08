# Release History

### v0.2.0 / 2021-03-08

* BREAKING CHANGE: The wrapper now copies (instead of moves) arguments and return values by default.
* It is now possible to control, per method, whether arguments and return values are copied or moved.
* Fixed: The respond_to? method did not work correctly for stubs.
* Improved: The wrapper server lifecycle is a bit more robust against worker crashes.

### v0.1.0 / 2021-03-02

* Initial release. HIGHLY EXPERIMENTAL.

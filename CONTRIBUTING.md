# Contributing to B2 Command Line Tool

We encourage outside contributors to perform changes on our codebase. Many such changes have been merged already. In order to make it easier to contribute, core developers of this project:

* provide guidance (through the issue reporting system)
* provide tool assisted code review (through the Pull Request system)
* maintain a set of unit tests
* maintain a set of integration tests (run with a production cloud)
* maintain development automation tools using [nox](https://github.com/theacodes/nox) that can easily:
   * format the code using [yapf](https://github.com/google/yapf)
   * runs linters to find subtle/potential issues with maintainability
   * run the test suite on multiple Python versions using [pytest](https://github.com/pytest-dev/pytest)
* maintain Continuous Integration (by using GitHub Actions) that:
   * runs all sorts of linters
   * checks if the Python distribution can be built
   * runs all tests on a matrix of 6 versions of Python (including pypy) and 3 operating systems (Linux, Mac OS X and Windows)
   * checks if the documentation can be built properly
* maintain other Continuous Integration tools (coverage tracker)

## Developer Info

You'll need to have [nox](https://github.com/theacodes/nox) installed:

* `pip install nox`

With `nox`, you can run different sessions (default are `lint` and `test`):

* `format` -> Format the code.
* `lint` -> Run linters.
* `test` (`test-3.5`, `test-3.6`, `test-3.7`, `test-3.8`, `test-3.9`) -> Run test suite.
* `cover` -> Perform coverage analysis.
* `build` -> Build the distribution.
* `deploy` -> Deploy the distribution to the PyPi.
* `doc` -> Build the documentation.
* `doc_cover` -> Perform coverage analysis for the documentation.

For example:

    $ nox -s format
    nox > Running session format
    nox > Creating virtual environment (virtualenv) using python3.9 in .nox/format
    ...

    $ nox -s format
    nox > Running session format
    nox > Re-using existing virtual environment at .nox/format.
    ...

    $ nox --no-venv -s format
    nox > Running session format
    ...

Sessions `test` ,`unit`, and `integration` can run on many Python versions, 3.5-3.9 by default.

Sessions other than than use the last given Python version, 3.9 by default.

You can change it:

    export NOX_PYTHONS=3.6,3.7

With the above setting, session `test` will run on Python 3.6 and 3.7, and all other sessions on Python 3.7.

Given Python interpreters should be installed in the operating system or via [pyenv](https://github.com/pyenv/pyenv).

## Linting

To run all available linters:

    nox -s lint

## Testing

To run all tests on every available Python version:

    nox -s test

To run all tests on a specific version:

    nox -s test-3.9

To run just unit tests:

    nox -s unit-3.9

To run just integration tests:

    export B2_TEST_APPLICATION_KEY=your_app_key
    export B2_TEST_APPLICATION_KEY_ID=your_app_key_id
    nox -s integration-3.9

## Documentation

To build the documentation and watch for changes (including the source code):

    nox -s doc

To just build the documentation:

    nox --non-interactive -s doc

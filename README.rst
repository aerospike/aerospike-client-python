Aerospike Python Client
=======================
|Build| |Release| |Wheel| |Downloads| |License|

.. |Build| image:: https://travis-ci.org/aerospike/aerospike-client-python.svg?branch=master
.. |Release| image:: https://img.shields.io/pypi/v/aerospike.svg
.. |Wheel| image:: https://img.shields.io/pypi/wheel/aerospike.svg
.. |Downloads| image:: https://img.shields.io/pypi/dm/aerospike.svg
.. |License| image:: https://img.shields.io/pypi/l/aerospike.svg

AI coding agent entry point
---------------------------

PyPI package ``aerospike``, version in the root ``VERSION`` file. Python 3.10-3.14.
Requires Aerospike server 4.9+. Ops in ``aerospike_helpers`` are functions, not client methods.

What to read, by task (task → read first → authoritative for):

* First put/get — ``examples/client/put.py``, ``get.py``, ``kvs.py`` — one call
* Client setup — ``doc/aerospike.rst`` (config), ``doc/client.rst`` (policies) — parameter semantics
* Signatures, types — ``aerospike-stubs/*.pyi`` — machine-readable signatures
* List / map / bit / HLL ops — ``aerospike_helpers/operations/`` — the operation set
* Expressions, path expressions — ``aerospike_helpers/expressions/``, ``doc/aerospike_helpers.expressions.rst`` — expression set
* Batch — ``doc/client.rst`` ``batch_*`` methods, then ``aerospike_helpers/batch/`` — behavior
* Queries, secondary indexes — ``examples/client/query*.py``, ``doc/query.rst`` — behavior
* Transactions — ``doc/transaction.rst`` to start a transaction, then ``doc/client.rst`` ``abort`` and ``commit`` methods  — semantics
* Errors — ``doc/exception.rst``, ``aerospike-stubs/exception.pyi`` — exception hierarchy

Repository map::

    doc/                  API reference; see doc/README.md, doc/for-ai-agents.rst
    aerospike-stubs/      type stubs (.pyi)
    aerospike_helpers/    data structures to be passed to client API methods: batch, expressions, operations, metrics, cdt_ctx
    examples/             client/ (incl. admin/), string_ops/, run_all_examples.py
    test/new_tests/       actual behavior; see test/README.md
    src/                  CPython extension that wraps around the C client; aerospike-client-c/ is not the Python API
    VERSION, BUILD.md, AGENTS.md, test/standalone/README.md, benchmarks/README.rst

Canonical reference application: no Python SubMilliPost in this repo yet; ``examples/client/`` is single-call only.
Precedence when sources disagree: ``doc/for-ai-agents.rst``.

Known traps:
* Ops are helpers in ``aerospike_helpers.operations``, passed to ``client.operate()`` — no ``client.list_append()``.
* Set ``{"key": aerospike.POLICY_KEY_SEND}`` when the user key matters (default is digest-only).
* Do not loop single-record calls. Pairs: ``put``/``batch_write``, ``get``/``batch_read``, ``select``/``batch_read`` (``bins``), ``exists``/``batch_read`` with an empty bin list, ``operate``/``batch_operate``, ``remove``/``batch_remove``, ``apply``/``batch_apply``.
* Do not special-case a one-key batch. A node sub-batch of size 1 already uses the single-record command for ``batch_read``, ``batch_operate``, ``batch_write``, ``batch_remove``, and ``batch_apply``.
* Nested CDT removal is ``modify_by_path`` with ``RemoveResult().compile()``. The path-modify flags do not delete.

Verifying generated code: ``doc/for-ai-agents.rst``.
Aerospike agent skills: https://github.com/aerospike/agent-skills (data modeling; this repo is the Python API).

Compatibility
-------------

The Python client for Aerospike works with Python 3.10 - 3.14 and supports the following OS'es:

* macOS 14, 15, 26
* RHEL 9 and 10
* Amazon Linux 2023
* Debian 12 and 13
* Ubuntu 22.04 and 24.04
* Windows (x64)

The client is also verified to run on these operating systems, but we do not officially support them (i.e we don't distribute wheels or prioritize fixing bugs for these OSes):

* Alpine Linux

**NOTE:** Aerospike Python client 5.0.0 and up MUST be used with Aerospike server 4.9 or later.
If you see the error "-10, ‘Failed to connect’", please make sure you are using server 4.9 or later.

Install
-------

::

    pip install aerospike

In most cases ``pip`` will install a precompiled binary (wheel) matching your OS
and version of Python. If a matching wheel isn't found it, or the
``--install-option`` argument is provided, pip will build the Python client
from source.

Please see the `build instructions <https://github.com/aerospike/aerospike-client-python/blob/master/BUILD.md>`__
for more.

Troubleshooting
~~~~~~~~~~~~~~~

::

    # client >=3.8.0 will attempt a manylinux wheel installation for Linux distros
    # to force a pip install from source:
    pip install aerospike --no-binary :all:

    # to troubleshoot pip versions >= 6.0 you can
    pip install --no-cache-dir aerospike

If you run into trouble installing the client on a supported OS, you may be
using an outdated ``pip``.
Versions of ``pip`` older than 7.0.0 should be upgraded, as well as versions of
``setuptools`` older than 18.0.0.


Troubleshooting macOS
~~~~~~~~~~~~~~~~~~~~~

In some versions of macOS, Python 2.7 is installed as ``python`` with
``pip`` as its associated package manager, and Python 3 is installed as ``python3``
with ``pip3`` as the associated package manager. Make sure to use the ones that
map to Python 3, such as ``pip3 install aerospike``.

Attempting to install the client with pip for the system default Python may cause permissions issues when copying necessary files. In order to avoid
those issues the client can be installed for the current user only with the command: ``pip install --user aerospike``

::

    # to trouleshoot installation on macOS try
    pip install --no-cache-dir --user aerospike


Build
-----

For instructions on manually building the Python client, please refer to
`BUILD.md <https://github.com/aerospike/aerospike-client-python/blob/master/BUILD.md>`__.

Documentation
-------------

Documentation is hosted at `aerospike-python-client.readthedocs.io <https://aerospike-python-client.readthedocs.io/>`__
and at `aerospike.com/apidocs/python <http://www.aerospike.com/apidocs/python/>`__.

Examples
--------

Example applications are provided in the `examples directory of the GitHub repository <https://github.com/aerospike/aerospike-client-python/tree/master/examples/client>`__

For examples, to run all code examples:

::

    python3 -m examples.run_all_examples

To run a specific code example from ``examples/client/kvs.py``:

    python3 -m examples.run_all_examples KVS

Benchmarks
----------

To run the benchmarks the python module 'tabulate' need to be installed. In order to display heap information the module `guppy` must be installed.
Note that `guppy` is only available for Python2. If `guppy` is not installed the benchmarks will still be runnable.
Benchmark applications are provided in the `benchmarks directory of the GitHub repository <https://github.com/aerospike/aerospike-client-python/tree/master/benchmarks>`__

By default the benchmarks will try to connect to a server located at 127.0.0.1:3000 , instructions on changing that setting and other command line flags may be displayed by appending the `--help` argument to the benchmark script. For example:
::

    python benchmarks/keygen.py --help

License
-------

The Aerospike Python Client is made availabled under the terms of the
Apache License, Version 2, as stated in the file ``LICENSE``.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual
files for details.

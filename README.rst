Aerospike Python Client
=======================
|Build| |Release| |Wheel| |Downloads| |License|

.. |Build| image:: https://travis-ci.org/aerospike/aerospike-client-python.svg?branch=master
.. |Release| image:: https://img.shields.io/pypi/v/aerospike.svg
.. |Wheel| image:: https://img.shields.io/pypi/wheel/aerospike.svg
.. |Downloads| image:: https://img.shields.io/pypi/dm/aerospike.svg
.. |License| image:: https://img.shields.io/pypi/l/aerospike.svg

Compatibility
-------------

The Python client for Aerospike works with Python 3.6, 3.7, 3.8, 3.9 running on
**64-bit** macOS 10.15+ and Linux (RHEL/CentOS 7 & 8; Debian 8, 9 & 10; Ubuntu
18.04 & 20.04).

Python 3.6 hits `End of Life <https://endoflife.date/python>`__ on December 23rd,
2021, and is now deprecated.

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

For examples, to run the ``kvs.py``:

::

    python examples/client/kvs.py


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

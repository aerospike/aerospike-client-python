Aerospike Python Client
=======================
|Build| |Release| |Wheel| |Downloads| |License|

.. |Build| image:: https://travis-ci.org/aerospike/aerospike-client-python.svg?branch=master
.. |Release| image:: https://img.shields.io/pypi/v/aerospike.svg
.. |Wheel| image:: https://img.shields.io/pypi/wheel/aerospike.svg
.. |Downloads| image:: https://img.shields.io/pypi/dm/aerospike.svg
.. |License| image:: https://img.shields.io/pypi/l/aerospike.svg

Dependencies
------------

The Python Client for Aerospike works on Python 2.6, 2.7 running on
**64-bit** OS X 10.9+ and Linux.

The client depends on:

-  Python devel Package
-  The Aerospike C client

RedHat 6+ and CentOS 6+
~~~~~~~~~~~~~~~~~~~~~~~

The following are dependencies for:

-  RedHat Enterprise (RHEL) 6 or newer
-  CentOS 6 or newer
-  and related distributions using ``yum`` package manager.

::

    sudo yum install python-devel
    sudo yum install openssl-devel

Debian 6+ and Ubuntu 12.04+
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following are dependencies for:

-  Debian 6 or newer
-  Ubuntu 12.04 or newer
-  and related distributions using ``apt-get`` package manager.

::

    sudo apt-get install python-dev
    sudo apt-get install libssl-dev

OS X
~~~~~~~~

By default OS X will be missing command line tools. On Mavericks (OS X 10.9)
and higher those `can be installed without Xcode <http://osxdaily.com/2014/02/12/install-command-line-tools-mac-os-x/>`__.

::

    xcode-select --install # install the command line tools, if missing

The dependencies can be installed through the OS X package manager `Homebrew <http://brew.sh/>`__.

::

    brew install openssl

Install
-------

Aerospike Python Client can be installed using `pip`:

::

    pip install aerospike

    # with pip versions >= 6.0 you can

    pip install --no-cache-dir aerospike

Build
-----

Instructions for building Aerospike Python Client, please refer to the 
``BUILD.md`` file for details.

Documentation
-------------

Documentation is hosted at `pythonhosted.org/aerospike <https://pythonhosted.org/aerospike/>`__
and at `aerospike.com/apidocs/python <http://www.aerospike.com/apidocs/python/>`.

Examples
--------

Example applications are provided in the `examples directory of the GitHub repository <https://github.com/aerospike/aerospike-client-python/tree/master/examples/client>`__

For examples, to run the ``kvs.py``:

::

    python examples/client/kvs.py


Becnhmarks
----------

To run the benchmarks the python modules 'guppy' and 'tabulate' need to be installed.
Benchmark applications are provided in the `benchmarks directory of the GitHub repository <https://github.com/aerospike/aerospike-client-python/tree/master/benchmarks>`__

License
-------

The Aerospike Python Client is made availabled under the terms of the
Apache License, Version 2, as stated in the file ``LICENSE``.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual
files for details.

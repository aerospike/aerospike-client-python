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

The Python client for Aerospike works with Python 2.6, 2.7, 3.4, 3.5 running on
**64-bit** OS X 10.9+ and Linux.

The client depends on:

- Python devel package
- OpenSSL
- The Aerospike C client

RedHat 6+ and CentOS 6+
~~~~~~~~~~~~~~~~~~~~~~~

The following are dependencies for:

-  RedHat Enterprise (RHEL) 6 or newer
-  CentOS 6 or newer
-  Related distributions which use the ``yum`` package manager

::

    sudo yum install python-devel
    sudo yum install openssl-devel

Debian 6+ and Ubuntu 12.04+
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following are dependencies for:

- Debian 6 or newer
- Ubuntu 12.04 or newer
- Related distributions which use the ``apt`` package manager

::

    sudo apt-get install python-dev
    sudo apt-get install libssl-dev

OS X
~~~~~~~~

By default OS X will be missing command line tools. On Mavericks (OS X 10.9)
and higher those `can be installed without Xcode <http://osxdaily.com/2014/02/12/install-command-line-tools-mac-os-x/>`__.

::

    xcode-select --install # install the command line tools, if missing

OpenSSL can be installed through the `Homebrew <http://brew.sh/>`__ OS X package
manager.

::

    brew install openssl

Install
-------

Aerospike Python Client can be installed using ``pip``:

::

    pip install aerospike

    # to troubleshoot pip versions >= 6.0 you can
    pip install --no-cache-dir aerospike

    # to trouleshoot installation on OS X El-Capitan (10.11)
    pip install --no-cache-dir --user aerospike

    # to have pip copy the Lua system files to a dir other than /usr/local/aerospike/lua
    pip install aerospike --install-option="--lua-system-path=/opt/aerospike/lua"

If you run into trouble installing the client on a supported OS, you may be
using an outdated ``pip``.
Versions of ``pip`` older than 7.0.0 should be upgraded, as well as versions of
``setuptools`` older than 18.0.0. Upgrading ``pip`` on OS X El-Capitan (10.11)
runs into `SIP issues <https://apple.stackexchange.com/questions/209572/how-to-use-pip-after-the-el-capitan-max-os-x-upgrade>`__
with ``pip install --user <module>`` as the recommended workaround.


Build
-----

For instructions on manually building the Python client, please refer to the
``BUILD.md`` file in this repo.

Documentation
-------------

Documentation is hosted at `pythonhosted.org/aerospike <https://pythonhosted.org/aerospike/>`__
and at `aerospike.com/apidocs/python <http://www.aerospike.com/apidocs/python/>`__.

Examples
--------

Example applications are provided in the `examples directory of the GitHub repository <https://github.com/aerospike/aerospike-client-python/tree/master/examples/client>`__

For examples, to run the ``kvs.py``:

::

    python examples/client/kvs.py


Benchmarks
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

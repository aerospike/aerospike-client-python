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

The Python client for Aerospike works with Python 2.7, 3.4, 3.5, 3.6 running on
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

Debian 6+ and Ubuntu 14.04+
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following are dependencies for:

- Debian 6 or newer
- Ubuntu 14.04 or newer
- Related distributions which use the ``apt`` package manager

::

    sudo apt-get install python-dev
    sudo apt-get install libssl-dev

- You may also need libz:

::

    sudo apt-get install zlib1g-dev

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

    # to trouleshoot installation on OS X El-Capitan (10.11) or OS X Sierra (10.12)
    pip install --no-cache-dir --user aerospike

If you run into trouble installing the client on a supported OS, you may be
using an outdated ``pip``.
Versions of ``pip`` older than 7.0.0 should be upgraded, as well as versions of
``setuptools`` older than 18.0.0.

Lua files
~~~~~~~~~~

The system .lua files used for client side aggregation will be installed.
By default pip will install the .lua files in a subdirectory named `aerospike/lua/` inside of the Python
installations directory for platform specific files. The location of the files can be found by running:

``pip show -f aerospike``


**Note** If the .lua files are stored somewhere besides `/usr/local/aerospike/lua`. and you wish to perform Stream UDF operations it will be necessary to specify the locations of the system modules as a configuration parameter to the Aerospike client constructor:

    config = {'hosts': [('127.0.0.1', 3000)], 'lua': {'system_path': '/path/to/lua'} ...}
    my_client = aerospike.client(config)


OS X Installation
~~~~~~~~~~~~~~~~~~
Upgrading ``pip`` on OS X El-Capitan (10.11) or OS X Sierra(10.12)
runs into `SIP issues <https://apple.stackexchange.com/questions/209572/how-to-use-pip-after-the-el-capitan-max-os-x-upgrade>`__
with ``pip install --user aerospike`` as the recommended workaround to install aerospike on those versions of OS X.

Attempting to install the client with pip for the system default Python may cause permssions issues when copying necesarry files. In order to avoid
those issues the client can be installed for the current user only with the command: ``pip install --user aerospike``

If the version of Python is not in the officially supported list, or the ``--install-option`` argument is provided, pip will attempt to compile the client from source. Please see the `build directions in the GitHub repository <https://github.com/aerospike/aerospike-client-python/blob/master/BUILD.md>`__
to troubleshoot any issues caused by compiling the client.


Build
-----

For instructions on manually building the Python client, please refer to the
``BUILD.md`` file in this repo.

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

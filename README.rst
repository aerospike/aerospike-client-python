Aerospike Python Client
=======================

Dependencies
------------

The Aerospike Python client depends on:

-  The Aerospike C client
-  Lua 5.1.5
-  Python devel Package

The Aerospike Python Client works on Python 2.6.\ *, 2.7.*

RedHat 6+ and CentOS 6+
~~~~~~~~~~~~~~~~~~~~~~~

The following are dependencies for:

-  RedHat Enterprise (RHEL) 6 or newer
-  CentOS 6 or newer
-  and related distributions using ``yum`` package manager.

::

    sudo yum install lua-devel

Debian 6+ and Ubuntu 12.04+
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following are dependencies for:

-  Debian 6 or newer
-  Ubuntu 12.04 or newer
-  and related distributions using ``apt-get`` package manager.

::

    sudo apt-get install liblua5.1-dev

Mac OS X
~~~~~~~~

We recommend building Lua from source. To download the source code and
build the library, follow the instruction provided in the *Lua* section
of the `Aerospike C Client Installation
Guide <http://aerospike.com/docs/client/c/install/macosx.html#lua>`__

Install
-------

Aerospike Python Client can be installed using `pip`:

::

    pip install aerospike

Build
-----

Instructions for building Aerospike Python Client, please refer to the 
`BUILD.md` file for details.

License
-------

The Aerospike Python Client is made availabled under the terms of the
Apache License, Version 2, as stated in the file ``LICENSE``.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual
files for details.

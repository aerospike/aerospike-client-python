# Build Aerospike Python Client

## Dependencies

The Python Client for Aerospike works on Python 2.6, 2.7 running on
**64-bit** OS X 10.9+ and Linux.

The client depends on:

-  Python devel Package
-  The Aerospike C client

### RedHat 6+ and CentOS 6+

The following are dependencies for:

- RedHat Enterprise (RHEL) 6 or newer 
- CentOS 6 or newer 
- and related distributions using `yum` package manager.

**Dependencies**

    sudo yum install python26-devel on CentOS 6 and similar
    sudo yum install python-devel # on CentOS 7
    sudo yum install openssl-devel

### Debian 6+ and Ubuntu 12.04+

The following are dependencies for:

- Debian 6 or newer 
- Ubuntu 12.04 or newer 
- and related distributions using `apt-get` package manager.

**Dependencies**

    sudo apt-get install build-essential python-dev
    sudo apt-get install libssl-dev


### Mac OS X

By default OS X will be missing command line tools. On Mavericks (OS X 10.9)
and higher those [can be installed without Xcode](http://osxdaily.com/2014/02/12/install-command-line-tools-mac-os-x/).

    xcode-select --install # install the command line tools, if missing

The dependencies can be installed through the OS X package manager [Homebrew](http://brew.sh/).

    brew install openssl


## Build

To build the library:

    python setup.py build --force

## Install

To install the library:

    python setup.py install --force

**Note** If you have already installed the Aerospike C Client and it is on your linker path you can build using:

    AEROSPIKE_LUA_PATH="path/to/lua-core/src" NO_RESOLVE_C_CLIENT_DEP=True python setup.py {build,install} --force


## Examples

**Note** If you did not install the library, then you will need to setup your `PYTHONPATH` environment variable. The `PYTHONPATH` should contain an entry for the directory where the Python module is stored. This is usually in `build/lib.*`.


Examples are in the `examples` directory. The following examples are available:

* `kvs.py` — Key-Value Store API Example
* `query.py` — Query API Example
* `scan.py` — Scan API Example
* `info.py` — Info API Example
* `simple.lua` — Simple UDF Example

Each example provides help/usage information when you specify the `--help` option. For example, for help on the `kvs.py` example, then run:

    python examples/client/kvs.py --help


### Running Examples

Simply call `python` with the path to the example

    python examples/client/kvs.py


## License

The Aerospike Python Client is made availabled under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license, 
all compatible with Apache License, Version 2. Please see individual files for details.

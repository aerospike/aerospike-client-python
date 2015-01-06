# Build Aerospike Python Client

## Dependencies

The Aerospike Python client depends on:

- The Aerospike C client 
- Lua 5.1.5
- Python devel Package

The Aerospike Python Client works on Python 2.6.*, 2.7.* 

### RedHat 6+ and CentOS 6+

The following are dependencies for:

- RedHat Enterprise (RHEL) 6 or newer 
- CentOS 6 or newer 
- and related distributions using `yum` package manager.

**Dependencies**

	sudo yum install lua-devel python26-devel
    sudo yum install python-devel # on CentOS 7 and variants

### Debian 6+ and Ubuntu 12.04+

The following are dependencies for:

- Debian 6 or newer 
- Ubuntu 12.04 or newer 
- and related distributions using `apt-get` package manager.

**Dependencies**

	sudo apt-get install build-essential python-dev liblua5.1-dev


### Mac OS X

The Python development and build tools are included with Mac OS X.

We recommend building Lua from source. To download the source code and build the library, follow the instruction provided in the _Lua_ section of the [Aerospike C Client Installation Guide](http://aerospike.com/docs/client/c/install/macosx.html#lua)

## Build

To build the library:

	python setup.py build --force

## Install

To install the library:

	sudo python setup.py install --force

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

# Aerospike Python Client

## Dependencies

The Aerospike Python client depends on:

- The Aerospike C client 
- Lua 5.1.5
- Python devel Package

The Aerospike Python Client works on Python 2.6.*, 2.7.* 


### Aerospike C Client

#### Install From Package

You can download and install the Aerospike C Client development package from:

	http://aerospike.com/download/client/c

When downloaded, you will want to extract the contents of the archive and install the "devel" package. The "devel" package will install header files and libraries. Read the Aerospike C Client **[Installation Guide](http://aerospike.com/docs/client/c/install/)** for additional details.

#### Build From Source

**Note** _The following ONLY pertains to users that are building the Aerospike C Client from source code. If you have installed the C client development package, then you can skip the following._

If you are building the Aerospike C Client from source, then set the `AEROSPIKE_C_HOME` to point to the directory containing the C client repository. 

	export AEROSPIKE_C_HOME={{path to c client source}}

For example, to get the source code, you can `git clone` the source repository into your home directory:

	cd ~
	git clone https://github.com/aerospike/aerospike-client-c

Then build the Aerospike C Client library per the instructions provided in the README.md in the repository.

Then you can set the environment variable:

	export AEROSPIKE_C_HOME=~/aerospike-client-c


### RedHat 6+ and CentOS 6+

The following are dependencies for:

- RedHat Enterprise (RHEL) 6 or newer 
- CentOS 6 or newer 
- and related distributions using `yum` package manager.

#### Install Python Development

	sudo yum install python26-devel

#### Install Lua Development

	sudo yum install lua-devel


### Debian 6+ and Ubuntu 12.04+

The following are dependencies for:

- Debian 6 or newer 
- Ubuntu 12.04 or newer 
- and related distributions using `apt-get` package manager.

#### Install Python Development

	sudo apt-get install build-essential python-dev

#### Install Lua Development

	sudo apt-get install liblua5.1-dev


### Mac OS X


#### Install Lua Development

We recommend building Lua from source. To download the source code and build the library, follow the instruction provided in the _Lua_ section of the [Aerospike C Client Installation Guide](http://aerospike.com/docs/client/c/install/macosx.html#lua)

## Usage

### Build

To build the library:

	python setup.py build --force

### Install

To install the library:

	sudo python setup.py install --force

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

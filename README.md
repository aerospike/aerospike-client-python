# Aerospike Python Client

## Dependencies

The Aerospike Python client depends on the Aerospike C client and Lua.

This was tested on Python 2.6.6, but may work up through Python 3.x. You will need to install Python development packages to build this.


### Environment Variables

If you are building the C client from source, rather than installing the development packages, then set the `AEROSPIKE_C_HOME` to point to the directory containing the C client output directory.

	export AEROSPIKE_C_HOME={path to c client source}/target/Linux-x86_64/

Set the `LD_LIBRARY_PATH` to include the C client library directory:

	export LD_LIBRARY_PATH=$AEROSPIKE_C_HOME/lib

Set the `CPATH` to include the C client include directory
  
  	export CPATH=$AEROSPIKE_C_HOME/include:$AEROSPIKE_C_HOME/include/ck


### Ubuntu 12.04

#### Install Python Development

	sudo apt-get install build-essential python-dev

#### Install Lua Development

	sudo apt-get install lua5.1-dev


### Mac OS X

#### Environment Variables

In addition to the environment variables defined earlier, you will want to set `DYLD_LIBRARY_PATH` to equal `LD_LIBRARY_PATH`:

	export DYLD_LIBRARY_PATH=$LD_LIBRARY_PATH


## Usage

### Build

To build the library:

	$ python setup.py build --force

### Install

To install the library:

	$ sudo python setup.py install --force

## Examples

Examples are in the `examples` directory. The following examples are available:

* `kvs.py` — Key-Value Store API Example
* `query.py` — Query API Example
* `scan.py` — Scan API Example
* `info.py` — Info API Example
* `simple.lua` — Simple UDF Example

### Running Examples

Simply call `python` with the path to the example

	python examples/client/kvs.py


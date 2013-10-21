# Aerospike Python Client

## Dependencies

This depends on the Aerospike C Client, and all its dependencies. You will need to install Aerospike C Client development package.

This was tested on Python 2.6.6, but may work up through Python 3.x. You will need to install Python development packages to build this.

## Usage

### Build

	python setup.py build --force; 
	
### Install
	
	sudo python setup.py install --force

## Examples

Examples are in the `examples` directory. The following examples are available:

* `kvs.py` — Key-Value Store API Example
* `query.py` — Query API Example
* `scan.py` — Scan API Example
* `info.py` — Info API Example
* `simple.lua` — Simple UDF Example

### Running Examples

Simply call `python` with the path to the example

	python examples/kvs.py


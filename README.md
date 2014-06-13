# Aerospike Python Client

## Dependencies

This depends on the Aerospike C Client, and all its dependencies. There are two ways to use the C client:

* Install Aerospike C client development package: 

* Reference your local Aerospike C client cloned repository using environment variables:

`export LD_LIBRARY_PATH=:<parent dir>/aerospike-client-c/target/<platform>/lib`	

`export CPATH=:<parent dir>/aerospike-client-c/target/<platform>/include:<parent dir>/aerospike-client-c/target/<platform>/include/ck`

This was tested on Python 2.6.6, but may work up through Python 3.x. You will need to install Python development packages to build this.

### For Ubuntu 12.04
#### Install Python Development
	sudo apt-get install build-essential python-dev
#### Install Lua Development
	sudo apt-get install lua5.1-dev

## Usage

### Linux Build

###CentOS
```bash
python setup.py build --force
```	
####Ubuntu 
Set the LD_PRELOAD environment variable
```bash
python setup.py build --force
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/liblua5.1.so:/usr/lib/x86_64-linux-gnu/librt.so
```
In the Aerospike C Client Makefile, change the optomizer flag 'O' to O = 1. Follow the build instructions for the Aerospike C client.

###OSX build (experimental)	
```bash
python setup_osx.py build --force
```	
### Install on Linux
	
	sudo python setup.py install --force

### Install on OSX (experimental)
	
	sudo python setup_osx.py install --force

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


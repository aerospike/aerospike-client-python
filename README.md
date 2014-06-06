# Aerospike Python Client

## Dependencies

This depends on the Aerospike C Client, and all its dependencies. There are two ways to use the C client:

* Install Aerospike C client development package: 

* Reference your local Aerospike C client cloned repository using environment variables:

`export LD_LIBRARY_PATH=:<parent dir>/aerospike-client-c/target/<platform>/lib`	

`export CPATH=:<parent dir>/aerospike-client-c/target/<platform>/include:<parent dir>/aerospike-client-c/target/<platform>/include/ck`

This was tested on Python 2.6.6, but may work up through Python 3.x. You will need to install Python development packages to build this.

## Usage

### Build

	python setup.py build --force
	
### Install on Lunix
	
	sudo python setup.py install --force

### Install on OSX
	
	sudo python setup_osx.py install --force

## Examples

Examples are in the `examples` directory. The following examples are available:

* `kvs.py` — Key-Value Store API Example
* `query.py` — Query API Example
* `scan.py` — Scan API Example
* `info.py` — Info API Example
* `simple.lua` — Simple UDF Example

### Running Examples CentOS

Simply call `python` with the path to the example

	python examples/kvs.py

### Running Examples Ububtu

Install Python Development
```bash
sudo apt-get install build-essential python-dev libsqlite3-dev libreadline6-dev libgdbm-dev zlib1g-dev libbz2-dev sqlite3 zip
```
Install Lua Development
```bash
sudo apt-get install lua5.1-dev
```
Set the LD_PRELOAD environment variable
```bash
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/liblua5.1.so:/usr/lib/x86_64-linux-gnu/librt.so
```

Then run `python` with the path to the example

	python examples/kvs.py
	

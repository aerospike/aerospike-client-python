# Manually Building the Python Client for Aerospike

First clone this repository to get the necessary files.

## Dependencies

The Python client for Aerospike works with Python 2.7, 3.4, 3.5, 3.6 running on
**64-bit** OS X 10.9+ and Linux.

The client depends on:

- The Python devel package
- OpenSSL
- The Aerospike C client


### RedHat 6+ and CentOS 6+

The following are dependencies for:

 -  RedHat Enterprise (RHEL) 6 or newer
 -  CentOS 6 or newer
 -  Related distributions which use the `yum` package manager

```
sudo yum install openssl-devel
sudo yum install python26-devel # on CentOS 6 and similar
sudo yum install python-devel # on CentOS 7
# Possibly needed
sudo yum install python-setuptools
```

To get `python26-devel` on older distros such as CentOS 5, see [Stack Overflow](http://stackoverflow.com/a/11684053/582436).


### Debian 6+ and Ubuntu 12.04+

The following are dependencies for:

 - Debian 6 or newer
 - Ubuntu 12.04 or newer
 - Related distributions which use the `apt` package manager

```
sudo apt-get install libssl-dev
sudo apt-get install build-essential python-dev
```


### OS X

By default OS X will be missing command line tools. On Mavericks (OS X 10.9)
and higher those [can be installed without Xcode](http://osxdaily.com/2014/02/12/install-command-line-tools-mac-os-x/).

    xcode-select --install # install the command line tools, if missing

The dependencies can be installed through the OS X package manager [Homebrew](http://brew.sh/).

    brew install openssl


## Build

To build the library:

    git submodule update --init
    python setup.py build --force


The helper `scripts/aerospike-client-c.sh` is triggered by `setup.py` to
download the appropriate C client. However, if one is present this will not
happen, and a build may fail against an old client. At that point you should
remove the directory `aerospike-client-c` and run the build command again.


### Troubleshooting OS X builds
Building on OS X versions >= 10.11 , may cause a few additional errors to be generated. If the build command fails with an
error similar to: `error: could not create '/usr/local/aerospike/lua': Permission denied` there are a couple of options:
	
- Rerun the build command with the additional command line flags `--user --prefix=` *Note that there are no charcters after the '='.* This will cause the library to only be installed for the current user, and store the library's data files in a user specific location.
- rerun the command with sudo.

If an error similar to `ld: targeted OS version does not support use of thread local variables` appears, it can be fixed by temporarily setting the `MACOSX_DEPLOYMENT_TARGET` environment variable to `'10.12'` e.g.

```
MACOSX_DEPLOYMENT_TARGET=10.12 python setup.py build --force
MACOSX_DEPLOYMENT_TARGET=10.12 python setup.py install --force
```

### Building on an Unsupported Linux Distro

If you are installing the Python client on an unsupported OS, such as CentOS 5,
you will need to first build the C client manually.

1. Clone the [aerospike/aerospike-client-c](https://github.com/aerospike/aerospike-client-c) repo from GitHub.
2. Install the dependencies. See the [README](https://github.com/aerospike/aerospike-client-c/blob/master/README.md).
3. Change directory to the C client, and build it.

```
git submodule update --init
make
```

4. Clone the [aerospike/aerospike-lua-core](https://github.com/aerospike/aerospike-lua-core) repo from GitHub.
5. Change directory to the Python client and build it.

```
export DOWNLOAD_C_CLIENT=0
export AEROSPIKE_C_HOME=/path/to/aerospike-c-client
export AEROSPIKE_LUA_PATH=/path/to/aerospike-lua-core/src
python setup.py build --force
```

If using sudo, you may need to set the values inline with the command:

```
sudo DOWNLOAD_C_CLIENT=0 AEROSPIKE_C_HOME=/path/to/aerospike-c-client AEROSPIKE_LUA_PATH=/path/to/aerospike-lua-core/src python setup.py build --force
```


## Install

To install the library:

    python setup.py install --force



### Troubleshooting OS X Installation
Installing on OS X versions >= 10.11 , may cause a few additional errors to be generated. If the install command fails with an
error similar to: `error: could not create '/usr/local/aerospike/lua': Permission denied` there are a couple of options:
	
- Rerun the install command with the additional command line flags `--user --prefix=` *Note that there are no charcters after the '='.* This will cause the library to only be installed for the current user, and store the library's data files in a user specific location.
- rerun the command with sudo.

### Lua System Modules

Stream UDF functionality requires a local copy of the system Lua modules.
By default, those Lua files are copied to an `aerospike` directory inside of Python's' installation path for system dependent packages. This directory can be viewed by running  `python -c "import sys; print(sys.prefix);" `


**Note** The default search location for the lua system files is `/usr/local/aerospike/lua`. If the .lua files are stored somewhere besides `/usr/local/aerospike/lua`. and you wish to perform Stream UDF operations it will be necessary to specify the locations of the system modules as a configuration parameter to the Aerospike client constructor:

	config = {'hosts': [('127.0.0.1', 3000)], 'lua': {'system_path': '/path/to/lua'} ...}
	my_client = aerospike.client(config)

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

# Manually Building the Python Client for Aerospike

First clone this repository to get the necessary files.

## Dependencies

The Python client for Aerospike works with Python 3.6, 3.7, 3.8, 3.9 running on
**64-bit** OS X 10.9+ and Linux.

The client depends on:

- The Python devel package
- OpenSSL
- The Aerospike C client

### RedHat 6+ and CentOS 6+

The following are dependencies for:

- RedHat Enterprise (RHEL) 6 or newer
- CentOS 6 or newer
- Related distributions which use the `yum` package manager

```sh
sudo yum install openssl-devel
sudo yum install python26-devel # on CentOS 6 and similar
sudo yum install python-devel # on CentOS 7
# Possibly needed
sudo yum install python-setuptools
```

To get `python26-devel` on older distros such as CentOS 5, see [Stack Overflow](http://stackoverflow.com/a/11684053/582436).

### Debian 6+ and Ubuntu 14.04+

The following are dependencies for:

- Debian 6 or newer
- Ubuntu 14.04 or newer
- Related distributions which use the `apt` package manager

```sh
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

    if repo is not cloned with "git clone --recurse-submodules --remote-submodules ...", run the following command to initialize necessary sub-modules:
        git submodule update --init --remote --checkout --recursive
    python setup.py build --force

### Troubleshooting OS X builds

Building on OS X versions >= 10.11 , may cause a few additional errors to be generated. If the build command fails with an
error similar to: `error: could not create '/usr/local/aerospike/lua': Permission denied` there are a couple of options:

- Rerun the build command with the additional command line flags `--user --prefix=` *Note that there are no charcters after the '='.* This will cause the library to only be installed for the current user, and store the library's data files in a user specific location.
- rerun the command with sudo.

If an error similar to `ld: targeted OS version does not support use of thread local variables` appears, it can be fixed by temporarily setting the `MACOSX_DEPLOYMENT_TARGET` environment variable to `'10.12'` e.g.

```sh
MACOSX_DEPLOYMENT_TARGET=10.12 python setup.py build --force
MACOSX_DEPLOYMENT_TARGET=10.12 python setup.py install --force
```

## Install

To install the library:

    python setup.py install --force

### Troubleshooting OS X Installation

Installing on OS X versions >= 10.11 , may cause a few additional errors to be generated. If the install command fails with an
error similar to: `error: could not create '/usr/local/aerospike/lua': Permission denied` there are a couple of options:

- Rerun the install command with the additional command line flags `--user --prefix=` *Note that there are no charcters after the '='.* This will cause the library to only be installed for the current user, and store the library's data files in a user specific location.
- rerun the command with sudo.

## Examples

**Note** If you did not install the library, then you will need to setup your `PYTHONPATH` environment variable. The `PYTHONPATH` should contain an entry for the directory where the Python module is stored. This is usually in `build/lib.*`.

Examples are in the `examples` directory. The following examples are available:

- `kvs.py` — Key-Value Store API Example
- `query.py` — Query API Example
- `scan.py` — Scan API Example
- `info.py` — Info API Example
- `simple.lua` — Simple UDF Example

Each example provides help/usage information when you specify the `--help` option. For example, for help on the `kvs.py` example, then run:

    python examples/client/kvs.py --help

### Running Examples

Simply call `python` with the path to the example

    python examples/client/kvs.py

## License

The Aerospike Python Client is made availabled under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

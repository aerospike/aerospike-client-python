# Manually Building the Python Client for Aerospike

First clone this repository to get the necessary files.

`git clone --recurse-submodules ...`

or to initialize the submodules after cloning

`git submodule update --init --checkout --recursive`

## Dependencies

The client depends on:

- The Python devel package
- OpenSSL 1.1 >= 1.1.1
- The Aerospike C client

### RedHat, CentOS, Amazon Linux 2023

The following are dependencies for:

- RedHat Enterprise (RHEL) 8 or newer
- CentOS 7 Linux
- Related distributions which use the `yum` package manager

```sh
sudo yum install openssl-devel
sudo yum install python-devel # on CentOS 7
# Possibly needed
sudo yum install python-setuptools
```

### Debian and Ubuntu

The following are dependencies for:

- Debian 10 or newer
- Ubuntu 20.04 or newer
- Related distributions which use the `apt` package manager

```sh
sudo apt-get install libssl-dev
sudo apt-get install build-essential python-dev
```

### Archlinux

AUR package is available for Python 3.

```sh
yaourt -S aerospike-client-python
```

Dependencies:

 ```sh
 sudo pacman -S binutils gcc
 ```

### Alpine Linux

Dependencies:

```sh
apk add py3-pip
apk add python3-dev
apk add zlib-dev
apk add git
# C client dependencies
apk add automake
apk add make
apk add musl-dev
apk add gcc
apk add openssl-dev
apk add lua-dev
apk add libuv-dev  # (for node.js)
apk add doxygen  # (for make docs)
apk add graphviz # (for make docs)
```

### macOS

By default macOS will be missing command line tools.

    xcode-select --install # install the command line tools, if missing

The dependencies can be installed through the macOS package manager [Homebrew](http://brew.sh/).

    brew install openssl@1
    # brew uninstall openssl@3

### All distros

Install `clang-format` for formatting the C source code:
```
sudo apt install clang-format
```

## Build

Before building the wheel, it is recommended to manually clean the C client build:
```
python3 setup.py clean
```
Sometimes the C client will not rebuild if you switch branches and update the C client submodule, and you will end up
using the wrong version of the C client. This can causes strange issues when building or testing the Python client.

Also, for macOS or any other operating system that doesn't have OpenSSL installed by default, you must install it and
specify its location when building the wheel. In macOS, you would run these commands:
```
export SSL_LIB_PATH="$(brew --prefix openssl@1.1)/lib/"
export CPATH="$(brew --prefix openssl@1.1)/include/"
export STATIC_SSL=1
```

Then build the source distribution and wheel.
```
python3 -m pip install -r requirements.txt
python3 -m build
```

<!-- To build the client without any optimizations (usually for debugging), pass in the DEBUG environment variable:
```
DEBUG=1 python3 -m build
``` -->

### Troubleshooting macOS

In some versions of macOS, Python 2.7 is installed as ``python`` with
``pip`` as its associated package manager, and Python 3 is installed as ``python3``
with ``pip3`` as the associated package manager. Make sure to use the ones that
map to Python 3.

Building on macOS versions >= 10.15 , may cause a few additional errors to be generated. If the build command fails with an
error similar to: `error: could not create '/usr/local/aerospike/lua': Permission denied` there are a couple of options:

- Rerun the build command with the additional command line flags `--user --prefix=` *Note that there are no charcters after the '='.* This will cause the library to only be installed for the current user, and store the library's data files in a user specific location.
- rerun the command with sudo.

## Install

Once the client is built:

    pip install .

### Troubleshooting macOS

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

## Contributing

### Precommit Hooks

All commits must pass precommit hook tests. To install precommit hooks:
```
pip install pre-commit
pre-commit install
```

This will run the lint tests for the C and Python code in this project.

See pre-commit's documentation for more usage explanations.

## License

The Aerospike Python Client is made availabled under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

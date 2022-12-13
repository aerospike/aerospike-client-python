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

### RedHat 7+ and CentOS 7+

The following are dependencies for:

- RedHat Enterprise (RHEL) 7 or newer
- CentOS 7 or newer
- Related distributions which use the `yum` package manager

```sh
sudo yum install openssl-devel
sudo yum install python-devel # on CentOS 7
# Possibly needed
sudo yum install python-setuptools
```

### Debian 8+ and Ubuntu 18.04+

The following are dependencies for:

- Debian 8 or newer
- Ubuntu 18.04 or newer
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

    export STATIC_SSL=1
    # substitute the paths to your OpenSSL 1.1 library
    export SSL_LIB_PATH=/usr/local/Cellar/openssl@1.1/1.1.1l/lib/
    export CPATH=/usr/local/Cellar/openssl@1.1/1.1.1l/include/

Before building the wheel, it is recommended to manually clean the C client build:
```
python3 setup.py clean
```
Sometimes the C client will not rebuild if you switch branches and update the C client submodule, and you will end up
using the wrong version of the C client. This can causes strange issues when building or testing the Python client.

Then build the source distribution and wheel:
```
pip install build
python3 -m build
```

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

### Codestyle

All code in `aerospike_helpers` must pass a lint test using `flake8`:
```
pip install flake8
```

The command is:
```
python3 -m flake8
```

All C source code must be formatted with `clang-format`:
```
clang-format -i <filename>
```

## License

The Aerospike Python Client is made availabled under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

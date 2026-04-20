#!/bin/bash

set -e
set -x

# On macOS and Windows, this is expected to run on bare metal
# On Linux, this is expected to run in a container from the manylinux image
os=$(uname -s)
[[ "$os" =~ CYGWIN* || "$os" =~ MINGW* ]]
# Known issue: https://github.com/koalaman/shellcheck/issues/2937
# shellcheck disable=SC2319
running_on_windows=$?
if [[ $running_on_windows -eq 0 ]]; then
    cd aerospike-client-c/vs
    nuget restore
    cd -
fi

arch=$(uname -m)

if [[ $os =~ Darwin* ]]; then
    # It is not documented in Github that openssl 3 / libyaml is installed in their macos images
    # so we install it here to be safe
    brew install openssl@3 libyaml

    # Set minimum macos version required to install wheel
    #
    # By default, clang will try to build the wheel to be compatible with macOS 11.0 / 10.13
    # We are trying to make the wheel compatible with the lowest supported macOS version for the Python client
    # Here, we are telling clang to target the latter.
    #
    # We use delocate to repair the wheel because it throws an error if
    # the openssl library version is newer than the wheel's macOS version tag
    # Linking the static libraries produces a warning for that same reason but it doesn't throw an error.
    #
    # Use single dash for backwards compatibility with older sw_vers
    export MACOSX_DEPLOYMENT_TARGET
    MACOSX_DEPLOYMENT_TARGET="$(sw_vers -productVersion | cut -d"." -f 1).0"

    if [[ $arch == "arm64" ]]; then
        # Ensure that linker can find brew packages
        # On mac m1, packages installed via brew are not in the linker's default library path
        libraries=('libyaml' 'openssl')
        for library in "${libraries[@]}"; do
          LIBRARY_PATH="${LIBRARY_PATH}:$(brew --prefix "$library")/lib"
        done
        export LIBRARY_PATH="$LIBRARY_PATH"
    else
        # This fixes an issue where there is not enough room in the wheel's shared library to replace the rpath
        # Just do it for all Python versions (even those that don't require more room) for futureproofing
        export LDFLAGS='-headerpad_max_install_names'
    fi
elif [[ $os =~ Linux* ]]; then
    yum install libyaml-devel -y
    if [[ $arch == "x86_64" ]]; then
        # manylinux_2_28 x64 image doesn't search in this directory for shared libraries
        export LD_LIBRARY_PATH=/usr/local/lib64
    fi
fi

python3 -m pip install build -c requirements.txt
python3 -m build

REPAIRED_WHEEL_DIR=wheelhouse

# TODO - macOS python versions / repair dependencies need to be installed without cibuildwheel
# Same with windows, probably.

unrepaired_wheel_path=$(find dist/ -type f -name '*.whl' | head -n 1)
if [[ $os =~ Linux* ]]; then
    if [[ "$VERIFY_REPAIR" != "" ]]; then
        # We want to check that our wheel links to the new openssl 3 install, not the system default
        # This assumes that ldd prints out the "soname" for the libraries
        # We can also manually verify the repair worked by checking the repaired wheel's compatibility tag
        auditwheel show "$unrepaired_wheel_path"
        WHEEL_DIR=wheel-contents
        unzip "$unrepaired_wheel_path" -d $WHEEL_DIR
        ldd $WHEEL_DIR/*.so | awk '{print $1}' | grep libssl.so.3
        ldd $WHEEL_DIR/*.so | awk '{print $1}' | grep libcrypto.so.3
    fi

    auditwheel repair -w "$REPAIRED_WHEEL_DIR" "$unrepaired_wheel_path"

    if [[ "$VERIFY_REPAIR" != "" ]]; then
        auditwheel show "$REPAIRED_WHEEL_DIR/*"
        rm -rf "$WHEEL_DIR"
    fi

elif [[ $os =~ Darwin* ]]; then
    pip install delocate -c requirements.txt
    delocate-wheel --require-archs "$arch" -w "$REPAIRED_WHEEL_DIR" -v "$unrepaired_wheel_path"
    if [[ "$VERIFY_REPAIR" != "" ]]; then
        # Do the same verification step like with Linux
        delocate-listdeps "$REPAIRED_WHEEL_DIR/*.whl" | grep libcrypto.3.dylib
        delocate-listdeps "$REPAIRED_WHEEL_DIR/*.whl" | grep libssl.3.dylib
    fi
elif [[ $running_on_windows ]]; then
    pip install delvewheel -c requirements.txt
    delvewheel repair -vv --add-path ./aerospike-client-c/vs/x64/Release -w wheelhouse "$unrepaired_wheel_path"
fi

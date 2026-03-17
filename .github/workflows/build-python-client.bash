# On macOS and Windows, this is expected to run on bare metal
# On Linux, this is expected to run in a container from the manylinux image
os=$(uname -s)
running_on_windows=$([[ "$os" =~ CYGWIN* || "$os" =~ MINGW* ]])
if [[ $running_on_windows ]]; then
    nuget restore
fi

if [[ $os =~ Darwin* ]]; then
    brew install openssl@3 libyaml

    export MACOSX_DEPLOYMENT_TARGET
    MACOSX_DEPLOYMENT_TARGET="$(sw_vers -productVersion | cut -d"." -f 1).0"

    if [[ $(uname -m) == "arm64" ]]; then
        libraries=('libyaml' 'openssl')
        for library in "${libraries[@]}"; do
          LIBRARY_PATH="${LIBRARY_PATH}:$(brew --prefix "$library")/lib"
        done
        export LIBRARY_PATH="$LIBRARY_PATH"
    else
        export LDFLAGS='-headerpad_max_install_names'
    fi
fi

if [[ $os =~ Linux* ]]; then
    yum install libyaml-devel -y
fi

python3 -m pip install build -c requirements.txt
python3 -m build

if [[ $os =~ Linux* ]]; then
    unrepaired_wheel_path=$(find dist/ -type f -name '*.whl' | head -n 1)
    if [[ "$DEBUG" != "" ]]; then
        auditwheel show "$unrepaired_wheel_path"
        WHEEL_DIR=wheel-contents
        unzip "$unrepaired_wheel_path" -d $WHEEL_DIR
        ldd $WHEEL_DIR/*.so | awk '{print $1}' | grep libssl.so.3
        ldd $WHEEL_DIR/*.so | awk '{print $1}' | grep libcrypto.so.3
    fi

    auditwheel repair -w wheelhouse/ "$unrepaired_wheel_path"

    if [[ "$DEBUG" != "" ]]; then
        auditwheel show wheelhouse/*
    fi

    # TODO: This should happen if any steps after creating the dir fail
    rm -rf "$WHEEL_DIR"
elif [[ $os =~ Darwin* ]]; then
    delocate-wheel --require-archs {delocate_archs} -w {dest_dir} -v {wheel}
    delocate-listdeps {dest_dir}/*.whl | grep libcrypto.3.dylib
    delocate-listdeps {dest_dir}/*.whl | grep libssl.3.dylib
elif [[ $running_on_windows ]]; then
    delvewheel repair -vv --add-path ./aerospike-client-c/vs/x64/Release -w wheelhouse "$unrepaired_wheel_path"
fi

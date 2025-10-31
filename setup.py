# -*- coding: utf-8 -*-
################################################################################
# Copyright 2013-2021 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import os
import platform
import sys
import subprocess
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
from distutils.command.clean import clean
import xml.etree.ElementTree as ET
import glob

################################################################################
# ENVIRONMENT VARIABLES
################################################################################

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
AEROSPIKE_C_HOME = os.path.join(BASE_PATH, 'aerospike-client-c')
AEROSPIKE_C_TARGET = None

PLATFORM = platform.platform(1)
LINUX = 'Linux' in PLATFORM
DARWIN = 'Darwin' in PLATFORM or 'macOS' in PLATFORM
WINDOWS = 'Windows' in PLATFORM

CWD = os.path.abspath(os.path.dirname(__file__))

# COVERAGE environment variable only meant for CI/CD workflow to generate C coverage data
# Not for developers to use, unless you know what the workflow is doing!
COVERAGE = os.getenv('COVERAGE')

# Applies no optimizations on both the C client and Python client
UNOPTIMIZED = os.getenv('UNOPTIMIZED')

# Include debug information on macOS (not included by default)
INCLUDE_DSYM = os.getenv('INCLUDE_DSYM')


machine = platform.machine()
if DARWIN:
    os.putenv('ARCHFLAGS', '-arch ' + machine)
    os.environ['ARCHFLAGS'] = '-arch ' + machine

################################################################################
# GENERIC BUILD SETTINGS
################################################################################

include_dirs = [
    'src/include',
    f'{AEROSPIKE_C_HOME}/src/include',
    'aerospike-client-c/modules/common/src/include'
]

# TODO: use CMake to generate compiler-independent flags?
if WINDOWS:
    extra_compile_args = []
else:
    extra_compile_args = [
        '-std=gnu99',
        '-g',
        '-Wall',
        '-fPIC',
        '-O1',
        '-fno-common',
        '-fno-strict-aliasing',
        '-D_FILE_OFFSET_BITS=64',
        '-D_REENTRANT',
        '-Wno-strict-prototypes'
    ]
    if machine == 'x86_64':
        extra_compile_args.append('-march=nocona')

extra_link_args = []

SANITIZER = os.getenv('SANITIZER')
if SANITIZER:
    SANITIZER_C_AND_LD_FLAGS = [
        '-fsanitize=address',
        '-fno-omit-frame-pointer'
    ]
    SANITIZER_CFLAGS = SANITIZER_C_AND_LD_FLAGS.copy()
    SANITIZER_CFLAGS.append('-fsanitize-recover=all')
    extra_compile_args.extend(SANITIZER_CFLAGS)

    SANITIZER_LDFLAGS = SANITIZER_C_AND_LD_FLAGS.copy()
    extra_link_args.extend(SANITIZER_LDFLAGS)

if not WINDOWS:
    # The Python client's C code doesn't rely on these libraries.
    # But the C client does rely on them, and we need to bundle these libraries
    # with the Python client wheel so users don't need to install the libraries locally.
    #
    # When repairing the wheel, we will copy these libraries into the wheel
    # and change the Python client shared library's RPATH to point to these bundled libraries.
    #
    # The repair step only copies the libraries that the Python client are linked against
    # TODO: only change rpath of c client, not python client?
    c_client_libraries = [
        'm',
        'z',
        'yaml',
        'ssl',
        'crypto',
        'pthread'
    ]
else:
    c_client_libraries = []

##########################
# GITHUB ACTIONS SETTINGS
##########################

if COVERAGE:
    extra_compile_args.append('-fprofile-arcs')
    extra_compile_args.append('-ftest-coverage')
    extra_link_args.append('-lgcov')

if UNOPTIMIZED:
    extra_compile_args.append('-O0')

################################################################################
# STATIC SSL LINKING BUILD SETTINGS
################################################################################

extra_objects = []

STATIC_SSL = os.getenv('STATIC_SSL')
if STATIC_SSL:
    # Statically link openssl
    SSL_LIB_PATH = os.getenv('SSL_LIB_PATH')
    extra_objects.append(SSL_LIB_PATH + 'libssl.a')
    extra_objects.append(SSL_LIB_PATH + 'libcrypto.a')
else:
    # Dynamically link openssl
    c_client_libraries.append('ssl')
    c_client_libraries.append('crypto')

################################################################################
# PLATFORM SPECIFIC BUILD SETTINGS
################################################################################

if WINDOWS:
    AEROSPIKE_C_TARGET = AEROSPIKE_C_HOME
    tree = ET.parse(f"{AEROSPIKE_C_TARGET}/vs/aerospike/packages.config")
    packages = tree.getroot()
    package = packages[0]
    c_client_dependencies_version = package.attrib["version"]

if DARWIN:
    # ---------------------------------------------------------------------------
    # Mac Specific Compiler and Linker Settings
    # ---------------------------------------------------------------------------
    extra_compile_args.append('-D_DARWIN_UNLIMITED_SELECT')

    AEROSPIKE_C_TARGET = AEROSPIKE_C_HOME + '/target/Darwin-' + machine

elif LINUX:
    # ---------------------------------------------------------------------------
    # Linux Specific Compiler and Linker Settings
    # ---------------------------------------------------------------------------
    extra_compile_args.append('-rdynamic')
    extra_compile_args.append('-finline-functions')

    c_client_libraries.append('rt')
    AEROSPIKE_C_TARGET = AEROSPIKE_C_HOME + '/target/Linux-' + machine
elif WINDOWS:
    c_client_libraries.append("pthreadVC2")
    extra_compile_args.append("-DAS_SHARED_IMPORT")
else:
    print("error: OS not supported:", PLATFORM, file=sys.stderr)
    sys.exit(8)

# We don't specify common places where users may install their own build of OpenSSL (that doesn't come with the distro);
# there would be too many places to check,
# and those places may not even exist for other users on different platforms.
# Users who build the sdist should specify the OpenSSL location using env vars
library_dirs = []

if not WINDOWS:
    extra_objects.append(AEROSPIKE_C_TARGET + '/lib/libaerospike.a')
else:
    library_dirs.append(f"{AEROSPIKE_C_TARGET}/vs/packages/aerospike-client-c-dependencies.\
                        {c_client_dependencies_version}/build/native/lib/x64/Release")
    # Needed for linking the Python client with the C client
    extra_objects.append(AEROSPIKE_C_TARGET + "/vs/x64/Release/aerospike.lib")

os.putenv('CPATH', ':'.join(include_dirs))
os.environ['CPATH'] = ':'.join(include_dirs)

################################################################################
# SETUP
################################################################################


class BuildAerospikeModule(build_ext):

    def run(self):
        # TODO: not sure if force needed if building in isolated temp venv?
        if self.force == 1:
            # run original c-extension clean task
            # clean.run(self)
            cmd = [
                'make',
                'clean'
            ]

            def clean():
                subprocess.run(cmd, cwd=AEROSPIKE_C_HOME)

            self.execute(clean, [], 'Clean core aerospike-client-c previous builds')

        # TODO: not sure if we need?
        # if LINUX:
        #     os.putenv('LD_LIBRARY_PATH', ':'.join(library_dirs))
        #     os.environ['LD_LIBRARY_PATH'] = ':'.join(library_dirs)
        # elif DARWIN:
        #     os.putenv('DYLD_LIBRARY_PATH', ':'.join(library_dirs))
        #     os.environ['DYLD_LIBRARY_PATH'] = ':'.join(library_dirs)

        # build core client
        if WINDOWS:
            cmd = [
                'msbuild',
                'vs/aerospike.sln',
                '/property:Configuration=Release',
                # Don't build the examples
                '/t:aerospike'
            ]
        else:
            cmd = [
                'make',
                'V=' + str(self.verbose),
            ]
            if UNOPTIMIZED:
                cmd.append('O=0')
            if SANITIZER:
                ext_cflags = " ".join(SANITIZER_CFLAGS)
                cmd.append(f"EXT_CFLAGS={ext_cflags}")
                ldflags = " ".join(SANITIZER_LDFLAGS)
                cmd.append(f"LDFLAGS={ldflags}")

        print(cmd, library_dirs, c_client_libraries)
        subprocess.run(cmd, cwd=AEROSPIKE_C_HOME, check=True)

        # run original c-extension build code
        build_ext.run(self)

        # For debugging in macOS, we need to generate and include the debug info for the CPython
        # extension in the wheel, since this isn't done automatically
        if DARWIN and INCLUDE_DSYM:
            print("Generating debug information on macOS")
            shared_library_paths = glob.glob(pathname="**/aerospike*.so", recursive=True)

            # Sanity check
            print(f"List of shared libraries: {shared_library_paths}")
            if len(shared_library_paths) > 1:
                print("error: only one shared library should be present.", file=sys.stderr)
                exit(1)

            shared_library_path = shared_library_paths[0]
            subprocess.run(["dsymutil", shared_library_path], check=True)

            dsym_path = f"{shared_library_path}.dSYM"
            print("Including debug information with wheel")
            extra_objects.append(dsym_path)


class CleanAerospikeModule(clean):

    def run(self):
        # run original c-extension clean task
        # clean.run(self)
        cmd = [
            'make',
            'clean'
        ]

        def clean():
            subprocess.run(cmd, cwd=AEROSPIKE_C_HOME)

        self.execute(clean, [], 'Clean core aerospike-client-c')


source_files = glob.glob(pathname="src/main/**/*.c", recursive=True)

setup(
    # Data files
    ext_modules=[
        Extension(
            name='aerospike',

            # Compile
            sources=source_files,
            include_dirs=include_dirs,
            extra_compile_args=extra_compile_args,

            # Link
            library_dirs=library_dirs,
            libraries=c_client_libraries,
            extra_objects=extra_objects,
            extra_link_args=extra_link_args,
        )
    ],
    cmdclass={
        'build_ext': BuildAerospikeModule,
        'clean': CleanAerospikeModule
    }
)

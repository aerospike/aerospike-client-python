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

from __future__ import print_function
import os
import platform
import sys
import subprocess
from setuptools import setup, Extension
from distutils.command.build import build
from distutils.command.clean import clean
import io
import glob

################################################################################
# ENVIRONMENT VARIABLES
################################################################################
machine = platform.machine()
os.putenv('ARCHFLAGS', '-arch ' + machine)
os.environ['ARCHFLAGS'] = '-arch ' + machine
BASEPATH = os.path.dirname(os.path.abspath(__file__))
AEROSPIKE_C_HOME = os.path.join(BASEPATH, 'aerospike-client-c')

AEROSPIKE_C_TARGET = None
PLATFORM = platform.platform(1)
LINUX = 'Linux' in PLATFORM
DARWIN = 'Darwin' in PLATFORM or 'macOS' in PLATFORM
CWD = os.path.abspath(os.path.dirname(__file__))

SSL_LIB_PATH = os.getenv('SSL_LIB_PATH')
EVENT_LIB = os.getenv('EVENT_LIB')

################################################################################
# GENERIC BUILD SETTINGS
################################################################################

include_dirs = [
    'src/include',
    '/usr/local/opt/openssl/include',
    'aerospike-client-c/modules/common/src/include'
]
include_dirs.extend([x for x in os.getenv('CPATH', '').split(':') if len(x) > 0])

extra_compile_args = [
    '-std=gnu99',
    '-g',
    '-Wall',
    '-fPIC',
    '-O1',
    '-DDEBUG',
    '-fno-common',
    '-fno-strict-aliasing',
    '-Wno-strict-prototypes',
    '-D_FILE_OFFSET_BITS=64',
    '-D_REENTRANT',
    '-DMARCH_' + machine,
    '-Wno-implicit-function-declaration'
]

if machine == 'x86_64':
    extra_compile_args.append('-march=nocona')
extra_objects = []
extra_link_args = []
library_dirs = [
    '/usr/local/lib'
]
libraries = [
    'pthread',
    'm',
    'z'
]

################################################################################
# STATIC SSL LINKING BUILD SETTINGS
################################################################################

STATIC_SSL = os.getenv('STATIC_SSL')

if STATIC_SSL:
    # Statically link openssl
    extra_objects.append(SSL_LIB_PATH + 'libssl.a')
    extra_objects.append(SSL_LIB_PATH + 'libcrypto.a')
else:
    # Dynamically link openssl
    libraries.append('ssl')
    libraries.append('crypto')
    library_dirs.append('/usr/local/opt/openssl/lib')

################################################################################
# PLATFORM SPECIFIC BUILD SETTINGS
################################################################################

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

    libraries.append('rt')
    AEROSPIKE_C_TARGET = AEROSPIKE_C_HOME + '/target/Linux-' + machine
else:
    print("error: OS not supported:", PLATFORM, file=sys.stderr)
    sys.exit(8)

include_dirs.append('/usr/local/opt/openssl/include')
include_dirs.append(AEROSPIKE_C_TARGET + '/include')

extra_objects.append(AEROSPIKE_C_TARGET + '/lib/libaerospike.a')

os.putenv('CPATH', ':'.join(include_dirs))
os.environ['CPATH'] = ':'.join(include_dirs)

################################################################################
# SETUP
################################################################################

# Get the long description from the relevant file
with io.open(os.path.join(CWD, 'README.rst'), "r", encoding='utf-8') as f:
    long_description = f.read()

# Get the version from the relevant file
with io.open(os.path.join(CWD, 'VERSION'), "r", encoding='utf-8') as f:
    version = f.read()

BASEPATH = os.path.dirname(os.path.abspath(__file__))
CCLIENT_PATH = os.path.join(BASEPATH, 'aerospike-client-c')

if EVENT_LIB == "libuv":
    extra_compile_args.append('-DAS_EVENT_LIB_DEFINED')
    library_dirs.append('/usr/local/lib/')
    libraries.append('uv')
elif EVENT_LIB == "libevent":
    extra_compile_args.append('-DAS_EVENT_LIB_DEFINED')
    library_dirs.append('/usr/local/lib/')
    libraries.append('event_core')
    libraries.append('event_pthreads')
elif EVENT_LIB == "libev":
    extra_compile_args.append('-DAS_EVENT_LIB_DEFINED')
    library_dirs.append('/usr/local/lib/')
    libraries.append('ev')
else:
    print("Building aerospike with no-async support\n")


class CClientBuild(build):

    def run(self):
        if self.force == 1:
            # run original c-extension clean task
            # clean.run(self)
            cmd = [
                'make',
                'clean'
            ]

            def clean():
                subprocess.run(cmd, cwd=CCLIENT_PATH)

            self.execute(clean, [], 'Clean core aerospike-client-c previous builds')

        os.putenv('LD_LIBRARY_PATH', ':'.join(library_dirs))
        os.environ['LD_LIBRARY_PATH'] = ':'.join(library_dirs)
        os.putenv('DYLD_LIBRARY_PATH', ':'.join(library_dirs))
        os.environ['DYLD_LIBRARY_PATH'] = ':'.join(library_dirs)

        # build core client
        cmd = [
            'make',
            'V=' + str(self.verbose),
        ]
        if EVENT_LIB is not None:
            cmd.append('EVENT_LIB=' + EVENT_LIB)

        def compile():
            print(cmd, library_dirs, libraries)
            subprocess.run(cmd, cwd=CCLIENT_PATH)

        self.execute(compile, [], 'Compiling core aerospike-client-c')
        # run original c-extension build code
        build.run(self)


class CClientClean(clean):

    def run(self):
        # run original c-extension clean task
        # clean.run(self)
        cmd = [
            'make',
            'clean'
        ]

        def clean():
            subprocess.run(cmd, cwd=CCLIENT_PATH)

        self.execute(clean, [], 'Clean core aerospike-client-c')


# Get all C source files in src/main
src_files = glob.glob("src/main/**/*.c", recursive=True)

setup(
    version=version.strip(),
    # Data files
    ext_modules=[
        Extension(
            name='aerospike',
            sources=src_files,

            # Compile
            include_dirs=include_dirs,
            extra_compile_args=extra_compile_args,

            # Link
            library_dirs=library_dirs,
            libraries=libraries,
            extra_objects=extra_objects,
            extra_link_args=extra_link_args,
        )
    ],
    packages=[
        'aerospike_helpers',
        'aerospike_helpers.operations',
        'aerospike_helpers.batch',
        'aerospike_helpers.expressions',
        'aerospike_helpers.awaitable'
    ],

    cmdclass={
        'build': CClientBuild,
        'clean': CClientClean
    }
)

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
from subprocess import Popen
from subprocess import call
from setuptools import setup, Extension
from distutils.command.build import build
from distutils.command.clean import clean
from multiprocessing import cpu_count
import time
import io

################################################################################
# ENVIRONMENT VARIABLES
################################################################################

os.putenv('ARCHFLAGS', '-arch x86_64')
os.environ['ARCHFLAGS'] = '-arch x86_64'
AEROSPIKE_C_VERSION = os.getenv('AEROSPIKE_C_VERSION')
BASEPATH = os.path.dirname(os.path.abspath(__file__))
AEROSPIKE_C_HOME = os.path.join(BASEPATH, 'aerospike-client-c')

AEROSPIKE_C_TARGET = None
PLATFORM = platform.platform(1)
LINUX = 'Linux' in PLATFORM
DARWIN = 'Darwin' in PLATFORM or 'macOS' in PLATFORM
CWD = os.path.abspath(os.path.dirname(__file__))
STATIC_SSL = os.getenv('STATIC_SSL')
SSL_LIB_PATH = os.getenv('SSL_LIB_PATH')
EVENT_LIB = os.getenv('EVENT_LIB')

################################################################################
# GENERIC BUILD SETTINGS
################################################################################

include_dirs = ['src/include'] + \
    [x for x in os.getenv('CPATH', '').split(':') if len(x) > 0] + \
    ['/usr/local/opt/openssl/include'] + \
    ['aerospike-client-c/modules/common/src/include']
extra_compile_args = [
    '-std=gnu99', '-g', '-Wall', '-fPIC', '-O1', '-DDEBUG',
    '-fno-common', '-fno-strict-aliasing', '-Wno-strict-prototypes',
    '-march=nocona',
    '-D_FILE_OFFSET_BITS=64', '-D_REENTRANT',
    '-DMARCH_x86_64',
    '-Wno-implicit-function-declaration'
]
extra_objects = []
extra_link_args = []
library_dirs = ['/usr/local/opt/openssl/lib', '/usr/local/lib']
libraries = [
    'ssl',
    'crypto',
    'pthread',
    'm',
    'z'
]
################################################################################
# STATIC SSL LINKING BUILD SETTINGS
################################################################################

if STATIC_SSL:
    extra_objects.extend(
        [SSL_LIB_PATH + 'libssl.a', SSL_LIB_PATH + 'libcrypto.a'])
    libraries.remove('ssl')
    libraries.remove('crypto')
    library_dirs.remove('/usr/local/opt/openssl/lib')

################################################################################
# PLATFORM SPECIFIC BUILD SETTINGS
################################################################################

if DARWIN:
    # ---------------------------------------------------------------------------
    # Mac Specific Compiler and Linker Settings
    # ---------------------------------------------------------------------------
    extra_compile_args = extra_compile_args + [
        '-D_DARWIN_UNLIMITED_SELECT'
    ]

    AEROSPIKE_C_TARGET = AEROSPIKE_C_HOME + '/target/Darwin-x86_64'

elif LINUX:
    # ---------------------------------------------------------------------------
    # Linux Specific Compiler and Linker Settings
    # ---------------------------------------------------------------------------
    extra_compile_args = extra_compile_args + [
        '-rdynamic', '-finline-functions'
    ]
    libraries = libraries + ['rt']
    AEROSPIKE_C_TARGET = AEROSPIKE_C_HOME + '/target/Linux-x86_64'
else:
    print("error: OS not supported:", PLATFORM, file=sys.stderr)
    sys.exit(8)

include_dirs = include_dirs + [
    '/usr/local/opt/openssl/include',
    AEROSPIKE_C_TARGET + '/include'
    ]
extra_objects = extra_objects + [
    AEROSPIKE_C_TARGET + '/lib/libaerospike.a'
]

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

# if EVENT_LIB is None or EVENT_LIB == "":
#     EVENT_LIB = "libevent"

if EVENT_LIB is not None:
    if EVENT_LIB == "libuv":
        extra_compile_args = extra_compile_args + ['-DAS_EVENT_LIB_DEFINED']
        library_dirs = library_dirs + ['/usr/local/lib/']
        libraries = libraries + ['uv']
    elif EVENT_LIB == "libevent":
        extra_compile_args = extra_compile_args + ['-DAS_EVENT_LIB_DEFINED']
        library_dirs = library_dirs + ['/usr/local/lib/']
        libraries = libraries + ['event_core', 'event_pthreads']
    elif EVENT_LIB == "libev":
        extra_compile_args = extra_compile_args + ['-DAS_EVENT_LIB_DEFINED']
        library_dirs = library_dirs + ['/usr/local/lib/']
        libraries = libraries + ['ev']
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
                call(cmd, cwd=CCLIENT_PATH)
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
            cmd = [
                'make',
                'V=' + str(self.verbose),
                'EVENT_LIB='+EVENT_LIB] 

        def compile():
            print(cmd, library_dirs, libraries)
            call(cmd, cwd=CCLIENT_PATH)

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
            call(cmd, cwd=CCLIENT_PATH)

        self.execute(clean, [], 'Clean core aerospike-client-c')


setup(
    name='aerospike',
    version=version.strip(),
    description='Aerospike Client Library for Python',
    long_description=long_description,
    author='Aerospike, Inc.',
    author_email='info@aerospike.com',
    url='http://aerospike.com',
    license='Apache Software License',
    keywords=['aerospike', 'nosql', 'database'],
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Database'
    ],

    # Package Data Files
    zip_safe=False,
    include_package_data=True,

    # Data files
    ext_modules=[
        Extension(
            # Extension Name
            'aerospike',

            # Source Files
            [
                'src/main/aerospike.c',
                'src/main/exception.c',
                'src/main/log.c',
                'src/main/client/type.c',
                'src/main/client/apply.c',
                'src/main/client/bit_operate.c',
                'src/main/client/cdt_list_operate.c',
                'src/main/client/cdt_map_operate.c',
                'src/main/client/hll_operate.c',
                'src/main/client/expression_operations.c',
                'src/main/client/cdt_operation_utils.c',
                'src/main/client/close.c',
                'src/main/client/connect.c',
                'src/main/client/exists.c',
                'src/main/client/exists_many.c',
                'src/main/client/get.c',
                'src/main/client/get_async.c',
                'src/main/client/put_async.c',
                'src/main/client/get_many.c',
                'src/main/client/batch_get_ops.c',
                'src/main/client/select_many.c',
                'src/main/client/info_single_node.c',
                'src/main/client/info_random_node.c',
                'src/main/client/info_node.c',
                'src/main/client/info.c',
                'src/main/client/put.c',
                'src/main/client/operate_list.c',
                'src/main/client/operate_map.c',
                'src/main/client/operate.c',
                'src/main/client/query.c',
                'src/main/client/remove.c',
                'src/main/client/scan.c',
                'src/main/client/select.c',
                'src/main/client/tls_info_host.c',
                'src/main/client/truncate.c',
                'src/main/client/admin.c',
                'src/main/client/udf.c',
                'src/main/client/sec_index.c',
                'src/main/serializer.c',
                'src/main/client/remove_bin.c',
                'src/main/client/get_key_digest.c',
                'src/main/query/type.c',
                'src/main/query/apply.c',
                'src/main/query/add_ops.c',
                'src/main/query/foreach.c',
                'src/main/query/predexp.c',
                'src/main/query/results.c',
                'src/main/query/select.c',
                'src/main/query/where.c',
                'src/main/query/execute_background.c',
                'src/main/scan/type.c',
                'src/main/scan/foreach.c',
                'src/main/scan/results.c',
                'src/main/scan/select.c',
                'src/main/scan/execute_background.c',
                'src/main/scan/apply.c',
                'src/main/scan/add_ops.c',
                'src/main/scan/paginate.c',
                'src/main/geospatial/type.c',
                'src/main/geospatial/wrap.c',
                'src/main/geospatial/unwrap.c',
                'src/main/geospatial/loads.c',
                'src/main/geospatial/dumps.c',
                'src/main/policy.c',
                'src/main/conversions.c',
                'src/main/convert_predexp.c',
                'src/main/convert_expressions.c',
                'src/main/policy_config.c',
                'src/main/calc_digest.c',
                'src/main/predicates.c',
                'src/main/tls_config.c',
                'src/main/global_hosts/type.c',
                'src/main/nullobject/type.c',
                'src/main/cdt_types/type.c',
                'src/main/key_ordered_dict/type.c',
                'src/main/client/set_xdr_filter.c',
                'src/main/client/get_nodes.c',
                'src/main/convert_partition_filter.c',
                'src/main/client/get_key_partition_id.c'
            ],

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
    packages=['aerospike_helpers', 'aerospike_helpers.operations',
              'aerospike_helpers.expressions', 'aerospike_helpers.awaitable'],

    cmdclass={
        'build': CClientBuild,
        'clean': CClientClean
    }
)

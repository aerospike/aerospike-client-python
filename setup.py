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
from subprocess import call, run
from setuptools import setup, Extension
from distutils.command.build import build
from distutils.command.clean import clean
from multiprocessing import cpu_count
import time
import io
import xml.etree.ElementTree as ET
import glob

################################################################################
# ENVIRONMENT VARIABLES
################################################################################
machine = platform.machine()
os.putenv('ARCHFLAGS', '-arch ' + machine)
os.environ['ARCHFLAGS'] = '-arch ' + machine
AEROSPIKE_C_VERSION = os.getenv('AEROSPIKE_C_VERSION')
BASEPATH = os.path.dirname(os.path.abspath(__file__))
AEROSPIKE_C_HOME = os.path.join(BASEPATH, 'aerospike-client-c')

AEROSPIKE_C_TARGET = None
PLATFORM = platform.platform(1)
LINUX = 'Linux' in PLATFORM
DARWIN = 'Darwin' in PLATFORM or 'macOS' in PLATFORM
WINDOWS = 'Windows' in PLATFORM

CWD = os.path.abspath(os.path.dirname(__file__))
STATIC_SSL = os.getenv('STATIC_SSL')
SSL_LIB_PATH = os.getenv('SSL_LIB_PATH')
# COVERAGE environment variable only meant for CI/CD workflow to generate C coverage data
# Not for developers to use, unless you know what the workflow is doing!
COVERAGE = os.getenv('COVERAGE')

# Applies no optimizations on both the C client and Python client
UNOPTIMIZED = os.getenv('UNOPTIMIZED')

# Include debug information on macOS (not included by default)
INCLUDE_DSYM = os.getenv('INCLUDE_DSYM')

################################################################################
# GENERIC BUILD SETTINGS
################################################################################

include_dirs = ['src/include'] + \
    [x for x in os.getenv('CPATH', '').split(':') if len(x) > 0] + \
    ['/usr/local/opt/openssl/include'] + \
    ['aerospike-client-c/modules/common/src/include']
extra_compile_args = [
    '-std=gnu99', '-g', '-Wall', '-fPIC', '-DDEBUG', '-O1',
    '-fno-common', '-fno-strict-aliasing',
    '-D_FILE_OFFSET_BITS=64', '-D_REENTRANT',
    '-DMARCH_' + machine,
]

if not WINDOWS:
    # Windows does not have this flag
    extra_compile_args.append("-Wno-strict-prototypes")
    extra_compile_args.append('-Wno-implicit-function-declaration')

if machine == 'x86_64':
    extra_compile_args.append('-march=nocona')
extra_objects = []

extra_link_args = []

SANITIZER=os.getenv('SANITIZER')
if SANITIZER:
    sanitizer_c_and_ld_flags = [
        '-fsanitize=address',
        '-fno-omit-frame-pointer'
    ]
    sanitizer_cflags = sanitizer_c_and_ld_flags.copy()
    sanitizer_cflags.append('-fsanitize-recover=all')
    extra_compile_args.extend(sanitizer_cflags)

    sanitizer_ldflags = sanitizer_c_and_ld_flags.copy()
    extra_link_args.extend(sanitizer_ldflags)

library_dirs = ['/usr/local/opt/openssl/lib', '/usr/local/lib']
libraries = [
    'ssl',
    'crypto',
    'pthread',
    'm',
    'z'
]

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

if STATIC_SSL:
    extra_objects.extend(
        [SSL_LIB_PATH + 'libssl.a', SSL_LIB_PATH + 'libcrypto.a'])
    libraries.remove('ssl')
    libraries.remove('crypto')
    library_dirs.remove('/usr/local/opt/openssl/lib')

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
    extra_compile_args = extra_compile_args + [
        '-D_DARWIN_UNLIMITED_SELECT'
    ]

    AEROSPIKE_C_TARGET = AEROSPIKE_C_HOME + '/target/Darwin-' + machine

elif LINUX:
    # ---------------------------------------------------------------------------
    # Linux Specific Compiler and Linker Settings
    # ---------------------------------------------------------------------------
    extra_compile_args = extra_compile_args + [
        '-rdynamic', '-finline-functions'
    ]
    libraries = libraries + ['rt']
    AEROSPIKE_C_TARGET = AEROSPIKE_C_HOME + '/target/Linux-' + machine
elif WINDOWS:
    libraries.clear()
    extra_compile_args.append("-DAS_SHARED_IMPORT")
    include_dirs.append(f"{AEROSPIKE_C_TARGET}/vs/packages/aerospike-client-c-dependencies.{c_client_dependencies_version}/build/native/include")
else:
    print("error: OS not supported:", PLATFORM, file=sys.stderr)
    sys.exit(8)

include_dirs = include_dirs + [
    '/usr/local/opt/openssl/include',

]
if not WINDOWS:
    include_dirs.append(AEROSPIKE_C_TARGET + '/include')
    extra_objects = extra_objects + [
        AEROSPIKE_C_TARGET + '/lib/libaerospike.a'
    ]
else:
    include_dirs.append(AEROSPIKE_C_TARGET + '/src/include')
    library_dirs.append(f"{AEROSPIKE_C_TARGET}/vs/packages/aerospike-client-c-dependencies.{c_client_dependencies_version}/build/native/lib/x64/Release")
    # Needed for linking the Python client with the C client
    extra_objects.append(AEROSPIKE_C_TARGET + "/vs/x64/Release/aerospike.lib")

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
        if WINDOWS:
            cmd = [
                'msbuild',
                'vs/aerospike.sln',
                '/property:Configuration=Release'
            ]
        else:
            cmd = [
                'make',
                'V=' + str(self.verbose),
            ]
            if UNOPTIMIZED:
                cmd.append('O=0')
            if SANITIZER:
                ext_cflags = " ".join(sanitizer_cflags)
                cmd.append(f"EXT_CFLAGS={ext_cflags}")
                ldflags = " ".join(sanitizer_ldflags)
                cmd.append(f"LDFLAGS={ldflags}")

        def compile():
            print(cmd, library_dirs, libraries)
            call(cmd, cwd=CCLIENT_PATH)

        self.execute(compile, [], 'Compiling core aerospike-client-c')
        # run original c-extension build code
        build.run(self)

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
            run(["dsymutil", shared_library_path], check=True)

            dsym_path = f"{shared_library_path}.dSYM"
            print("Including debug information with wheel")
            extra_objects.append(dsym_path)

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
    version=version.strip(),
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
                'src/main/client/get_many.c',
                'src/main/client/batch_get_ops.c',
                'src/main/client/select_many.c',
                'src/main/client/info_single_node.c',
                'src/main/client/info_random_node.c',
                'src/main/client/info.c',
                'src/main/client/put.c',
                'src/main/client/operate.c',
                'src/main/client/query.c',
                'src/main/client/remove.c',
                'src/main/client/scan.c',
                'src/main/client/select.c',
                'src/main/client/truncate.c',
                'src/main/client/admin.c',
                'src/main/client/udf.c',
                'src/main/client/sec_index.c',
                'src/main/serializer.c',
                'src/main/client/remove_bin.c',
                'src/main/query/type.c',
                'src/main/query/apply.c',
                'src/main/query/add_ops.c',
                'src/main/query/paginate.c',
                'src/main/query/get_parts.c',
                'src/main/query/foreach.c',
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
                'src/main/scan/get_parts.c',
                'src/main/geospatial/type.c',
                'src/main/geospatial/wrap.c',
                'src/main/geospatial/unwrap.c',
                'src/main/geospatial/loads.c',
                'src/main/geospatial/dumps.c',
                'src/main/policy.c',
                'src/main/conversions.c',
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
                'src/main/client/get_expression_base64.c',
                'src/main/client/get_cdtctx_base64.c',
                'src/main/client/get_nodes.c',
                'src/main/convert_partition_filter.c',
                'src/main/client/get_key_partition_id.c',
                'src/main/client/batch_write.c',
                'src/main/client/batch_operate.c',
                'src/main/client/batch_remove.c',
                'src/main/client/batch_apply.c',
                'src/main/client/batch_read.c',
                'src/main/client/metrics.c',
                'src/main/transaction/type.c',
                'src/main/client/mrt.c'
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
    package_data={
        "aerospike-stubs": [
            "__init__.pyi",
            "aerospike.pyi",
            "exception.pyi",
            "predicates.pyi",
        ]
    },
    packages=['aerospike_helpers', 'aerospike_helpers.operations', 'aerospike_helpers.batch',
              'aerospike_helpers.expressions',
              'aerospike_helpers.metrics',
              'aerospike-stubs'],
    cmdclass={
        'build': CClientBuild,
        'clean': CClientClean
    }
)

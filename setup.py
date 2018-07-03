# -*- coding: utf-8 -*-
################################################################################
# Copyright 2013-2017 Aerospike, Inc.
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
import errno
import os
import platform
import sys
from distutils.command.build import build
from setuptools.command.install import install
from distutils.sysconfig import get_config_vars
from setuptools import setup, Extension
from shutil import copytree, copy2
from subprocess import Popen


class InstallCommand(install):
    user_options = install.user_options + [
        ('lua-system-path=', None, 'Path to the lua system files')
    ]

    def initialize_options(self):
        install.initialize_options(self)
        self.lua_system_path = None

    def finalize_options(self):
        install.finalize_options(self)

    def run(self):
        global lua_system_path
        lua_system_path = self.lua_system_path
        install.run(self)


class BuildCommand(build):
    user_options = build.user_options + [
        ('lua-system-path=', None, 'Path to the lua system files')
    ]

    def initialize_options(self):
        build.initialize_options(self)
        self.lua_system_path = None

    def finalize_options(self):
        build.finalize_options(self)

    def run(self):
        global lua_system_path
        lua_system_path = self.lua_system_path
        build.run(self)


################################################################################
# ENVIRONMENT VARIABLES
################################################################################

os.putenv('ARCHFLAGS', '-arch x86_64')
os.environ['ARCHFLAGS'] = '-arch x86_64'
AEROSPIKE_C_VERSION = os.getenv('AEROSPIKE_C_VERSION')
if not AEROSPIKE_C_VERSION:
    AEROSPIKE_C_VERSION = '4.3.13'
DOWNLOAD_C_CLIENT = os.getenv('DOWNLOAD_C_CLIENT')
AEROSPIKE_C_HOME = os.getenv('AEROSPIKE_C_HOME')
PREFIX = None
PLATFORM = platform.platform(1)
LINUX = 'Linux' in PLATFORM
DARWIN = 'Darwin' in PLATFORM
CWD = os.path.abspath(os.path.dirname(__file__))

################################################################################
# HELPER FUNCTION FOR RESOLVING THE C CLIENT DEPENDENCY
################################################################################


def resolve_c_client(lua_src_path):
    global PREFIX, AEROSPIKE_C_VERSION, DOWNLOAD_C_CLIENT
    global extra_objects, include_dirs

    if PREFIX:
        os.putenv('PREFIX', PREFIX)
        os.environ['PREFIX'] = PREFIX
    if AEROSPIKE_C_VERSION:
        os.putenv('AEROSPIKE_C_VERSION', AEROSPIKE_C_VERSION)
        os.environ['AEROSPIKE_C_VERSION'] = AEROSPIKE_C_VERSION
    if DOWNLOAD_C_CLIENT:
        os.putenv('DOWNLOAD_C_CLIENT', DOWNLOAD_C_CLIENT)
        os.environ['DOWNLOAD_C_CLIENT'] = DOWNLOAD_C_CLIENT

    print('info: Executing', './scripts/aerospike-client-c.sh', file=sys.stdout)
    os.chmod('./scripts/aerospike-client-c.sh', 0o0755)
    os.chmod('./scripts/os_version', 0o0755)
    p = Popen(['./scripts/aerospike-client-c.sh'], env=os.environ)
    rc = p.wait()

    if rc != 0:
        print("error: scripts/aerospike-client-c.sh", rc, file=sys.stderr)
        sys.exit(1)

    # Prefix for Aerospike C client libraries and headers
    aerospike_c_prefix = './aerospike-client-c'
    if not os.path.isdir(aerospike_c_prefix):
        print("error: Directory not found:", aerospike_c_prefix, file=sys.stderr)
        sys.exit(2)

    # -------------------------------------------------------------------------------
    # Check for aerospike.h
    # -------------------------------------------------------------------------------
    aerospike_h = aerospike_c_prefix + '/include/aerospike/aerospike.h'
    if not os.path.isfile(aerospike_h):
        print("error: aerospike.h not found:", aerospike_h, file=sys.stderr)
        sys.exit(3)
    print("info: aerospike.h found:", aerospike_h, file=sys.stdout)
    include_dirs = include_dirs + [
        '/usr/local/opt/openssl/include',
        aerospike_c_prefix + '/include',
        aerospike_c_prefix + '/include/ck'
        ]

    # -------------------------------------------------------------------------------
    # Check for libaerospike.a
    # -------------------------------------------------------------------------------
    aerospike_a = aerospike_c_prefix + '/lib/libaerospike.a'
    if not os.path.isfile(aerospike_a):
        print("error: libaerospike.a not found:", aerospike_a, file=sys.stderr)
        sys.exit(4)
    print("info: libaerospike.a found:", aerospike_a, file=sys.stdout)
    extra_objects = extra_objects + [
        aerospike_a
        ]

    # ---------------------------------------------------------------------------
    # Environment Variables
    # ---------------------------------------------------------------------------
    os.putenv('CPATH', ':'.join(include_dirs))
    os.environ['CPATH'] = ':'.join(include_dirs)
    os.putenv('LD_LIBRARY_PATH', ':'.join(library_dirs))
    os.environ['LD_LIBRARY_PATH'] = ':'.join(library_dirs)
    os.putenv('DYLD_LIBRARY_PATH', ':'.join(library_dirs))
    os.environ['DYLD_LIBRARY_PATH'] = ':'.join(library_dirs)

    # ---------------------------------------------------------------------------
    # Deploying the system lua files
    # ---------------------------------------------------------------------------

################################################################################
# GENERIC BUILD SETTINGS
################################################################################

include_dirs = ['src/include'] + [x for x in os.getenv('CPATH', '').split(':') if len(x) > 0]
extra_compile_args = [
    '-std=gnu99', '-g', '-Wall', '-fPIC', '-O1',
    '-fno-common', '-fno-strict-aliasing', '-Wno-strict-prototypes',
    '-march=nocona',
    '-D_FILE_OFFSET_BITS=64', '-D_REENTRANT', '-D_GNU_SOURCE'
    ]
extra_objects = []
extra_link_args = []
library_dirs = ['/usr/local/opt/openssl/lib']
libraries = [
  'ssl',
  'crypto',
  'pthread',
  'm',
  'z'
  ]

################################################################################
# PLATFORM SPECIFIC BUILD SETTINGS
################################################################################

if DARWIN:
    # ---------------------------------------------------------------------------
    # Mac Specific Compiler and Linker Settings
    # ---------------------------------------------------------------------------
    extra_compile_args = extra_compile_args + [
        '-D_DARWIN_UNLIMITED_SELECT',
        '-DMARCH_x86_64'
        ]

    if AEROSPIKE_C_HOME:
        PREFIX = AEROSPIKE_C_HOME + '/target/Darwin-x86_64'

elif LINUX:
    # ---------------------------------------------------------------------------
    # Linux Specific Compiler and Linker Settings
    # ---------------------------------------------------------------------------
    extra_compile_args = extra_compile_args + [
        '-rdynamic', '-finline-functions',
        '-DMARCH_x86_64'
        ]
    libraries = libraries + ['rt']
    if AEROSPIKE_C_HOME:
        PREFIX = AEROSPIKE_C_HOME + '/target/Linux-x86_64'
else:
    print("error: OS not supported:", PLATFORM, file=sys.stderr)
    sys.exit(8)

################################################################################
# RESOLVE C CLIENT DEPENDENCY AND LUA SYSTEM PATH
################################################################################

lua_system_path = ''


# If the C client is packaged elsewhere, assume the libraries are available
lua_src_path = "modules/aerospike-lua-core/src"

if os.environ.get('NO_RESOLVE_C_CLIENT_DEP', None):
    has_c_client = True
    libraries = libraries + ['aerospike']
    lua_src_path = os.environ.get('AEROSPIKE_LUA_PATH', lua_src_path)
else:
    has_c_client = False

lua_files = [
                lua_src_path + '/aerospike.lua',
                lua_src_path + '/as.lua',
                lua_src_path + '/stream_ops.lua'
            ]

for file in lua_files:
    if not os.path.isfile(file):
        print("Warning: lua file {} not found, exiting".format(file), file=sys.stderr)
        sys.exit(4)


# If system-path isn't specified this will install relative to sys.exec_prefix
data_files = [
    ('aerospike/lua', lua_files)
]

if not has_c_client:
    if (('build' in sys.argv or 'build_ext' in sys.argv or
         'install' in sys.argv or 'bdist_wheel' in sys.argv)):
        resolve_c_client(lua_src_path)

################################################################################
# SETUP
################################################################################

# Get the long description from the relevant file
with open(os.path.join(CWD, 'README.rst')) as f:
    long_description = f.read()

# Get the version from the relevant file
with open(os.path.join(CWD, 'VERSION')) as f:
    version = f.read()

setup(
    cmdclass={
        'build': BuildCommand,
        'install': InstallCommand,
    },
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
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Database'
    ],

    # Package Data Files
    zip_safe=False,
    include_package_data=True,
    package_data={
        'aerospike': [
            lua_src_path + '/*.lua',
        ]
    },

    # Data files
    data_files=data_files,
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
                'src/main/client/cdt_list_operate.c',
                'src/main/client/close.c',
                'src/main/client/connect.c',
                'src/main/client/exists.c',
                'src/main/client/exists_many.c',
                'src/main/client/get.c',
                'src/main/client/get_many.c',
                'src/main/client/select_many.c',
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
                'src/main/query/foreach.c',
                'src/main/query/predexp.c',
                'src/main/query/results.c',
                'src/main/query/select.c',
                'src/main/query/where.c',
                'src/main/scan/type.c',
                'src/main/scan/foreach.c',
                'src/main/scan/results.c',
                'src/main/scan/select.c',
                'src/main/geospatial/type.c',
                'src/main/geospatial/wrap.c',
                'src/main/geospatial/unwrap.c',
                'src/main/geospatial/loads.c',
                'src/main/geospatial/dumps.c',
                'src/main/conversions.c',
                'src/main/policy.c',
                'src/main/policy_config.c',
                'src/main/calc_digest.c',
                'src/main/predicates.c',
                'src/main/tls_config.c',
                'src/main/global_hosts/type.c',
                'src/main/nullobject/type.c',
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
    packages=['aerospike_helpers', 'aerospike_helpers.operations']

)

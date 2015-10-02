# -*- coding: utf-8 -*-
################################################################################
# Copyright 2013-2014 Aerospike, Inc.
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

# from distutils.core import setup, Extension
from setuptools import setup, Extension
from os import path
from subprocess import call

################################################################################
# ENVIRONMENT VARIABLES
################################################################################

os.putenv('ARCHFLAGS','-arch x86_64')
os.environ['ARCHFLAGS'] = '-arch x86_64'
AEROSPIKE_C_VERSION = os.getenv('AEROSPIKE_C_VERSION')
if not AEROSPIKE_C_VERSION:
    AEROSPIKE_C_VERSION = '3.1.24'
DOWNLOAD_C_CLIENT = os.getenv('DOWNLOAD_C_CLIENT')
AEROSPIKE_C_HOME = os.getenv('AEROSPIKE_C_HOME')
PREFIX = None
PLATFORM =  platform.platform(1)
LINUX = 'Linux' in PLATFORM
DARWIN = 'Darwin' in PLATFORM
CWD = path.abspath(path.dirname(__file__))

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

library_dirs = []

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

    #---------------------------------------------------------------------------
    # Mac Specific Compiler and Linker Settings
    #---------------------------------------------------------------------------

    extra_compile_args = extra_compile_args + [
        '-D_DARWIN_UNLIMITED_SELECT',
        '-DMARCH_x86_64'
        ]

    if AEROSPIKE_C_HOME:
        PREFIX = AEROSPIKE_C_HOME + '/target/Darwin-x86_64'

elif LINUX:

    #---------------------------------------------------------------------------
    # Linux Specific Compiler and Linker Settings
    #---------------------------------------------------------------------------

    extra_compile_args = extra_compile_args + [
        '-rdynamic', '-finline-functions',
        '-DMARCH_x86_64'
        ]

    libraries = libraries + ['rt']

    if AEROSPIKE_C_HOME:
        PREFIX = AEROSPIKE_C_HOME + '/target/Linux-x86_64'

else:
    print("error: OS not supported:", PLATFORM, file=sys.stderr)
    sys.exit(1)


################################################################################
# RESOLVE C CLIENT DEPENDENCY
################################################################################

# If the C client is packaged elsewhere, assume the libraries are available
if os.environ.get('NO_RESOLVE_C_CLIENT_DEP', None):
    libraries = libraries + ['aerospike']
    # Can override the lua path
    lua_path = os.environ.get('AEROSPIKE_LUA_PATH', "/usr/local/share/aerospike/lua")
    data_files = [
        ('aerospike', []),
        ('aerospike/lua', [
            lua_path + '/aerospike.lua',
            lua_path + '/as.lua',
            lua_path + '/stream_ops.lua',
            ]
        )
    ]

else:
    lua_path = "aerospike-client-c/lua"

    data_files = [
        ('aerospike', []),
        ('aerospike/lua', [
            lua_path + '/aerospike.lua',
            lua_path + '/as.lua',
            lua_path + '/stream_ops.lua',
            ]
        )
    ]
    
    if 'build' in sys.argv or 'build_ext' in sys.argv or 'install' in sys.argv:

        # Prefix for Aerospike C client libraries and headers
        aerospike_c_prefix = './aerospike-client-c'

        #-------------------------------------------------------------------------------
        # Execute Aerospike C Client Resolver
        #-------------------------------------------------------------------------------

        print('info: Executing','./scripts/aerospike-client-c.sh', file=sys.stdout)

        os.chmod('./scripts/aerospike-client-c.sh',0755)

        if PREFIX:
            os.putenv('PREFIX', PREFIX)

        if AEROSPIKE_C_VERSION:
            os.putenv('AEROSPIKE_C_VERSION', AEROSPIKE_C_VERSION)

        if DOWNLOAD_C_CLIENT:
            os.putenv('DOWNLOAD_C_CLIENT', DOWNLOAD_C_CLIENT)

        rc = call(['./scripts/aerospike-client-c.sh'])
        if rc != 0 :
            print("error: scripts/aerospike-client-c.sh", rc, file=sys.stderr)
            sys.exit(1)


        if not os.path.isdir(aerospike_c_prefix):
            print("error: Directory not found:", aerospike_c_prefix, file=sys.stderr)
            sys.exit(1)

        #-------------------------------------------------------------------------------
        # Check for aerospike.h
        #-------------------------------------------------------------------------------

        aerospike_h = aerospike_c_prefix + '/include/aerospike/aerospike.h'

        if not os.path.isfile(aerospike_h):
            print("error: aerospike.h not found:", aerospike_h, file=sys.stderr)
            sys.exit(1)

        print("info: aerospike.h found:", aerospike_h, file=sys.stdout)

        include_dirs = [
            aerospike_c_prefix + '/include', 
            aerospike_c_prefix + '/include/ck'
            ] + include_dirs

        #-------------------------------------------------------------------------------
        # Check for libaerospike.a
        #-------------------------------------------------------------------------------

        aerospike_a = aerospike_c_prefix + '/lib/libaerospike.a'

        if not os.path.isfile(aerospike_a):
            print("error: libaerospike.a not found:", aerospike_a, file=sys.stderr)
            sys.exit(1)

        print("info: libaerospike.a found:", aerospike_a, file=sys.stdout)
        extra_objects = [
            aerospike_a
            ] + extra_objects

        #---------------------------------------------------------------------------
        # Environment Variables
        #---------------------------------------------------------------------------

        os.putenv('CPATH', ':'.join(include_dirs))
        os.putenv('LD_LIBRARY_PATH', ':'.join(library_dirs))
        os.putenv('DYLD_LIBRARY_PATH', ':'.join(library_dirs))


################################################################################
# SETUP
################################################################################

# Get the long description from the relevant file
with open(path.join(CWD, 'README.rst')) as f:
    long_description = f.read()

# Get the version from the relevant file
with open(path.join(CWD, 'VERSION')) as f:
    version = f.read()

setup(
    name = 'aerospike', 
    
    version = version.strip(), 

    description = 'Aerospike Client Library for Python',
    long_description = long_description,
    
    author = 'Aerospike, Inc.',
    author_email = 'info@aerospike.com',
    url = 'http://aerospike.com',

    license = 'Apache Software License',

    keywords = ['aerospike', 'nosql', 'database'],

    classifiers = [
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Database'
    ],


    zip_safe = False,

    include_package_data = True,

    # Package Data Files
    package_data = {
        'aerospike': [
            lua_path + '/*.lua',
        ]
    },

    # Data files
    data_files = data_files,

    eager_resources = [
        lua_path + '/aerospike.lua',
        lua_path + '/as.lua',
        lua_path + '/stream_ops.lua',
    ],

    ext_modules = [
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
                'src/main/client/close.c',
                'src/main/client/connect.c',
                'src/main/client/exists.c',
                'src/main/client/exists_many.c',
                'src/main/client/get.c',
                'src/main/client/get_many.c',
                'src/main/client/select_many.c',
                'src/main/client/info_node.c',
                'src/main/client/info.c',
                'src/main/client/key.c',
                'src/main/client/put.c',
                'src/main/client/operate.c',
                'src/main/client/query.c',
                'src/main/client/remove.c',
                'src/main/client/scan.c',
                'src/main/client/select.c',
                'src/main/client/admin.c',
                'src/main/client/udf.c',
                'src/main/client/sec_index.c',
                'src/main/serializer.c',
                'src/main/client/remove_bin.c',
                'src/main/client/get_key_digest.c',
                'src/main/client/lstack.c',
                'src/main/client/lset.c',
                'src/main/client/lmap.c',
                'src/main/client/llist.c',
                'src/main/key/type.c',
                'src/main/key/apply.c',
                'src/main/key/exists.c',
                'src/main/key/get.c',
                'src/main/key/put.c',
                'src/main/key/remove.c',
                'src/main/query/type.c',
                'src/main/query/apply.c',
                'src/main/query/foreach.c',
                'src/main/query/results.c',
                'src/main/query/select.c',
                'src/main/query/where.c',
                'src/main/scan/type.c',
                'src/main/scan/foreach.c',
                'src/main/scan/results.c',
                'src/main/scan/select.c',
                'src/main/lstack/type.c',
                'src/main/lstack/lstack_operations.c',
                'src/main/lset/type.c',
                'src/main/lset/lset_operations.c',
                'src/main/llist/type.c',
                'src/main/llist/llist_operations.c',
                'src/main/lmap/type.c',
                'src/main/lmap/lmap_operations.c',
                'src/main/geospatial/type.c',
                'src/main/geospatial/wrap.c',
                'src/main/geospatial/unwrap.c',
                'src/main/geospatial/loads.c',
                'src/main/geospatial/dumps.c',
                'src/main/conversions.c',
                'src/main/policy.c',
                'src/main/predicates.c'
            ],

            # Compile
            include_dirs = include_dirs,
            extra_compile_args = extra_compile_args,

            # Link
            library_dirs = library_dirs,
            libraries = libraries,
            extra_objects = extra_objects,
            extra_link_args = extra_link_args,
        )
    ]
  )


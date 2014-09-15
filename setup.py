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

AEROSPIKE_C_VERSION = os.getenv('AEROSPIKE_C_VERSION')
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
    '-fno-common', '-fno-strict-aliasing', '-finline-functions', 
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
  'm'
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
        '-undefined','dynamic_lookup',
        '-DLUA_DEBUG_HOOK',
        '-DMARCH_x86_64'
        ]
    
    libraries = libraries + ['lua']

    if AEROSPIKE_C_HOME:
        PREFIX = AEROSPIKE_C_HOME + '/target/Darwin-x86_64'

elif LINUX:

    #---------------------------------------------------------------------------
    # Linux Specific Compiler and Linker Settings
    #---------------------------------------------------------------------------

    extra_compile_args = extra_compile_args + [
        '-rdynamic',
        '-DMARCH_x86_64'
        ]

    libraries = libraries + ['rt']

    if AEROSPIKE_C_HOME:
        PREFIX = AEROSPIKE_C_HOME + '/target/Linux-x86_64'

    #---------------------------------------------------------------------------
    # The following will attempt to resolve the Lua 5.1 library dependency
    #---------------------------------------------------------------------------

    lua_dirs = [
      '/usr/local/lib',
      '/usr/lib',
      '/usr/local/lib64',
      '/usr/lib64',
      '/usr/local/lib/x86_64-linux-gnu',
      '/usr/lib/x86_64-linux-gnu',
      '/lib/x86_64-linux-gnu',
      '/lib',
      ]

    lua_aliases = ['lua','lua5.1','lua-5.1']

    liblua = None
    for directory in lua_dirs:
      for alias in lua_aliases:
        library = directory + '/lib' + alias + '.so'
        if os.path.isfile(library):
            libraries = libraries + [alias]
            liblua = alias
            print("info: liblua found: ", library, file=sys.stdout)
            break
      if liblua:
        break

    if not liblua:
        print("error: liblua was not found:\n   ", "\n    ".join(lua_dirs), file=sys.stderr)
        sys.exit(1)

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
    
    if 'build' in sys.argv or 'install' in sys.argv :

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
    
    version = version, 

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
                'src/main/client/type.c',
                'src/main/client/apply.c',
                'src/main/client/close.c',
                'src/main/client/connect.c',
                'src/main/client/exists.c',
                'src/main/client/get.c',
                'src/main/client/info.c',
                'src/main/client/key.c',
                'src/main/client/put.c',
                'src/main/client/query.c',
                'src/main/client/remove.c',
                'src/main/client/scan.c',
                'src/main/client/select.c',
                'src/main/client/admin.c',
                'src/main/client/udf.c',
                'src/main/client/sec_index.c',
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

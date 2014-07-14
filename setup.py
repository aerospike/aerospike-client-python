# -*- coding: utf-8 -*-
from __future__ import print_function

from distutils.core import setup, Extension

import os
import platform
import sys


AEROSPIKE_C_HOME = os.getenv('AEROSPIKE_C_HOME')
PLATFORM =  platform.platform(1)
LINUX = 'Linux' in PLATFORM
DARWIN = 'Darwin' in PLATFORM

library_dirs = ['/usr/local/lib','/usr/lib'] + [x for x in os.getenv('LD_LIBRARY_PATH', '').split(':') if len(x) > 0]

include_dirs = ['src/include'] + [x for x in os.getenv('CPATH', '').split(':') if len(x) > 0]

extra_objects = []

extra_compile_args = [
    '-std=gnu99', '-g', '-Wall', '-fPIC', '-O1',
    '-fno-common', '-fno-strict-aliasing', '-finline-functions', 
    '-march=nocona', 
    '-D_FILE_OFFSET_BITS=64', '-D_REENTRANT', '-D_GNU_SOURCE'
    ]

extra_link_args = []

library_search = library_dirs + [
  '/usr/local/lib',
  '/usr/lib',
  '/usr/local/lib64',
  '/usr/lib64',
  '/usr/local/lib/x86_64-linux-gnu',
  '/usr/lib/x86_64-linux-gnu',
  '/lib/x86_64-linux-gnu',
  '/lib',
  ] 

libraries = [
  'ssl',
  'crypto',
  'pthread',
  'm'
  ]

library_ext = '.so'


# Prefix for Aerospike C client libraries and headers
aerospike_c_prefix = None

# If True, then link libaerospike statically, otherwise it will be dynamically linked
aerospike_c_static = True


################################################################################
# PLATFORM SPECIFIC SETTINGS
################################################################################

if DARWIN:

    if AEROSPIKE_C_HOME:
        aerospike_c_prefix = AEROSPIKE_C_HOME + '/target/Darwin-x86_64'
    else:
        aerospike_c_prefix = '/usr/local'

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

    library_ext = '.dylib'

elif LINUX:

    if AEROSPIKE_C_HOME:
        aerospike_c_prefix = AEROSPIKE_C_HOME + '/target/Linux-x86_64'
    else:
        aerospike_c_prefix = '/usr'

    #---------------------------------------------------------------------------
    # Mac Specific Compiler and Linker Settings
    #---------------------------------------------------------------------------

    extra_compile_args = extra_compile_args + [
        '-rdynamic',
        '-DMARCH_x86_64'
        ]

    libraries = libraries + ['rt']

    #---------------------------------------------------------------------------
    # The following will attempt to resolve the Lua 5.1 library dependency
    #---------------------------------------------------------------------------

    lua_aliases = ['lua','lua5.1']

    liblua = None
    for directory in library_search:
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
        print("error: liblua was not found:\n   ", "\n    ".join(library_search), file=sys.stderr)
        sys.exit(1)

else:
    print("error: OS not supported:", PLATFORM, file=sys.stderr)
    sys.exit(1)


################################################################################
# RESOLVE AEROSPIKE C CLIENT DEPENDNECY
################################################################################

if not os.path.isdir(aerospike_c_prefix):
    print("error: Directory not found:", aerospike_c_prefix, file=sys.stderr)
    sys.exit(1)

#-------------------------------------------------------------------------------
# Check for aerospike.h
#-------------------------------------------------------------------------------

aerospike_h = aerospike_c_prefix + '/include/aerospike/aerospike.h'

if not os.path.isfile(aerospike_h):
    print("error: File not found:", PLATFORM, file=sys.stderr)
    sys.exit(1)

include_dirs = [
    aerospike_c_prefix + '/include', 
    aerospike_c_prefix + '/include/ck'
] + include_dirs


#-------------------------------------------------------------------------------
# Check for libaerospike
#-------------------------------------------------------------------------------

aerospike_a = aerospike_c_prefix + '/lib/libaerospike.a'
aerospike_a_found = False

aerospike_so = aerospike_c_prefix + '/lib/libaerospike' + library_ext
aerospike_so_found = False

if os.path.isfile(aerospike_a):
    aerospike_a_found = True
    print("info: libaerospike found:", aerospike_a, file=sys.stdout)

if os.path.isfile(aerospike_so):
    aerospike_so_found = True
    print("info: libaerospike found:", aerospike_so, file=sys.stdout)

if aerospike_a_found and aerospike_c_static :

    #---------------------------------------------------------------------------
    # Using libaerospike.a
    #---------------------------------------------------------------------------

    print("info: Linking libaerospike statically:", aerospike_a, file=sys.stdout)

    extra_objects = [aerospike_a] + extra_objects

elif aerospike_so_found:

    #---------------------------------------------------------------------------
    # Using libaerospike.so
    #---------------------------------------------------------------------------

    print("info: Linking libaerospike dynamically:", aerospike_so, file=sys.stdout)

    library_dirs = [
        aerospike_c_prefix + '/lib'
    ] + library_dirs

    libraries = ['aerospike'] + libraries

else:

    print("error: Not able to find libaerospike: ", aerospike_so, aerospike_a, file=sys.stderr)
    sys.exit(1)

#---------------------------------------------------------------------------
# Environment Variables
#---------------------------------------------------------------------------

os.putenv('CPATH', ':'.join(include_dirs))
os.putenv('LD_LIBRARY_PATH', ':'.join(library_dirs))
os.putenv('DYLD_LIBRARY_PATH', ':'.join(library_dirs))


################################################################################
# SETUP
################################################################################

setup(
    name        = 'aerospike-client-python', 
    version     = '1.0', 
    ext_modules = [
      Extension( 'aerospike', 
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
        include_dirs = include_dirs,
        library_dirs = library_dirs,
        libraries = libraries,
        extra_link_args = extra_link_args,
        extra_objects = extra_objects,
        extra_compile_args = extra_compile_args
      )
    ]
  )

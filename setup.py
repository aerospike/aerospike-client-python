from distutils.core import setup, Extension
import os
import platform

ostype =  platform.platform()
linux = 'Linux' in ostype
osx = 'Darwin' in ostype

# BUILDING:
#
#   ARCHFLAGS="-arch x86_64" python setup.py build
#

library_dirs = [x for x in os.getenv('LD_LIBRARY_PATH', '').split(':') if len(x) > 0]

extra_compile_args = [
          '-std=gnu99', '-g', '-Wall', '-fPIC', '-O1',
          '-fno-common', '-fno-strict-aliasing', '-finline-functions', 
          '-march=nocona', 
          '-D_FILE_OFFSET_BITS=64', '-D_REENTRANT', '-D_GNU_SOURCE'
        ]

libraries = [ 
  'aerospike',
  'ssl',
  'crypto',
  'pthread',
  'm',
  'lua5.1'
  ]

if osx:
    extra_compile_args.append('-D_DARWIN_UNLIMITED_SELECT')
    extra_compile_args.append('-undefined dynamic_lookup')
    extra_compile_args.append('-DLUA_DEBUG_HOOK')
    extra_compile_args.append('-DMARCH_x86_64')
    library_dirs = ['/usr/local/lib','/usr/lib']
else:
    extra_compile_args.append('-rdynamic')
    extra_compile_args.append('-DMARCH_x86_64')
    libraries.append('rt')

setup(
    name        = 'aerospike-client-python', 
    version     = '1.0', 
    ext_modules = [
      Extension( 'aerospike', 
        [ 
          'src/main/aerospike.c', 
          'src/main/client.c',
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
          'src/main/conversions.c',
          'src/main/key.c',
          'src/main/key/apply.c',
          'src/main/key/exists.c',
          'src/main/key/get.c',
          'src/main/key/put.c',
          'src/main/key/remove.c',
          'src/main/policy.c',
          'src/main/query.c',
          'src/main/query/apply.c',
          'src/main/query/foreach.c',
          'src/main/query/results.c',
          'src/main/query/select.c',
          'src/main/query/where.c',
          'src/main/scan.c',
          'src/main/scan/foreach.c',
          'src/main/scan/results.c',
        ], 
        include_dirs = [
          'src/include'
          # '/usr/include/ck',
          # '/usr/local/include/ck',
        ],
        library_dirs = library_dirs,
        libraries = libraries,
        extra_objects = [
          # '/usr/local/lib/liblua.dylib'
        ],
        extra_compile_args = extra_compile_args
      )
    ]
  )

from distutils.core import setup, Extension
import os
import platform

ostype =  platform.platform(1)
linux = 'Linux' in ostype
osx = 'Darwin' in ostype

# BUILDING:
#
#   ARCHFLAGS="-arch x86_64" python setup.py build
#

extra_compile_args = [
          '-std=gnu99', '-g', '-Wall', '-fPIC', '-O1',
          '-fno-common', '-fno-strict-aliasing', '-finline-functions', 
          '-march=nocona', 
          '-D_FILE_OFFSET_BITS=64', '-D_REENTRANT', '-D_GNU_SOURCE'
        ]

library_dirs = [x for x in os.getenv('LD_LIBRARY_PATH', '').split(':') if len(x) > 0]

library_search = library_dirs + [
  '/usr/local/lib',
  '/usr/lib',
  '/usr/local/lib/x86_64-linux-gnu',
  '/usr/lib/x86_64-linux-gnu',
  '/lib/x86_64-linux-gnu',
  '/lib',
  ] 

libraries = [ 
  'aerospike',
  'ssl',
  'crypto',
  'pthread',
  'm'
  ]

lua_aliases = ['lua','lua5.1']
library_extname = '.so'

if osx:
    
    extra_compile_args = extra_compile_args + [
      '-D_DARWIN_UNLIMITED_SELECT',
      '-undefined','dynamic_lookup',
      '-DLUA_DEBUG_HOOK',
      '-DMARCH_x86_64'
      ]
    
    library_dirs = ['/usr/local/lib','/usr/lib'] + library_dirs
    library_extname = '.dylib'
    libraries = libraries + ['lua']

else:
    
    extra_compile_args.append('-rdynamic')
    extra_compile_args.append('-DMARCH_x86_64')
    
    library_dirs = ['/usr/local/lib','/usr/lib'] + library_dirs
    libraries = libraries + ['rt']
    
    liblua = None
    for d in library_dirs:
      for f in lua_aliases:
        library = d + '/lib' + f + library_extname
        if os.path.isfile(library):
          libraries = libraries + [f]
          liblua = f
          break
      if liblua:
        break

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
        include_dirs = [
          'src/include'
        ],
        library_dirs = library_dirs,
        libraries = libraries,
        extra_objects = [],
        extra_compile_args = extra_compile_args
      )
    ]
  )

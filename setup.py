from distutils.core import setup, Extension
import os

setup(
    name        = 'aerospike-client-python', 
    version     = '1.0', 
    ext_modules = [
      Extension( 'aerospike', 
        [ 
          'src/main/aerospike.c', 
          'src/main/client.c',
          'src/main/client/close.c',
          'src/main/client/info.c',
          'src/main/client/key.c',
          'src/main/client/query.c',
          'src/main/client/scan.c',
          'src/main/conversions.c',
          'src/main/key.c',
          'src/main/key/apply.c',
          'src/main/key/exists.c',
          'src/main/key/get.c',
          'src/main/key/put.c',
          'src/main/key/remove.c',
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
          'src/include',
        ],
        library_dirs = [
        ],
        libraries = [ 
          'ssl',
          'crypto',
          'pthread',
          'm',
          'rt',
          'lua',
          'aerospike'
        ],
        extra_objects = [
          # '/usr/lib/libaerospike.a'
        ],
        extra_compile_args = [
          '-std=gnu99', '-g', '-rdynamic', '-Wall',
          '-fno-common', '-fno-strict-aliasing', '-fPIC',
          '-D_FILE_OFFSET_BITS=64', '-D_REENTRANT', '-D_GNU_SOURCE', '-DMEM_COUNT'
        ]
      )
    ]
  )

project('aerospike', 'c')

incdir = include_directories('src/include')

py = import('python').find_installation(pure: false)

py.extension_module(
    'aerospike',
    'src/main/aerospike.c',
    install: true,
    include_directories: incdir
)

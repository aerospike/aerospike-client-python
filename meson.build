project('aerospike', 'c')

py = import('python').find_installation(pure: false)

py.extension_module(
    'aerospike',
    'src/main/aerospike.c',
    install: true,
)

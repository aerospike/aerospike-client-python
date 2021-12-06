# Version 7.0.0

## Breaking Changes:

### Dropped support for Manylinux2010 wheels

### Added Manylinux2014 wheels build support
Manylinux 2014 is based on Centos7 \
Please refer manylinux compatibility chart for more info: https://github.com/pypa/manylinux \
ubuntu18 may not be supported with manylinux2014 builds

### Log API
Behavior of aerospike.set_log_handler(...) API is changed to do console logging within core client instead of callback. \
By default, error level console logging is enabled. \

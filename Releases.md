# Version 7.0.0

## Breaking Changes:

### Dropped support for Manylinux2010 wheels

### Added Manylinux2014 wheels build support
Manylinux 2014 is based on Centos7 \
Please refer manylinux compatibility chart for more info: https://github.com/pypa/manylinux \
ubuntu18 may not be supported with manylinux2014 builds \

### Log API
Console logging supported by default within core client instead of call back. \
Api aerospike.set_log_handler(...) changed to aerospike.enable_log_handler \

## Features:
CLIENT-1193	Python: Support partition scans \
CLIENT-1570	Python: client support for PKI auth \
CLIENT-1584	Python: Support batch read operations \
CLIENT-1541	Python: Support paging scans \
CLIENT-1558	Python-client - "query user(s) info API \

## Improvements:
CLIENT-1555	Unify Python-Client build/setup along with C-Client build \

## Fixes:

## Updates:

# Version 6.1.2

## Fixes
CLIENT-1639 python pip install now fails with 6.1.0

## Updates
 * Upgraded to [Aerospike C Client 5.2.6](https://download.aerospike.com/download/client/c/notes.html#5.2.6)
 
# Version 6.1.0

## Breaking Changes

### Dropped support for Manylinux2010 wheels

### Added Manylinux2014 wheels build support
Manylinux 2014 is based on Centos7 \
Please refer manylinux compatibility chart for more info: https://github.com/pypa/manylinux \
ubuntu18 may not be supported with manylinux2014 builds

## Features
CLIENT-1193	Python: Support partition scans \
CLIENT-1570	Python: client support for PKI auth \
CLIENT-1584	Python: Support batch read operations \
CLIENT-1541	Python: Support paging scans \
CLIENT-1558	Python-client - "query user(s) info API

## Improvements
CLIENT-1555	Remove dependency on c-client binary from python client source install

## Fixes
CLIENT-1566 Python-Client hangs intermittently in automation cluster

## Updates
 * Upgraded to [Aerospike C Client 5.2.6](https://download.aerospike.com/download/client/c/notes.html#5.2.6)
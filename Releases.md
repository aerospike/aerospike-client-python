# Version 7.2.0
date: 10/14/2022

## Bug Fixes

   - [CLIENT-1854] - Python client crashes while doing IO during server upgrade/downgrade

## Deprecations​

    - client().connect() has been deprecated and it is a no-op. Client call itself establishes the connection object.
    - client().close() has been deprecated and it is a no-op. Client destroy unwinds the conection.
   - If user authentication method is used, then make sure to add user/password information into config dictionary for client call to succeed

ex) config = {
        'hosts': hosts,
        'policies': {'auth_mode': aerospike.AUTH_INTERNAL},
    }
    client = aerospike.client(config)
    client.connect(user, password)

    the above is changed to:

config = {
        'hosts': hosts,
        'policies': {'auth_mode': aerospike.AUTH_INTERNAL},
        'user': user,
        'password': password
    }
    client = aerospike.client(config)
    #following is no-op
    client.connect(user, password)


# Version 7.1.1
date: 10/03/2022

## Bug Fixes

   - [CLIENT-1784] - Potential Memory leak with Python client.

## Improvements

   - [CLIENT-1830] - Add CDT CTX base64 method for using sindex-create info command.
   - [CLIENT-1825] - Expose ttl as part query object attributes.
  
# Version  7.1.0
date: 09/09/2022

## Bug Fixes

   - [CLIENT-1810] - Read policy POLICY_KEY_SEND is not respected when set at the client level.

## New Features

   - [CLIENT-1799] - Support batch read operations.

## Improvements

  - [CLIENT-1749] - Add 'EXISTS' return type for CDT read operations.
  - [CLIENT-1801] - Support creating an secondary index on elements within a CDT using context.
  - [CLIENT-1795] - Make c-client "fail_if_not_connected" option configurable.
  - [CLIENT-1791] - Review and clean up Sphinx documentation.
  - [CLIENT-1792] - Update build instructions.

# Version 7.0.2
date: 05/31/2022

## Bug Fixes

   - [CLIENT-1742] - Fix reference count leaks in client 7.x Batch APIs.
   - [CLIENT-1753] - Fix reference count leak in cdt_ctx map_key_create and list_index_create cases.
   - [CLIENT-1710] - Change BatchRecords default argument from an empty list to None.
   
# Version 7.0.1
date: 04/18/2022

## Bug Fixes

   - [CLIENT-1708] Fix 'Unable to load batch_records module' error when batch_operate, batch_apply, or batch_remove are used without importing aerospike_helpers.batch.records.

# Version 7.0.0
date: 4/06/2022

## Breaking Changes

   - Old predexp support has been removed. See [Incompatible API Changes](https://developer.aerospike.com/client/python/usage/incompatible#version-700) for details.
   - Remove support for deprecated Debian 8.
   - IndexNotFound and IndexNotReadable errors can now trigger retries.
   - Bytes blobs read from the database will now be read as bytes instead of bytearray. See [Incompatible API Changes](https://developer.aerospike.com/client/python/usage/incompatible#version-700) for details.
   - Query policies max_records and records_per_second are now fields of the Query class. See [Incompatible API Changes](https://developer.aerospike.com/client/python/usage/incompatible#version-700) for details.

## Features

   - [CLIENT-1651] - Provide an API to extract an expression's base-64 representation.
   - [CLIENT-1655] - Support new 6.0 truncate, udf-admin, and sindex-admin privileges. This feature requires server version 6.0+.
   - [CLIENT-1659] - Support batch_write, batch_apply, batch_operate, and batch_remove client methods. This feature requires server version 6.0+.
   - [CLIENT-1658] - Support partition queries. This feature requires server version 6.0+.
   - [CLIENT-1690] - Support get_partitions_status for Scan objects.
   - [CLIENT-1693] - Add short_query query policy. This feature requires server version 6.0+.

## Improvements

   - [CLIENT-1687] - Deprecate send_set_name batch policy. Batch transactions now always send set name to the server.
   - [CLIENT-1681] - Drop predexp support.
   - [CLIENT-1683] - Add max retries exceeded exception.
   - [CLIENT-1691] - Document partition scan functionality.
   - [CLIENT-1692] - Update C client to 6.0.
   - [CLIENT-1657] - Move Python client CI tests to github actions.
   - [CLIENT-1694] - Make query policies max_records and records_per_second Query fields instead.
   - [CLIENT-1675] - Bytes blobs read from the database will now be read as bytes instead of bytearray.
   - [CLIENT-1634] - Remove support for deprecated Debian 8.

## Updates

   - [Aerospike C Client 6.0.0.](/download/client/c/notes.html#6.0.0)

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
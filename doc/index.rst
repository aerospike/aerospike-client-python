############
Introduction
############

``aerospike`` is a package which provides a Python client for
Aerospike database clusters. The Python client is a CPython module, built on
the Aerospike C client.

* :mod:`aerospike` - the module containing the Client, Query, Scan, and large ordered list (LList) classes.

* :mod:`aerospike.predicates` is a submodule containing predicate helpers for use with the Query class.

* :mod:`aerospike.exception` is a submodule containing the exception hierarchy for AerospikeError and its subclasses.

* :ref:`Data_Mapping` How Python types map to Aerospike Server types

.. seealso::
    The `Python Client Manual <http://www.aerospike.com/docs/client/python/>`_
    for a quick guide.

Content
#######

.. toctree::
    :maxdepth: 3

    aerospike
    data_mapping
    client
    scan
    query
    predicates
    geojson
    exception


Indices and tables
##################

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

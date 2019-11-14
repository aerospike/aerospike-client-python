############
Introduction
############

``aerospike`` is a package which provides a Python client for
Aerospike database clusters. The Python client is a CPython module, built on
the Aerospike C client.

* :mod:`aerospike` - the module containing the Client, Query, and Scan Classes.

* :mod:`aerospike.predicates` is a submodule containing predicate helpers for use with the Query class.

* :mod:`aerospike.predexp` is a submodule containing predicate expression helpers for use with the Query class.

* :mod:`aerospike.exception` is a submodule containing the exception hierarchy for AerospikeError and its subclasses.

* :mod:`aerospike_helpers` is a helper package for the list, map and bitwise operate commands.

* :ref:`Data_Mapping` How Python types map to Aerospike Server types

.. seealso::
    The `Python Client Manual <http://www.aerospike.com/docs/client/python/>`_
    for a quick guide.

Content
#######

.. toctree::
    :maxdepth: 4

    aerospike
    client
    data_mapping
    scan
    query
    predicates
    predexp
    geojson
    exception
    aerospike_helpers 

Indices and tables
##################

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

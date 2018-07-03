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

* :ref:`Data_Mapping` How Python types map to Aerospike Server types

* :mod:`aerospike_helpers.operations` Operation helper functions.

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
    predexp
    geojson
    exception
    modules


Indices and tables
##################

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

############
Introduction
############

``aerospike`` is a package which provides a Python client for
Aerospike database clusters. The Python client is a CPython module, built on
the Aerospike C client.

Layout
======

+----------------------------+------------------------------------------------------------------------+
| Module                     | Description                                                            |
+============================+========================================================================+
| :mod:`aerospike`           | Contains the Client, Query, and Scan Classes.                          |
+----------------------------+------------------------------------------------------------------------+
| :mod:`aerospike.predicates`| Predicate helpers for the Query class.                                 |
+----------------------------+------------------------------------------------------------------------+
| :mod:`aerospike.exception` | Contains the exception hierarchy for AerospikeError and its subclasses.|
+----------------------------+------------------------------------------------------------------------+
| :mod:`aerospike_helpers`   | Bin operations (list, map, bitwise, etc.), \                           |
|                            | Aerospike expressions, \                                               |
|                            | batch operations, \                                                    |
|                            | complex data type context                                              |
+----------------------------+------------------------------------------------------------------------+

The ``aerospike`` module contains these classes:

=========================== ===========
Class                       Description
=========================== ===========
:ref:`aerospike.scan`       Contains scan operations of entire sets.
:ref:`aerospike.query`      Handles queries over secondary indexes.
:ref:`aerospike.geojson`    Handles GeoJSON type data.
:ref:`client`               Aerospike client API
=========================== ===========

In addition, the :ref:`Data_Mapping` page explains how **Python** types map to **Aerospike Server** types.

.. seealso::
    The `Python Client Manual <http://www.aerospike.com/docs/client/python/>`_
    for a quick guide.

Content
#######

.. toctree::
    :maxdepth: 4

    aerospike
    client
    scan
    query
    predicates
    exception
    aerospike_helpers
    geojson
    data_mapping
    key_ordered_dict

Indices and tables
##################

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

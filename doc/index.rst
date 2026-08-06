############
Introduction
############

The Aerospike Python client enables you to build an application in Python with an
Aerospike cluster as its database. The client manages the connections to the
cluster and handles the commands performed against it.

The Python client is a CPython module built on the Aerospike C client.

.. rubric:: Data Model

At the top is the **namespace**, a container that has one set of policy rules
for all its data, and is similar to the *database* concept in an RDBMS, only
distributed across the cluster. A namespace is subdivided into **sets**,
similar to *tables*.

Pairs of key-value data called **bins** make up **records**, similar to
*columns* of a *row* in a standard RDBMS. Aerospike is schema-less, meaning
that you do not need to define your bins in advance.

Records are uniquely identified by their key, and record metadata is contained
in an in-memory primary index.

.. seealso::
    `Architecture Overview <https://aerospike.com/docs/database/learn/architecture/>`_
    and `Aerospike Data Model
    <https://aerospike.com/docs/database/learn/architecture/data-storage/data-model/>`_ for more
    information about Aerospike.

Layout
======

    * :mod:`aerospike`
        * Constructors for the Client and GeoJSON classes
        * Server-side types
        * Serialization
        * Logging
        * Helper function for calculating key digest
        * Constants
    * :mod:`aerospike.predicates`
        * Query predicates
    * :mod:`aerospike.exception`
        * All exception classes
        * Exception hierarchy
    * :mod:`aerospike_helpers`
        * Bin operations (list, map, bitwise, etc.)
        * Aerospike expressions
        * Batch commands
        * Complex data type context

The :class:`aerospike` module contains these classes:

=================================    ===========
Class                                Description
=================================    ===========
:ref:`client`                        Aerospike client API
:ref:`aerospike.Scan`                Contains scan operations of entire sets.
:ref:`aerospike.Query`               Handles queries over secondary indexes.
:ref:`aerospike.geojson`             Handles GeoJSON type data.
:ref:`aerospike.KeyOrderedDict`      Key ordered dictionary
:ref:`aerospike.Transaction`         Transaction
:ref:`aerospike.ConfigProvider`      Dynamic config provider
=================================    ===========

In addition, the :ref:`Data_Mapping` page explains how **Python** types map to **Aerospike Server** types.

.. seealso::
    The `Python Client Manual <https://aerospike.com/docs/develop/client/python/install/>`_
    for a quick guide.

Content
#######

.. toctree::
    :maxdepth: 4

    aerospike
    client
    scan
    query
    geojson
    key_ordered_dict
    transaction
    config_provider
    predicates
    exception
    aerospike_helpers
    data_mapping

Indices and tables
##################

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

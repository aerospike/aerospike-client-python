.. _aerospike:


************************************************
:mod:`aerospike` --- Aerospike Client for Python
************************************************

.. module:: aerospike
    :platform: 64-bit Linux and OS X
    :synopsis: Python Aerospike client interface.

The Aerospike client enables you to build an application in Python with an
Aerospike cluster as its database. The client manages connections and handles
the transactions performed against the cluster.

**Data Model**

At the top is the ``namespace``, a container that has one set of policy rules
for all its data, and is similar to the *database* concept in an RDBMS, only
distributed across the cluster. A namespace is subdivided into ``sets``,
similar to *tables*.

Pairs of key-value data called ``bins`` make up ``records``, similar to
*columns* of a *row* in a standard RDBMS. Aerospike is schema-less, meaning
that you do not need to define your bins in advance.

Records are uniquely identified by their key, and sets have a primary index
containing the keys of all records in the set.

.. seealso::
    `System Overview` <http://www.aerospike.com/docs/architecture/index.html>`_
    and `Aerospike Data Model
    <http://www.aerospike.com/docs/architecture/data-model.html>`_ for more
    information about Aerospike.


.. exception:: Error

    Raised when an error specific to Tokyo Cabinet happens.


.. function:: client(config)

    Creates a new instance of the Client class.

    :param dict config: The client's configuration

    ::

      'hosts': a list of (address, port) tuples identifying the cluster
      'policies': a dictionary of policies
        'timeout'          : default timeout in milliseconds
        'key'              : default key policy for this client
        'exists'           : default exists policy for this client
        'gen'              : default generation policy for this client
        'retry'            : default retry policy for this client
        'consistency_level': default consistency level policy for this client
        'replica'          : default replica policy for this client
        'commit_level'     : default commit level policy for this client

    Example::

    import aerospike

    config = {
        'hosts':    [ ('127.0.0.1', 3000) ],
        'policies': {'timeout': 1000}}
    client = aerospike.client(config).connect()


.. data:: OPERATOR_APPEND

    The append-to-bin operator for the multi-ops method :py:meth:`~aerospike.Client.get`

.. data:: OPERATOR_INCR

    The increment-bin operator for the multi-ops method :py:meth:`~aerospike.Client.operate`

.. data:: INT_MIN

    The largest positive and negative integers as defined by the platform
    (mainly used for testing :meth:`addint`).


.. toctree::
    :maxdepth: 1

    Client
    Query
    Scan
    LList

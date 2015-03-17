.. _aerospike:

*************************************************
:mod:`aerospike` --- Aerospike Client for Python
*************************************************

.. module:: aerospike
    :platform: 64-bit Linux and OS X
    :synopsis: Aerospike client for Python.

The Aerospike client enables you to build an application in Python with an
Aerospike cluster as its database. The client manages connections and handles
the transactions performed against the cluster.

.. rubric:: Data Model

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
    `System Overview <http://www.aerospike.com/docs/architecture/index.html>`_
    and `Aerospike Data Model
    <http://www.aerospike.com/docs/architecture/data-model.html>`_ for more
    information about Aerospike.


.. py:function:: client(config)

    Creates a new instance of the Client class.

    :param dict config: the client's configuration.

        .. hlist::
            :columns: 1

            * **hosts** a :class:`list` of (address, port) tuples identifying the cluster
            * **policies** a :class:`dict` of policies
                * **timeout** default timeout in milliseconds
                * **key** default key policy for this client
                * **exists** default exists policy for this client
                * **gen** default generation policy for this client
                * **retry** default retry policy for this client
                * **consistency_level** default consistency level policy for this client
                * **replica** default replica policy for this client
                * **commit_level** default commit level policy for this client

    :return: an :py:class:`aerospike.Client` class.

    .. seealso::
        `Client Policies <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html>`_.

    .. code-block:: python

        import aerospike

        config = {
            'hosts':    [ ('127.0.0.1', 3000) ],
            'policies': {'timeout': 1000}}
        client = aerospike.client(config)


.. rubric:: Serialization

.. note::

    By default, the :py:class:`aerospike.Client` maps supported types such \
    as :class:`int`, :class:`str`, :class:`list`, :class:`dict` to matching
    aerospike server `types <http://www.aerospike.com/docs/guide/data-types.html>`_ \
    (int, string, list, map). When an unsupported type is encountered the module \
    uses `cPickle <https://docs.python.org/2/library/pickle.html?highlight=cpickle#module-cPickle>`_ \
    to serialize and deserialize the data, storing it into a *bytes* of type \
    `'Python' <https://www.aerospike.com/docs/udf/api/bytes.html#encoding-type>`_ \
    (`AS_BYTES_PYTHON <http://www.aerospike.com/apidocs/c/d0/dd4/as__bytes_8h.html#a0cf2a6a1f39668f606b19711b3a98bf3>`_).

    Two functions :py:func:`set_serializer` and :py:func:`set_deserializer` \
    allow for user-defined functions to handle serialization, instead. \
    The serialized data is stored as \
    'Generic' type *bytes* of type (\
    `AS_BYTES_BLOB <http://www.aerospike.com/apidocs/c/d0/dd4/as__bytes_8h.html#a0cf2a6a1f39668f606b19711b3a98bf3>`_).

.. py:function:: set_serializer(callback)

    Overrides the default serializer with a user-defined fucntion *callback*.

    :param callback callback: the function to invoke for serialization.

    .. code-block:: python

        import aerospike
        import cPickle as pickle

        def my_serializer(val):
            return pickle.dumps(val)

        aerospike.set_serializer(my_serializer)

    .. versionadded:: 1.0.39


.. py:function:: set_deserializer(callback)

    Overrides the default serializer with a user-defined fucntion *callback*.

    :param callback callback: the function to invoke for deserialization.

    .. code-block:: python

        import aerospike
        import cPickle as pickle

        def my_deserializer(val):
            return pickle.loads(val)

        aerospike.set_deserializer(my_serializer)

    .. versionadded:: 1.0.39


.. _aerospike_operators:

Operators
---------

.. data:: OPERATOR_APPEND

    The append-to-bin operator for the multi-ops method :py:meth:`~aerospike.Client.operate`

.. data:: OPERATOR_INCR

    The increment-bin operator for the multi-ops method :py:meth:`~aerospike.Client.operate`

.. data:: OPERATOR_PREPEND

    The prepend-to-bin operator for the multi-ops method :py:meth:`~aerospike.Client.operate`

.. data:: OPERATOR_READ

    The read-bin operator for the multi-ops method :py:meth:`~aerospike.Client.operate`

.. data:: OPERATOR_TOUCH

    The touch-record operator for the multi-ops method :py:meth:`~aerospike.Client.operate`

.. data:: OPERATOR_WRITE

    The write-bin operator for the multi-ops method :py:meth:`~aerospike.Client.operate`

.. _aerospike_policies:

Policies
--------

.. data:: POLICY_COMMIT_LEVEL_ALL

    An option of the *'commit_level'* policy

.. data:: POLICY_COMMIT_LEVEL_MASTER

.. data:: POLICY_CONSISTENCY_ALL

    An option of the *'consistency_level'* policy

.. data:: POLICY_CONSISTENCY_ONE

.. data:: POLICY_EXISTS_CREATE

    An option of the *'exists'* policy

.. data:: POLICY_EXISTS_CREATE_OR_REPLACE

.. data:: POLICY_EXISTS_IGNORE

.. data:: POLICY_EXISTS_REPLACE

.. data:: POLICY_EXISTS_UPDATE

.. data:: POLICY_GEN_EQ

    An option of the *'gen'* policy

.. data:: POLICY_GEN_GT

.. data:: POLICY_GEN_IGNORE

.. data:: POLICY_KEY_DIGEST

    An option of the *'key'* policy

.. data:: POLICY_KEY_SEND

.. data:: POLICY_REPLICA_ANY

    An option of the *'replica'* policy

.. data:: POLICY_REPLICA_MASTER

.. data:: POLICY_RETRY_NONE

    An option of the *'retry'* policy

.. data:: POLICY_RETRY_ONCE

.. _aerospike_scan_constants:

Scan Constants
--------------

.. data:: SCAN_PRIORITY_AUTO

.. data:: SCAN_PRIORITY_HIGH

.. data:: SCAN_PRIORITY_LOW

.. data:: SCAN_PRIORITY_MEDIUM

.. data:: SCAN_STATUS_ABORTED

.. data:: SCAN_STATUS_COMPLETED

.. data:: SCAN_STATUS_INPROGRESS

.. data:: SCAN_STATUS_UNDEF

.. versionadded:: 1.0.39



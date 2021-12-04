.. _aerospike.exception:

***************************************************
:mod:`aerospike.exception` --- Aerospike Exceptions
***************************************************

.. module:: aerospike.exception
    :platform: 64-bit Linux and OS X
    :synopsis: Exceptions raised by the Aerospike client.

.. code-block:: python


    import aerospike
    from aerospike import exception as ex

    try:
        config = { 'hosts': [ ('127.0.0.1', 3000)], 'policies': { 'total_timeout': 1200}}
        client = aerospike.client(config).connect()
        client.close()
    except ex.AerospikeError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))

.. versionadded:: 1.0.44

In Doubt Status
---------------
  The in doubt status of a caught exception can be checked by looking at the 5th element of its `args` tuple

  .. code-block:: python

      key = 'test', 'demo', 1
      record = {'some': 'thing'}
      try:
        client.put(key, record)
      except AerospikeError as exc:
        print("The in doubt nature of the operation is: {}".format(exc.args[4])

.. versionadded:: 3.0.1

Exception Types
---------------
.. py:exception:: AerospikeError

    The parent class of all exceptions raised by the Aerospike client, inherits
    from :py:exc:`exceptions.Exception` . `These attributes should be checked by
    executing `exc.args[i]` where i is the index of the attribute. For example
    to check `in_doubt`, run `exc.args[4]`

    .. py:attribute:: code

        The associated status code.

    .. py:attribute:: msg

        The human-readable error message.

    .. py:attribute:: file
    .. py:attribute:: line

    .. py:attribute:: in_doubt

        True if it is possible that the operation succeeded.

.. py:exception:: ClientError

    Exception class for client-side errors, often due to mis-configuration or
    misuse of the API methods. Subclass of :py:exc:`~aerospike.exception.AerospikeError`.

.. py:exception:: InvalidHostError

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: ParamError

    The operation was not performed because of invalid parameters.

.. py:exception:: ServerError

    The parent class for all errors returned from the cluster.

.. py:exception:: InvalidRequest

    Protocol-level error. Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: OpNotApplicable

    The operation cannot be applied to the current bin value on the server.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: FilteredOut

    The transaction was not performed because the predexp was false.

.. py:exception:: ServerFull

    The server node is running out of memory and/or storage device space
    reserved for the specified namespace.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: AlwaysForbidden

    Operation not allowed in current configuration.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: UnsupportedFeature

    Encountered an unimplemented server feature.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: DeviceOverload

    The server node's storage device(s) can't keep up with the write load.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: NamespaceNotFound

    Namespace in request not found on server.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: ForbiddenError

    Operation not allowed at this time.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: ElementExistsError

    Raised when trying to alter a map key which already exists, when using a create_only policy.

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: ElementNotFoundError

    Raised when trying to alter a map key which does not exist, when using an update_only policy.

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: RecordError

    The parent class for record and bin exceptions exceptions associated with
    read and write operations. Subclass of :py:exc:`~aerospike.exception.ServerError`.

    .. py:attribute:: key

        The key identifying the record.

    .. py:attribute:: bin

        Optionally the bin associated with the error.

.. py:exception:: RecordKeyMismatch

    Record key sent with transaction did not match key stored on server.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: RecordNotFound

    Record does not exist in database. May be returned by read, or write with
    policy :py:data:`aerospike.POLICY_EXISTS_UPDATE`.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: RecordGenerationError

    Generation of record in database does not satisfy write policy.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: RecordExistsError

    Record already exists. May be returned by write with policy
    :py:data:`aerospike.POLICY_EXISTS_CREATE`. Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: RecordBusy

    Too may concurrent requests for one record - a "hot-key" situation.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: RecordTooBig

    Record being (re-)written can't fit in a storage write block.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: BinNameError

    Length of bin name exceeds the limit of 14 characters.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: BinIncompatibleType

    Bin modification operation can't be done on an existing bin due to its
    value type (for example appending to an integer).
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: IndexError

    The parent class for indexing exceptions.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

    .. py:attribute:: index_name

        The name of the index associated with the error.

.. py:exception:: IndexNotFound

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: IndexFoundError

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: IndexOOM

    The index is out of memory.
    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: IndexNotReadable

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: IndexNameMaxLen

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: IndexNameMaxCount

    Reached the maximum allowed number of indexes.
    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: QueryError

    Exception class for query errors.
    Subclass of :py:exc:`~aerospike.exception.AerospikeError`.

.. py:exception:: QueryQueueFull

    Subclass of :py:exc:`~aerospike.exception.QueryError`.

.. py:exception:: QueryTimeout

    Subclass of :py:exc:`~aerospike.exception.QueryError`.

.. py:exception:: ClusterError

    Cluster discovery and connection errors.
    Subclass of :py:exc:`~aerospike.exception.AerospikeError`.

.. py:exception:: ClusterChangeError

    A cluster state change occurred during the request. This may also be
    returned by scan operations with the fail-on-cluster-change flag set.
    Subclass of :py:exc:`~aerospike.exception.ClusterError`.

.. py:exception:: AdminError

    The parent class for exceptions of the security API.

.. py:exception:: ExpiredPassword

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: ForbiddenPassword

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: IllegalState

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidCommand

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidCredential

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidField

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidPassword

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidPrivilege

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidRole

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidUser

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: NotAuthenticated

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: RoleExistsError

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: RoleViolation

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: SecurityNotEnabled

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: SecurityNotSupported

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: SecuritySchemeNotSupported

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: UserExistsError

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: UDFError

    The parent class for UDF exceptions exceptions.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

    .. py:attribute:: module

        The UDF module associated with the error.

    .. py:attribute:: func

        Optionally the name of the UDF function.

.. py:exception:: UDFNotFound

    Subclass of :py:exc:`~aerospike.exception.UDFError`.

.. py:exception:: LuaFileNotFound

    Subclass of :py:exc:`~aerospike.exception.UDFError`.


Exception Hierarchy
-------------------

.. parsed-literal::

    AerospikeError (*)
     +-- TimeoutError (9)
     +-- ClientError (-1)
     |    +-- InvalidHost (-4)
     |    +-- ParamError (-2)
     +-- ServerError (1)
          +-- InvalidRequest (4)
          +-- ServerFull (8)
          +-- AlwaysForbidden (10)
          +-- UnsupportedFeature (16)
          +-- DeviceOverload (18)
          +-- NamespaceNotFound (20)
          +-- ForbiddenError (22)
          +-- ElementNotFoundError (23)
          +-- ElementExistsError (24)
          +-- RecordError (*)
          |    +-- RecordKeyMismatch (19)
          |    +-- RecordNotFound (2)
          |    +-- RecordGenerationError (3)
          |    +-- RecordExistsError (5)
          |    +-- RecordTooBig (13)
          |    +-- RecordBusy (14)
          |    +-- BinNameError (21)
          |    +-- BinIncompatibleType (12)
          +-- IndexError (204)
          |    +-- IndexNotFound (201)
          |    +-- IndexFoundError (200)
          |    +-- IndexOOM (202)
          |    +-- IndexNotReadable (203)
          |    +-- IndexNameMaxLen (205)
          |    +-- IndexNameMaxCount (206)
          +-- QueryError (213)
          |    +-- QueryQueueFull (211)
          |    +-- QueryTimeout (212)
          +-- ClusterError (11)
          |    +-- ClusterChangeError (7)
          +-- AdminError (*)
          |    +-- SecurityNotSupported (51)
          |    +-- SecurityNotEnabled (52)
          |    +-- SecuritySchemeNotSupported (53)
          |    +-- InvalidCommand (54)
          |    +-- InvalidField (55)
          |    +-- IllegalState (56)
          |    +-- InvalidUser (60)
          |    +-- UserExistsError (61)
          |    +-- InvalidPassword (62)
          |    +-- ExpiredPassword (63)
          |    +-- ForbiddenPassword (64)
          |    +-- InvalidCredential (65)
          |    +-- InvalidRole (70)
          |    +-- RoleExistsError (71)
          |    +-- RoleViolation (81)
          |    +-- InvalidPrivilege (72)
          |    +-- NotAuthenticated (80)
          +-- UDFError (*)
               +-- UDFNotFound (1301)
               +-- LuaFileNotFound (1302)

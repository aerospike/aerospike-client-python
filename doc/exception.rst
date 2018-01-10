.. _aerospike.exception:

***************************************************
:mod:`aerospike.exception` --- Aerospike Exceptions
***************************************************

.. module:: aerospike.exception
    :platform: 64-bit Linux and OS X
    :synopsis: Exceptions raised by the Aerospike client.

.. code-block:: python

    from __future__ import print_function

    import aerospike
    from aerospike.exception import *

    try:
        config = { 'hosts': [ ('127.0.0.1', 3000)], 'policies': { 'total_timeout': 1200}}
        client = aerospike.client(config).connect()
        client.close()
    except ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))


.. versionadded:: 1.0.44


.. py:exception:: AerospikeError

    The parent class of all exceptions raised by the Aerospike client, inherits
    from :py:exc:`exceptions.Exception`

    .. py:attribute:: code

        The associated status code.

    .. py:attribute:: msg

        The human-readable error message.

    .. py:attribute:: file
    .. py:attribute:: line

.. py:exception:: ClientError

    Exception class for client-side errors, often due to mis-configuration or
    misuse of the API methods. Subclass of :py:exc:`~aerospike.exception.AerospikeError`.

.. py:exception:: InvalidHostError

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: ParamError

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: ServerError

    The parent class for all errors returned from the cluster.

.. py:exception:: InvalidRequest

    Protocol-level error. Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: ServerFull

    The server node is running out of memory and/or storage device space
    reserved for the specified namespace.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: NoXDR

    XDR is not available for the cluster.
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

.. py:exception:: RecordGenerationError

    Record already exists. May be returned by write with policy
    :py:data:`aerospike.POLICY_EXISTS_CREATE`. Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: RecordBusy

    Record being (re-)written can't fit in a storage write block.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: RecordTooBig

    Too may concurrent requests for one record - a "hot-key" situation.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: BinNameError

    Length of bin name exceeds the limit of 14 characters.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: BinExistsError

    Bin already exists. Occurs only if the client has that check enabled.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: BinNotFound

    Bin-level replace-only supported on server but not on client.
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

.. py:exception:: LDTError

    The parent class for Large Data Type exceptions.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

    .. py:attribute:: key

        The key identifying the record.

    .. py:attribute:: bin

        The bin containing the LDT.

.. py:exception:: LargeItemNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTInternalError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTUniqueKeyError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTInsertError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTSearchError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTDeleteError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTInputParamError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTTypeMismatch

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTBinNameNull

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTBinNameNotString

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTBinNameTooLong

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTTooManyOpenSubrecs

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTTopRecNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTSubRecNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTBinNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTBinExistsError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTBinDamaged

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTSubrecPoolDamaged

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTSubrecDamaged

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTSubrecOpenError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTSubrecUpdateError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTSubrecCreateError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTSubrecDeleteError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTSubrecCloseError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTToprecUpdateError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTToprecCreateError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTFilterFunctionBad

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTFilterFunctionNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTKeyFunctionBad

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTKeyFunctionNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTTransFunctionBad

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTTransFunctionNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTUntransFunctionBad

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTUntransFunctionNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTUserModuleBad

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: LDTUserModuleNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.


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
          +-- NoXDR (10)
          +-- UnsupportedFeature (16)
          +-- DeviceOverload (18)
          +-- NamespaceNotFound (20)
          +-- ForbiddenError (22)
          +-- RecordError (*)
          |    +-- RecordKeyMismatch (19)
          |    +-- RecordNotFound (2)
          |    +-- RecordGenerationError (3)
          |    +-- RecordExistsError (5)
          |    +-- RecordTooBig (13)
          |    +-- RecordBusy (14)
          |    +-- BinNameError (21)
          |    +-- BinExistsError (6)
          |    +-- BinNotFound (17)
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
          |    +-- UDFNotFound (1301)
          |    +-- LuaFileNotFound (1302)
          +-- LDTError (*)
               +-- LargeItemNotFound (125)
               +-- LDTInternalError (1400)
               +-- LDTNotFound (1401)
               +-- LDTUniqueKeyError (1402)
               +-- LDTInsertError (1403)
               +-- LDTSearchError (1404)
               +-- LDTDeleteError (1405)
               +-- LDTInputParamError (1409)
               +-- LDTTypeMismatch (1410)
               +-- LDTBinNameNull (1411)
               +-- LDTBinNameNotString (1412)
               +-- LDTBinNameTooLong (1413)
               +-- LDTTooManyOpenSubrecs (1414)
               +-- LDTTopRecNotFound (1415)
               +-- LDTSubRecNotFound (1416)
               +-- LDTBinNotFound (1417)
               +-- LDTBinExistsError (1418)
               +-- LDTBinDamaged (1419)
               +-- LDTSubrecPoolDamaged (1420)
               +-- LDTSubrecDamaged (1421)
               +-- LDTSubrecOpenError (1422)
               +-- LDTSubrecUpdateError (1423)
               +-- LDTSubrecCreateError (1424)
               +-- LDTSubrecDeleteError (1425)
               +-- LDTSubrecCloseError (1426)
               +-- LDTToprecUpdateError (1427)
               +-- LDTToprecCreateError (1428)
               +-- LDTFilterFunctionBad (1430)
               +-- LDTFilterFunctionNotFound (1431)
               +-- LDTKeyFunctionBad (1432)
               +-- LDTKeyFunctionNotFound (1433)
               +-- LDTTransFunctionBad (1434)
               +-- LDTTransFunctionNotFound (1435)
               +-- LDTUntransFunctionBad (1436)
               +-- LDTUntransFunctionNotFound (1437)
               +-- LDTUserModuleBad (1438)
               +-- LDTUserModuleNotFound (1439)



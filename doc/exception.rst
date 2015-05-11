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
      config = { 'hosts': [ ('127.0.0.1', 3000)], 'policies': { 'timeout': 1200}}
      client = aerospike.client(config).connect()
      client.close()
    except ClientError as e:
      print("Error: {0} [{1}]".format(e.msg, e.code))


.. py:exception:: aerospike.exception.AerospikeError

    The parent class of all exceptions raised by the Aerospike client, inherets
    from :py:exc:`exceptions.Exception`

    .. py:attribute:: code

        The associated status code.

    .. py:attribute:: msg

        The human-readable error message.

    .. py:attribute:: file
    .. py:attribute:: line

.. py:exception:: aerospike.exception.ClientError

    Exception class for client-side errors, often due to mis-configuration or
    misuse of the API methods. Subclass of :py:exc:`~aerospike.exception.AerospikeError`.

.. py:exception:: aerospike.exception.InvalidHostError

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: aerospike.exception.ParamError

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: aerospike.exception.ServerError

    The parent class for all errors returned from the cluster.

.. py:exception:: aerospike.exception.InvalidRequest

    Protocol-level error. Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: aerospike.exception.ServerFull

    The server node is running out of memory and/or storage device space
    reserved for the specified namespace.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: aerospike.exception.NoXDR

    XDR is not available for the cluster.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: aerospike.exception.UnsupportedFeature

    Encountered an unimplemented server feature.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: aerospike.exception.DeviceOverload

    The server node's storage device(s) can't keep up with the write load.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: aerospike.exception.NamespaceNotFound

    Namespace in request not found on server.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: aerospike.exception.ForbiddenError

    Operation not allowed at this time.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: aerospike.exception.RecordError

    The parent class for record and bin exceptions exceptions associated with
    read and write operations. Subclass of :py:exc:`~aerospike.exception.ServerError`.

    .. py:attribute:: key

        The key identifying the record.

    .. py:attribute:: bin

        Optionally the bin associated with the error.

.. py:exception:: aerospike.exception.RecordKeyMismatch

    Record key sent with transaction did not match key stored on server.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: aerospike.exception.RecordNotFound

    Record does not exist in database. May be returned by read, or write with
    policy :py:data:`aerospike.POLICY_EXISTS_UPDATE`.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: aerospike.exception.RecordGenerationError

    Generation of record in database does not satisfy write policy.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: aerospike.exception.RecordGenerationError

    Record already exists. May be returned by write with policy
    :py:data:`aerospike.POLICY_EXISTS_CREATE`. Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: aerospike.exception.RecordBusy

    Record being (re-)written can't fit in a storage write block.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: aerospike.exception.RecordTooBig

    Too may concurrent requests for one record - a "hot-key" situation.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: aerospike.exception.BinNameError

    Length of bin name exceeds the limit of 14 characters.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: aerospike.exception.BinExistsError

    Bin already exists. Occurs only if the client has that check enabled.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: aerospike.exception.BinNotFound

    Bin-level replace-only supported on server but not on client.
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: aerospike.exception.BinIncompatibleType

    Bin modification operation can't be done on an existing bin due to its
    value type (for example appending to an integer).
    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: aerospike.exception.IndexError

    The parent class for indexing exceptions.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

    .. py:attribute:: index_name

        The name of the index associated with the error.

.. py:exception:: aerospike.exception.IndexNotFound

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: aerospike.exception.IndexFoundError

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: aerospike.exception.IndexOOM

    The index is out of memory.
    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: aerospike.exception.IndexNotReadable

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: aerospike.exception.IndexNameMaxLen

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: aerospike.exception.IndexNameMaxCount

    Reached the maximum allowed number of indexes.
    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: aerospike.exception.QueryError

    Exception class for query errors.
    Subclass of :py:exc:`~aerospike.exception.AerospikeError`.

.. py:exception:: aerospike.exception.QueryQueueFull

    Subclass of :py:exc:`~aerospike.exception.QueryError`.

.. py:exception:: aerospike.exception.QueryTimeout

    Subclass of :py:exc:`~aerospike.exception.QueryError`.

.. py:exception:: aerospike.exception.ClusterError

    Cluster discovery and connection errors.
    Subclass of :py:exc:`~aerospike.exception.AerospikeError`.

.. py:exception:: aerospike.exception.ClusterChangeError

    A cluster state change occurred during the request. This may also be
    returned by scan operations with the fail-on-cluster-change flag set.
    Subclass of :py:exc:`~aerospike.exception.ClusterError`.

.. py:exception:: aerospike.exception.AdminError

    The parent class for exceptions of the security API.

.. py:exception:: aerospike.exception.ExpiredPassword

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.ForbiddenPassword

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.IllegalState

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.InvalidCommand

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.InvalidCredential

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.InvalidField

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.InvalidPassword

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.InvalidPrivilege

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.InvalidRole

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.InvalidUser

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.NotAuthenticated

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.RoleExistsError

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.RoleViolation

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.SecurityNotEnabled

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.SecurityNotSupported

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.SecuritySchemeNotSupported

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.UserExistsError

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: aerospike.exception.UDFError

    The parent class for UDF exceptions exceptions.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

    .. py:attribute:: module

        The UDF module associated with the error.

    .. py:attribute:: func

        Optionally the name of the UDF function.

.. py:exception:: aerospike.exception.UDFNotFound

    Subclass of :py:exc:`~aerospike.exception.UDFError`.

.. py:exception:: aerospike.exception.LuaFileNotFound

    Subclass of :py:exc:`~aerospike.exception.UDFError`.

.. py:exception:: aerospike.exception.LDTError

    The parent class for Large Data Type exceptions.
    Subclass of :py:exc:`~aerospike.exception.ServerError`.

    .. py:attribute:: key

        The key identifying the record.

    .. py:attribute:: bin

        The bin containing the LDT.

.. py:exception:: aerospike.exception.LargeItemNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTInternalError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTUniqueKeyError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTInsertError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTSearchError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTDeleteError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTInputParamError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTTypeMismatch

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTBinNameNull

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTBinNameNotString

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTBinNameTooLong

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTTooManyOpenSubrecs

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTTopRecNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTSubRecNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTBinNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTBinExistsError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTBinDamaged

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTSubrecPoolDamaged

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTSubrecDamaged

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTSubrecOpenError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTSubrecUpdateError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTSubrecCreateError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTSubrecDeleteError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTSubrecCloseError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTToprecUpdateError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTToprecCreateError

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTFilterFunctionBad

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTFilterFunctionNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTKeyFunctionBad

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTKeyFunctionNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTTransFunctionBad

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTTransFunctionNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTUntransFunctionBad

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTUntransFunctionNotFound

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTUserModuleBad

    Subclass of :py:exc:`~aerospike.exception.LDTError`.

.. py:exception:: aerospike.exception.LDTUserModuleNotFound

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



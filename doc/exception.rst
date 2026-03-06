.. _aerospike.exception:

***************************************************
:mod:`aerospike.exception` --- Aerospike Exceptions
***************************************************

Example
-------

.. module:: aerospike.exception
    :platform: 64-bit Linux and OS X
    :synopsis: Exceptions raised by the Aerospike client.

This is a simple example on how to catch an exception thrown by the Aerospike client:

.. code-block:: python

    import aerospike
    from aerospike import exception as ex

    try:
        config = { 'hosts': [ ('127.0.0.1', 3000)], 'policies': { 'total_timeout': 1200}}
        client = aerospike.client(config)
        client.close()
    except ex.AerospikeError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))

.. versionadded:: 1.0.44

Base Class
---------------

.. py:exception:: AerospikeError

    The parent class of all exceptions raised by the Aerospike client.

    An exception of this class type must have the following attributes:

    .. py:attribute:: code

        The associated status code.

    .. py:attribute:: msg

        The human-readable error message.

    .. py:attribute:: file

        File where the exception occurred.

    .. py:attribute:: line

        Line in the file where the exception occurred.

    .. py:attribute:: in_doubt

        ``True`` if it is possible that the command succeeded. See :ref:`indoubt`.

    In addition to accessing these attributes by their names, \
    they can also be checked by calling ``exc.args[i]``, where ``exc`` is the exception object and \
    ``i`` is the index of the attribute in the order they appear above. \
    For example, run ``exc.args[4]`` to get the ``in_doubt`` flag.

    Inherits from :py:exc:`exceptions.Exception`.

Client Errors
-------------

.. py:exception:: ClientError

    Exception class for client-side errors, often due to misconfiguration or misuse of the API methods.

    Error code: ``-1``

.. py:exception:: ParamError

    The operation was not performed because of invalid parameters.

    Error code: ``-2``

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: InvalidHostError

    Host name could not be found in DNS lookup.

    Error code: ``-4``

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: ClientAbortError

    Query or scan was aborted in user's callback.

    Error code: ``-5``

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: InvalidNodeError

    Node invalid or could not be found.

    Error code: ``-8``

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: TLSError

    TLS error occurred.

    Error code: ``-9``

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: ConnectionError

    Synchronous connection error.

    Error code: ``-10``

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: MaxRetriesExceeded

    Max retries limit reached.

    Error code: ``-12``

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: MaxErrorRateExceeded

    The operation was not performed because the maximum error rate has been exceeded.

    Error code: ``-14``

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: NoResponse

    No response received from server.

    Error code: ``-15``

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: BatchFailed

    One or more keys failed in a batch.

    Error code: ``-16``

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: TransactionFailed

    Transaction failed.

    Error code: ``-17``

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: TransactionAlreadyCommitted

    Transaction abort called, but the transaction was already committed.

    Error code: ``-18``

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

.. py:exception:: TransactionAlreadyAborted

    Transaction commit called, but the transaction was already aborted.

    Error code: ``-19``

    Subclass of :py:exc:`~aerospike.exception.ClientError`.

Server Errors
-------------

.. py:exception:: ServerError

    The parent class for all errors returned from the cluster.

    Error code: ``1``

.. py:exception:: InvalidRequest

    Request protocol invalid, or invalid protocol field.

    Error code: ``4``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: ServerFull

    The server node is running out of memory and/or storage device space
    reserved for the specified namespace.

    Error code: ``8``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: AlwaysForbidden

    Operation not allowed in current configuration.

    Error code: ``10``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: ScanAbortedError

    Scan aborted by user.

    Error code: ``15``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: UnsupportedFeature

    Encountered an unimplemented server feature.

    Error code: ``16``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: DeviceOverload

    The server node's storage device(s) can't keep up with the write load.

    Error code: ``18``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: NamespaceNotFound

    Namespace in request not found on server.

    Error code: ``20``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: ForbiddenError

    Operation not allowed at this time.

    Error code: ``22``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: ElementExistsError

    Raised when trying to alter a map key which already exists, when using a ``create_only`` policy.

    Error code: ``23``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: ElementNotFoundError

    Raised when trying to alter a map key which does not exist, when using an ``update_only`` policy.

    Error code: ``24``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: OpNotApplicable

    The operation cannot be applied to the current bin value on the server.

    Error code: ``26``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: FilteredOut

    The command was not performed because the expression was false.

    Error code: ``27``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: LostConflict

    Write command loses conflict to XDR.

    Error code: ``28``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: BatchDisabledError

    Batch functionality has been disabled.

    Error code: ``150``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: BatchMaxRequestError

    Batch max requests have been exceeded.

    Error code: ``151``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: BatchQueueFullError

    All batch queues are full.

    Error code: ``152``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: InvalidGeoJSON

    Invalid/Unsupported GeoJSON.

    Error code: ``160``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: QueryAbortedError

    Query was aborted.

    Error code: ``210``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

Record Errors
-------------

.. py:exception:: RecordError

    The parent class for record and bin exceptions exceptions associated with read and write operations.

    .. py:attribute:: key

        The key identifying the record.

    .. py:attribute:: bin

        (Optional) the bin associated with the error.

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: RecordNotFound

    Record does not exist in database. May be returned by either a read or a \
    write with the policy :py:data:`aerospike.POLICY_EXISTS_UPDATE`.

    Error code: ``2``

    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: RecordGenerationError

    Generation of record in database does not satisfy write policy.

    Error code: ``3``

    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: RecordExistsError

    Record already exists. May be returned by a write with policy :py:data:`aerospike.POLICY_EXISTS_CREATE`.

    Error code: ``5``

    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: BinExistsError

    Bin already exists on a create-only operation.

    Error code: ``6``

    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: BinIncompatibleType

    Bin modification operation can't be done on an existing bin due to its value type \
    (for example appending to an integer).

    Error code: ``12``

    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: RecordTooBig

    Record being (re-)written can't fit in a storage write block.

    Error code: ``13``

    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: RecordBusy

    Too may concurrent requests for one record - a "hot-key" situation.

    Error code: ``14``

    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: BinNotFound

    Bin not found on update-only operation.

    Error code: ``17``

    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: RecordKeyMismatch

    Record key sent with command did not match key stored on server.

    Error code: ``19``

    Subclass of :py:exc:`~aerospike.exception.RecordError`.

.. py:exception:: BinNameError

    Either length of bin name exceeds the limit of 15 characters, or namespace's bin name quota was exceeded.

    Error code: ``21``

    Subclass of :py:exc:`~aerospike.exception.RecordError`.

Index Errors
------------

.. py:exception:: IndexError

    The parent class for secondary index exceptions.

    Error code: ``204``

    .. py:attribute:: name

        The name of the index associated with the error.

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: IndexFoundError

    Error code: ``200``

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: IndexNotFound

    Error code: ``201``

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: IndexOOM

    The index is out of memory.

    Error code: ``202``

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: IndexNotReadable

    Error code: ``203``

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: IndexNameMaxLen

    Index name is too long.

    Error code: ``205``

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

.. py:exception:: IndexNameMaxCount

    Reached the maximum allowed number of indexes.

    Error code: ``206``

    Subclass of :py:exc:`~aerospike.exception.IndexError`.

Query Errors
------------

.. py:exception:: QueryQueueFull

    Query processing queue is full.

    Error code: ``211``

    Subclass of :py:exc:`~aerospike.exception.QueryError`.

.. py:exception:: QueryTimeout

    Secondary index query timed out on server.

    Error code: ``212``

    Subclass of :py:exc:`~aerospike.exception.QueryError`.

.. py:exception:: QueryError

    Exception class for query errors.

    Error code: ``213``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

Cluster Errors
--------------

.. py:exception:: ClusterChangeError

    A cluster state change occurred during the request. This may also be \
    returned by scan operations with the ``fail-on-cluster-change`` flag set.

    Error code: ``7``

    Subclass of :py:exc:`~aerospike.exception.ClusterError`.

.. py:exception:: ClusterError

    Cluster discovery and connection errors.

    Error code: ``11``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

Admin Errors
------------

.. py:exception:: AdminError

    The parent class for exceptions of the security API.

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

.. py:exception:: SecurityNotSupported

    Security functionality not supported by connected server.

    Error code: ``51``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: SecurityNotEnabled

    Security functionality not enabled by connected server.

    Error code: ``52``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: SecuritySchemeNotSupported

    Security scheme not supported.

    Error code: ``53``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidCommand

    Administration command is invalid.

    Error code: ``54``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidField

    Administration field is invalid.

    Error code: ``55``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: IllegalState

    Security protocol not followed.

    Error code: ``56``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidUser

    User name is invalid.

    Error code: ``60``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: UserExistsError

    User was previously created.

    Error code: ``61``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidPassword

    Error code: ``62``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: ExpiredPassword

    Error code: ``63``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: ForbiddenPassword

    Forbidden password (e.g. recently used)

    Error code: ``64``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidCredential

    Security credential is invalid.

    Error code: ``65``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidRole

    Role name is invalid.

    Error code: ``70``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: RoleExistsError

    Role already exists.

    Error code: ``71``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidPrivilege

    Error code: ``72``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidWhitelist

    Invalid IP whitelist.

    Error code: ``73``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: QuotasNotEnabled

    Quotas not enabled on server.

    Error code: ``74``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: InvalidQuota

    Error code: ``75``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: NotAuthenticated

    User must be authenticated before performing database operations.

    Error code: ``80``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: RoleViolation

    User does not possess the required role to perform the database operation.

    Error code: ``81``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: NotWhitelisted

    Command not allowed because sender IP not whitelisted.

    Error code: ``82``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

.. py:exception:: QuotaExceeded

    Error code: ``83``

    Subclass of :py:exc:`~aerospike.exception.AdminError`.

UDF Errors
----------

.. py:exception:: UDFError

    The parent class for UDF exceptions exceptions.

    Error code: ``100``

    Subclass of :py:exc:`~aerospike.exception.ServerError`.

    .. py:attribute:: module

        The UDF module associated with the error.

    .. py:attribute:: func

        Optionally the name of the UDF function.

.. py:exception:: UDFNotFound

    UDF does not exist.

    Error code: ``1301``

    Subclass of :py:exc:`~aerospike.exception.UDFError`.

.. py:exception:: LuaFileNotFound

    LUA file does not exist.

    Error code: ``1302``

    Subclass of :py:exc:`~aerospike.exception.UDFError`.

.. _indoubt:

In Doubt Status
---------------
  The ``in-doubt`` status of a caught exception can be checked by looking at the 5th element of its `args` tuple:

  .. code-block:: python

      key = 'test', 'demo', 1
      record = {'some': 'thing'}
      try:
        client.put(key, record)
      except AerospikeError as exc:
        print("The in doubt nature of the operation is: {}".format(exc.args[4])

.. versionadded:: 3.0.1

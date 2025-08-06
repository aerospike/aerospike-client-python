    .. hlist::
        :columns: 1
        * **max_retries** (:class:`int`)
            | Maximum number of retries before aborting the current command. The initial attempt is not counted as a
            | retry.
            |
            | If max_retries is exceeded, the command will return error ``AEROSPIKE_ERR_TIMEOUT``.
            |
            | Default: ``0``

            .. warning:: Database writes that are not idempotent (such as "add") should not be retried because the write operation may be performed multiple times \
                if the client timed out previous command attempts. It's important to use a distinct write policy for non-idempotent writes, which sets max_retries = `0`;

        * **sleep_between_retries** (:class:`int`)
            | Milliseconds to sleep between retries. Enter ``0`` to skip sleep.
            |
            | Default: ``0``
        * **socket_timeout** (:class:`int`)
            | Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not ``0`` and the socket has been idle for at least socket_timeout, both max_retries and
            | total_timeout are checked. If max_retries and total_timeout are not exceeded, the command is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to ``total_timeout``. \
                If ``socket_timeout`` is ``0``, there will be no socket idle limit.
            |
            | Default: ``30000``
        * **total_timeout** (:class:`int`)
            | Total command timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the command in the wire protocol.
            | The client will most likely timeout first, but the server also has the capability to timeout the command.
            |
            | If ``total_timeout`` is not ``0`` and ``total_timeout`` is reached before the command completes, the command will
            | return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is ``0``, there will be no total time limit.
            |
            | Default: ``1000``
        * **compress** (:class:`bool`)
            | Compress client requests and server responses.
            |
            | Use zlib compression on write or batch read commands when the command buffer size is greater than 128 bytes. In
            | addition, tell the server to compress it's response on read commands. The server response compression threshold is
            | also 128 bytes.
            |
            | This option will increase cpu and memory usage (for extra compressed buffers), but decrease the size of data sent
            | over the network.
            |
            | This compression feature requires the Enterprise Edition Server.
            |
            | Default: ``False``
        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a command.
            |
            | Default: None

            .. note:: Requires Aerospike server version >= 5.2.

        * .. include:: ./txn.rst

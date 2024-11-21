.. _aerospike.Transaction:

.. currentmodule:: aerospike

=================================================================
:class:`aerospike.Transaction` --- Multi Record Transaction Class
=================================================================

In the whole documentation, "commands" are all database commands that can be sent to the server (e.g single-record
operations, batch, scans, or queries).

"Transactions" link individual commands (except for scan and query) together so they can be rolled forward or
backward consistently.

Methods
=======

.. class:: Transaction

    Initialize multi-record transaction (MRT), assign random transaction id and initialize
    reads/writes hashmaps with default capacities.

    For both parameters, an unsigned 32-bit integer must be passed and the minimum value should be 16.

    :param reads_capacity: expected number of record reads in the MRT. Defaults to ``128``.
    :type reads_capacity: int, optional
    :param writes_capacity: expected number of record writes in the MRT. Defaults to ``128``.
    :type writes_capacity: int, optional

    .. py:attribute:: id

        Get the random transaction id that was generated on class instance creation.

        The value is an unsigned 64-bit integer.

        This attribute is read-only.

        :type: int
    .. py:attribute:: in_doubt

        This attribute is read-only.

        :type: bool
    .. py:attribute:: state

        One of the :ref:`mrt_state` constants.

        This attribute is read-only.

        :type: int
    .. py:attribute:: timeout

        MRT timeout in seconds. The timer starts when the MRT monitor record is created.
        This occurs when the first command in the MRT is executed. If the timeout is reached before
        :py:meth:`~aerospike.Client.commit` or :py:meth:`aerospike.Client.abort` is called, the server will expire and
        rollback the MRT.

        The default client MRT timeout is zero. This means use the server configuration ``mrt-duration``
        as the MRT timeout. The default ``mrt-duration`` is 10 seconds.

        This attribute can be read and written to.

        :type: int

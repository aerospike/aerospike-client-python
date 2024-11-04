.. _aerospike.Transaction:

.. currentmodule:: aerospike

=================================================================
:class:`aerospike.Transaction` --- Multi Record Transaction Class
=================================================================

In this documentation, "commands" are all database commands that can be sent to the server (e.g single-record
operations, batch, scans, or queries).

"Transactions" link individual commands (except for scan and query) together so they can be rolled forward or
backward consistently.

Methods
=======

.. class:: Transaction

    Initialize multi-record transaction (MRT), assign random transaction id and initialize
    reads/writes hashmaps with default capacities. The default MRT timeout is 10 seconds.

    If setting any parameters, both parameters must be specified.
    For both parameters, an unsigned 32-bit integer must be passed and the minimum value is 16.
    If neither is specified, both parameters are set to 128 by default.

    :param reads_capacity: expected number of record reads in the MRT.
    :type reads_capacity: int, optional
    :param writes_capacity: expected number of record writes in the MRT.
    :type writes_capacity: int, optional

    .. py:attribute:: id

        Get the random transaction id that was generated on class instance creation.

        The value is an unsigned 64-bit integer.

        :type: int
    .. py:attribute:: in_doubt
        :type: bool
    .. py:attribute:: state
        :type: int. One of the :ref:`mrt_state` constants.
    .. py:attribute:: timeout
        :type: int

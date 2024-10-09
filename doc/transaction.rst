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
    reads/writes hashmaps with given capacities. If settings any parameters, both parameters must be specified.

    :param reads_capacity: (unsigned 32-bit integer) expected number of record reads in the MRT. Minimum value is 16.
        If not specified, defaults to 128.
    :type reads_capacity: int, optional
    :param writes_capacity: (unsigned 32-bit integer) expected number of record writes in the MRT. Minimum value is 16.
        If not specified, defaults to 128.
    :type writes_capacity: int, optional

    :noindex:

    .. method:: id() -> int

        Get the random transaction id that was generated on class instance creation.

        The value is an unsigned 64-bit integer.

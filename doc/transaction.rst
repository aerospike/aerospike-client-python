.. _aerospike.Transaction:

.. currentmodule:: aerospike

=================================================================
:class:`aerospike.Transaction` --- Multi Record Transaction Class
=================================================================

In this documentation, "commands" are single-record operations, batch operations, scans, or queries.

"Transactions" link individual commands (except for scan and query) together so they can be rolled forward or
backward consistently.

Methods
=======

.. class:: Transaction
    :noindex:

    .. method:: id()

        Get the random transaction id that was generated on class instance creation.

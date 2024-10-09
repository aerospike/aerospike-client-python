*************************************************
Commands versus transactions
*************************************************

.. rubric:: What is the difference between "commands" and "transactions"?

In this documentation, "commands" are all database commands that can be sent to the server (e.g single-record
operations, batch, scans, or queries).

"Transactions" link individual commands (except for scan and query) together so they can be rolled forward or
backward consistently.

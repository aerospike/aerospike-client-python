.. note::

    When one node owns only one key from this batch (a node sub-batch of size 1),
    the client sends that key with the equivalent single-record command instead of
    the batch protocol. Do not special-case a one-key batch in application code.

**on_locking_only** (:class:`bool`)

    NOTE: this policy does not work for config level policies.

    Execute the write command only if the record is not already locked by this transaction.
    If this field is true and the record is already locked by this transaction, the command will
    raise an Aerospike exception with server error code ``AEROSPIKE_MRT_ALREADY_LOCKED`` (``126``).

    This field is useful for safely retrying non-idempotent writes as an alternative to simply
    aborting the transaction.

    Default: :py:obj:`False`

**key**

One of the :ref:`POLICY_KEY` values such as :data:`aerospike.POLICY_KEY_DIGEST`

This option is unique from other options such that it uses union precedence;
if this is set to :data:`aerospike.POLICY_KEY_SEND` either in dynamic config, config-level, or command-level,
:data:`aerospike.POLICY_KEY_SEND` will always be applied.

Default: :data:`aerospike.POLICY_KEY_DIGEST`

.. _aerospike_operation_helpers.expressions:

aerospike\_helpers\.expressions package
=======================================

Classes for the creation and use of Aerospike expressions.

Overview
--------

Aerospike expressions are a small domain specific language that allow for filtering
records in commands by manipulating and comparing bins and record metadata.
Expressions can be used everywhere that predicate expressions have been used and
allow for expanded functionality and customizability.

.. note::
  See `Expressions <https://aerospike.com/docs/develop/expressions>`_.

In the Python client, Aerospike expressions are built using a series of classes that represent
comparison and logical operators, bins, metadata operations, and bin operations.
Expressions are constructed using a Lisp like syntax by instantiating an expression that yields a boolean,
such as :meth:`~aerospike_helpers.expressions.base.Eq` or :meth:`~aerospike_helpers.expressions.base.And`,
while passing them other expressions and constants as arguments, and finally calling the
:meth:`~aerospike_helpers.expressions.resources._BaseExpr.compile` method.

Example:

.. testcode::

    # See if integer bin "bin_name" contains a value equal to 10.
    from aerospike_helpers import expressions as exp
    expr = exp.Eq(exp.IntBin("bin_name"), 10).compile()

By passing a compiled expression to a command via the "expressions" policy field,
the command will filter the results.

Example:

.. testsetup::

  import aerospike
  config = {"hosts": [("127.0.0.1", 3000)]}
  client = aerospike.client(config)

  keys = [("test", "demo", i) for i in range(1, 5)]
  client.batch_remove(keys=keys)

  client.close()

.. testcode::

  import aerospike
  from aerospike_helpers import expressions as exp
  import pprint

  # Connect to database
  config = {"hosts": [("127.0.0.1", 3000)]}
  client = aerospike.client(config)

  # Write player records to database
  keys = [("test", "demo", i) for i in range(1, 5)]
  records = [
              {'user': "Chief"  , 'scores': [6, 12, 4, 21], 'kd': 1.2},
              {'user': "Arbiter", 'scores': [5, 10, 5, 8] , 'kd': 1.0},
              {'user': "Johnson", 'scores': [8, 17, 20, 5], 'kd': 0.9},
              {'user': "Regret" , 'scores': [4, 2, 3, 5]  , 'kd': 0.3}
          ]
  for key, record in zip(keys, records):
      client.put(key, record)

  # Example #1: Get players with a K/D ratio >= 1.0

  kdGreaterThan1 = exp.GE(exp.FloatBin("kd"), 1.0).compile()
  policy = {"expressions": kdGreaterThan1}
  brs = client.batch_read(keys, policy=policy)

  # Pretty print records' bins
  for br in brs.batch_records:
      # error code for FILTERED_OUT = 27
      pprint.pprint(br.record[2] if br.result != 27 else None)

  # Example #2: Get player with scores higher than 20
  # By nesting expressions, we can create complicated filters

  # Get top score
  getTopScore = exp.ListGetByRank(
                  None,
                  aerospike.LIST_RETURN_VALUE,
                  exp.ResultType.INTEGER,
                  -1,
                  exp.ListBin("scores")
                  )
  # ...then compare it
  scoreHigherThan20 = exp.GE(getTopScore, 20).compile()
  policy = {"expressions": scoreHigherThan20}
  brs = client.batch_read(keys, policy=policy)

  for br in brs.batch_records:
      pprint.pprint(br.record[2] if br.result != 27 else None)

.. testoutput::

  {'kd': 1.2, 'scores': [6, 12, 4, 21], 'user': 'Chief'}
  {'kd': 1.0, 'scores': [5, 10, 5, 8], 'user': 'Arbiter'}
  None
  None
  {'kd': 1.2, 'scores': [6, 12, 4, 21], 'user': 'Chief'}
  None
  {'kd': 0.9, 'scores': [8, 17, 20, 5], 'user': 'Johnson'}
  None


Currently, Aerospike expressions are supported for:
  * Record commands
  * Batched commands
  * UDF apply methods (apply, scan apply, and query apply)
  * Query invoke methods (foreach, results, execute background)
  * Scan invoke methods (same as query invoke methods)

Filter Behavior
---------------

This section describes the behavior of methods when a record is filtered out by an expression.

For:
  * Record commands
  * Numeric operations
  * String operations

An exception :exc:`~aerospike.exception.FilteredOut` is thrown.

For:

  * :meth:`~aerospike.Client.batch_write` (records filtered out by a batch or batch record policy)
  * :meth:`~aerospike.Client.batch_operate` (records filtered out by a batch or batch write policy)
  * :meth:`~aerospike.Client.batch_apply` (records filtered out by a batch or batch apply policy)

The filtered out record's:

    * ``BatchRecord.record`` is set to :py:obj:`None`
    * ``BatchRecord.result`` is set to ``27``

Terminology
-----------

Aerospike expressions are evaluated server side, and expressions used for filtering are called **filter expressions**.
They do not return any values to the client or write any values to the server.

When the following documentation says an expression returns a **list expression**,
it means that the expression returns a list during evaluation on the server side.

Expressions used with :meth:`~aerospike_helpers.operations.expression_operations.expression_read`
or :meth:`~aerospike_helpers.operations.expression_operations.expression_write` do send their return values to the
client or write them to the server.
These expressions are called **operation expressions**.

When these docs say that an expression parameter requires an integer or **integer expression**,
it means it will accept a literal integer or an expression that will return an integer during evaluation.

When the docs say that an expression returns an **expression**,
this means that the data type returned may vary (usually depending on the ``return_type`` parameter).

.. note::

    Currently, Aerospike expressions for the python client do not support comparing ``as_python_bytes`` blobs.

    Only comparisons between **key ordered** map values and map expressions are supported.

.. note:: Requires server version >= 5.2.0

Assume all in-line examples run this code beforehand:

.. testsetup::

    import aerospike
    import aerospike_helpers.expressions as exp

.. code-block:: Python

    import aerospike
    import aerospike_helpers.expressions as exp

aerospike\_helpers\.expressions\.base module
---------------------------------------------

.. automodule:: aerospike_helpers.expressions.base
    :members:
    :special-members: __init__
    :show-inheritance:
    :private-members: _Key

aerospike\_helpers\.expressions\.list module
--------------------------------------------

.. automodule:: aerospike_helpers.expressions.list
    :members:
    :special-members: __init__

aerospike\_helpers\.expressions\.map module
-------------------------------------------

.. automodule:: aerospike_helpers.expressions.map
    :members:
    :special-members: __init__

aerospike\_helpers\.expressions\.bit module
-------------------------------------------

.. automodule:: aerospike_helpers.expressions.bitwise
    :members:
    :special-members: __init__

aerospike\_helpers\.expressions\.hll module
--------------------------------------------

.. automodule:: aerospike_helpers.expressions.hll
    :members:
    :special-members: __init__

aerospike\_helpers\.expressions\.arithmetic module
---------------------------------------------------

.. automodule:: aerospike_helpers.expressions.arithmetic
    :members:
    :special-members: __init__

aerospike\_helpers\.expressions\.bitwise_operators module
----------------------------------------------------------

.. automodule:: aerospike_helpers.expressions.bitwise_operators
    :members:
    :special-members: __init__

aerospike\_helpers\.expressions\.string module
----------------------------------------------

.. automodule:: aerospike_helpers.expressions.string
    :members:
    :special-members:

aerospike\_helpers\.expressions\.resources module
--------------------------------------------------

.. autodata:: aerospike_helpers.expressions.resources.TypeExpression

.. autoclass:: aerospike_helpers.expressions.resources.ResultType
  :members:
  :undoc-members:

.. autoclass:: aerospike_helpers.expressions.resources._BaseExpr

.. automethod:: aerospike_helpers.expressions.resources._BaseExpr.compile

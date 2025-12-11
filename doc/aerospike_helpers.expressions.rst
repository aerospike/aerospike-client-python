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
while passing them other expressions and constants as arguments, and finally calling the :meth:`compile` method.

Example::

    # See if integer bin "bin_name" contains a value equal to 10.
    from aerospike_helpers import expressions as exp
    expr = exp.Eq(exp.IntBin("bin_name"), 10).compile()

By passing a compiled expression to a command via the "expressions" policy field,
the command will filter the results.

Example:

.. include:: examples/expressions/top.py
  :code: python

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

Expression Type Aliases
-----------------------

The following documentation uses type aliases that map to standard Python types.

.. list-table:: Aliases to Standard Python Types
    :widths: 25 75
    :header-rows: 1

    * - Alias
      - Type
    * - AerospikeExpression
      - _BaseExpr
    * - TypeResultType
      - Optional[int]
    * - TypeFixedEle
      - Union[int, float, str, bytes, dict]
    * - TypeFixed
      - Optional[Dict[str, TypeFixedEle]]
    * - TypeCompiledOp
      - Tuple[int, TypeResultType, TypeFixed, int]
    * - TypeExpression
      - List[TypeCompiledOp]
    * - TypeChild
      - Union[int, float, str, bytes, _AtomExpr]
    * - TypeChildren
      - Tuple[TypeChild, ...]
    * - TypeBinName
      - Union[_BaseExpr, str]
    * - TypeListValue
      - Union[_BaseExpr, List[Any]]
    * - TypeIndex
      - Union[_BaseExpr, int, aerospike.CDTInfinite]
    * - TypeCTX
      - Union[None, List[cdt_ctx._cdt_ctx]]
    * - TypeRank
      - Union[_BaseExpr, int, aerospike.CDTInfinite]
    * - TypeCount
      - Union[_BaseExpr, int, aerospike.CDTInfinite]
    * - TypeValue
      - Union[_BaseExpr, Any]
    * - TypePolicy
      - Union[Dict[str, Any], None]
    * - TypeComparisonArg
      - Union[_BaseExpr, int, str, list, dict, aerospike.CDTInfinite]
    * - TypeGeo
      - Union[_BaseExpr, aerospike.GeoJSON]
    * - TypeKey
      - Union[_BaseExpr, Any]
    * - TypeKeyList
      - Union[_BaseExpr, List[Any]]
    * - TypeBitValue
      - Union[bytes, bytearray]
    * - TypeNumber
      - Union[_BaseExpr, int, float]
    * - TypeFloat
      - Union[_BaseExpr, float]
    * - TypeInteger
      - Union[_BaseExpr, int]
    * - TypeBool
      - Union[_BaseExpr, bool]

.. note:: Requires server version >= 5.2.0

Assume all in-line examples run this code beforehand::

    import aerospike
    import aerospike_helpers.expressions as exp

aerospike\_helpers\.expressions\.base module
---------------------------------------------

.. automodule:: aerospike_helpers.expressions.base
    :exclude-members: LoopVar, LoopVarMap, LoopVarList, LoopVarStr, LoopVarFloat, LoopVarInt, SelectByPath, ModifyByPath
    :members:
    :special-members:
    :show-inheritance:

aerospike\_helpers\.expressions\.list module
--------------------------------------------

.. automodule:: aerospike_helpers.expressions.list
    :members:
    :special-members:

aerospike\_helpers\.expressions\.map module
-------------------------------------------

.. automodule:: aerospike_helpers.expressions.map
    :members:
    :special-members:

aerospike\_helpers\.expressions\.bit module
-------------------------------------------

.. automodule:: aerospike_helpers.expressions.bitwise
    :members:
    :special-members:

aerospike\_helpers\.expressions\.hll module
--------------------------------------------

.. automodule:: aerospike_helpers.expressions.hll
    :members:
    :special-members:

aerospike\_helpers\.expressions\.arithmetic module
---------------------------------------------------

.. automodule:: aerospike_helpers.expressions.arithmetic
    :members:
    :special-members:

aerospike\_helpers\.expressions\.bitwise_operators module
----------------------------------------------------------

.. automodule:: aerospike_helpers.expressions.bitwise_operators
    :members:
    :special-members:


aerospike\_helpers\.expressions\.resources module
--------------------------------------------------

.. automodule:: aerospike_helpers.expressions.resources

    .. autoclass:: ResultType
      :members:
      :undoc-members:
      :member-order: bysource

.. _aerospike_operation_helpers.expressions:

aerospike\_helpers\.expressions package
=======================================

Classes for the creation and use of Aerospike Expressions. See:: `Aerospike Expressions <https://www.aerospike.com/docs/guide/expressions/>`_.

Aerospike Expressions are a small domain specific language that allow for filtering
records in transactions by manipulating and comparing bins and record metadata.
Expressions can be used everywhere that predicate expressions have been used and
allow for expanded functionality and customizability.

In the Python client, Aerospike expressions are built using a series of classes that represent
comparison and logical operators, bins, metadata operations, and bin operations.
Expressions are constructed using a Lisp like syntax by instantiating an expression that yields a boolean, such as Eq() or And(), 
while passing them other expressions and constants as arguments, and finally calling the compile() method. See the example below.

Example::

    # See if integer bin "bin_name" contains a value equal to 10.
    from aerospike_helpers import expressions as exp
    expr = exp.Eq(exp.IntBin("bin_name"), 10).compile()

By passing these compiled expressions to transactions via the "expressions" policy field,
the expressions will filter the results. See the example below.

Example::

    import aerospike
    from aerospike_helpers import expressions as exp
    from aerospike import exception as ex
    import sys

    TEST_NS = "test"
    TEST_SET = "demo"
    FIRST_RECORD_INDEX = 0
    SECOND_RECORD_INDEX = 1
    BIN_INDEX = 2

    # Configure the client.
    config = {"hosts": [("127.0.0.1", 3000)]}

    # Create a client and connect it to the cluster.
    try:
        client = aerospike.client(config).connect()
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    # Write records
    keys = [(TEST_NS, TEST_SET, i) for i in range(1, 5)]
    records = [
                {'user': "Chief"     , 'team': "blue", 'scores': [6, 12, 4, 21], 'kd': 1.0,  'status': "MasterPlatinum" },
                {'user': "Arbiter"   , 'team': "blue", 'scores': [5, 10, 5, 8] , 'kd': 1.0,  'status': "MasterGold"     },
                {'user': "Johnson"   , 'team': "blue", 'scores': [8, 17, 20, 5], 'kd': 0.99, 'status': "SergeantGold"   },
                {'user': "Regret"    , 'team': "red" , 'scores': [4, 2, 3, 5]  , 'kd': 0.33, 'status': "ProphetSilver"  }
            ]

    try:
        for key, record in zip(keys, records):
            client.put(key, record)
    except ex.RecordError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))


    # EXAMPLE 1: Get records for users who's top scores are above 20 using a scan.
    try:
        expr = exp.GT(exp.ListGetByRank(None, aerospike.LIST_RETURN_VALUE, exp.ResultType.INTEGER, -1, exp.ListBin("scores")), # rank -1 == largest value
                        20).compile()

        scan = client.scan(TEST_NS, TEST_SET)
        policy = {
            'expressions': expr
        }

        records = scan.results(policy)
        # This scan will only return the record for "Chief" since it is the only account with a score over 20 using batch get.
        print(records[FIRST_RECORD_INDEX][BIN_INDEX])
    except ex.AerospikeError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    # EXPECTED OUTPUT:
    # {'user': 'Chief', 'team': 'blue', 'scores': [6, 12, 4, 21], 'kd': 1.0, 'status': 'MasterPlatinum'}


    # EXAMPLE 2: Get player's records with a kd >= 1.0 with a status including "Gold".
    try:
        expr = exp.And(
            exp.CmpRegex(aerospike.REGEX_ICASE, '.*Gold', exp.StrBin('status')),
            exp.GE(exp.FloatBin("kd"), 1.0)).compile()

        policy = {
            'expressions': expr
        }

        records = client.get_many(keys, policy)
        # This get_many will only return the record for "Arbiter" since it is the only account with a kd >= 1.0 and Gold status.
        print(records[SECOND_RECORD_INDEX][BIN_INDEX])
    except ex.AerospikeError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)
    finally:
        client.close()

    # EXPECTED OUTPUT:
    # {'user': 'Arbiter', 'team': 'blue', 'scores': [5, 10, 5, 8], 'kd': 1.0, 'status': 'MasterGold'}

By nesting expressions, complicated filters can be created. See the example below.

Example::

    from aerospike_helpers import expressions as exp
    expr = Eq(
        exp.ListGetByIndexRangeToEnd(ctx, aerospike.LIST_RETURN_VALUE, 0,
            exp.ListSort(ctx, aerospike.LIST_SORT_DEFAULT,
                exp.ListAppend(ctx, policy, value_x,
                    exp.ListAppendItems(ctx, policy, value_y,
                        exp.ListInsert(ctx, policy, 1, value_z, bin_name))))),
        expected_answer
    ),



.. note::

    Aerospike expressions are evaluated server side, expressions used for filtering are called filter-expressions
    and do not return any values to the client or write any values to the server.

    When the following documentation says an expression returns a "list expression", it means that the expression returns a
    list during evalution on the server side.

    Expressions used with expression_read() or expression_write() do send their return values to the client or write them to the server.
    These expressions are called operation-expressions.

    When these docs say that an expression parameter requires an "integer or integer expression".
    It means it will accept a literal integer, or an expression that will return an integer during evaluation.

    When the docs say an expression returns an "expression" this means that the data type returned may vary, usually depending on the `return_type` parameter.

.. note::

    Currently, Aerospike expressions for the python client do not support comparing as_python_bytes blobs.
    Comparisons between constant map values and map expressions are  also unsupported.

Expression Type Aliases
-----------------------
The expressions module documentaiton uses type aliases.
The following is a table of how the type aliases map to standard types.

.. list-table:: Title
    :widths: 25 75
    :header-rows: 1

    * - Type Name
      - Type Description
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

aerospike\_helpers\.expressions\.base\ module
---------------------------------------------

.. automodule:: aerospike_helpers.expressions.base
    :members:
    :special-members:

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

aerospike\_helpers\.expressions\.hll\ module
--------------------------------------------

.. automodule:: aerospike_helpers.expressions.hll
    :members:
    :special-members:

aerospike\_helpers\.expressions\.arithmetic\ module
---------------------------------------------------

.. automodule:: aerospike_helpers.expressions.arithmetic
    :members:
    :special-members:

aerospike\_helpers\.expressions\.bitwise_operators\ module
----------------------------------------------------------

.. automodule:: aerospike_helpers.expressions.bitwise_operators
    :members:
    :special-members:


aerospike\_helpers\.expressions\.resources\ module
--------------------------------------------------

.. automodule:: aerospike_helpers.expressions.resources

    .. autoclass:: ResultType
      :members:
      :undoc-members:
      :member-order: bysource

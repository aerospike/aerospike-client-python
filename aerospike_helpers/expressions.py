'''
Classes for the creation and use of aerospike expressions. See:: `Aerospike Expressions <https://www.aerospike.com/docs/guide/expressions/>`_.

Aerospike Expressions are a small domain specific language that allow for filtering
records in transactions by manipulating and comparing bins and record metadata. 
Expressions can be used everywhere that predicate expressions have been used and
allow for expanded functionality and customizability.

In the Python client, Aerospike expressions are built using a series of classes that represent
comparrison and logical operators, bins, metadata operations, and bin operations.
Expressions are constructed using a Lisp like syntax by instantiating an expression that yields a boolean, such as Eq() or And(), 
while passing them other expressions and constants as arguments, and finally calling the compile() method. See the example below.

Example::

    # See if integer bin "bin_name" contains a value equal to 10.
    from aerospike_helpers import expressions as expr
    expr = expr.Eq(expr.IntBin("bin_name"), 10).compile()

By passing these compiled expressions to transactions via the "expressions" policy field,
the expressions will filter the results. See the example below.

Example::

    from __future__ import print_function
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
                {'user': "SgtJohnson", 'team': "blue", 'scores': [8, 17, 20, 5], 'kd': 0.99, 'status': "SergeantGold"   },
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
    
Note::

    Aerospike expressions are evaluated server side, and do not return any values to the client themselves.
    When the following documentation says an expression returns a "list expression", it means that the expression returns a
    list during evalution on the server side. When these docs say that a parameter requires an "integer or integer expression"
    It means it will accept a literal integer, or an expression that will return an integer during evaluation. When the docs say
    an expression returns a "expression" this means that the data type returned may vary, usually depending on the `return_type` parameter.

Current Limitations::

    Currently, Aerospike expressions for the python client do not support comparing as_python_bytes blobs.
    Comparrisions between constant map values and map expressions are  also unsupported.

The expressions module uses typehints, here are a table of custom typehints mapped to standard types.

.. list-table:: Title
    :widths: 25 75
    :header-rows: 1

    * - Type Name
      - Type Description
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
      - Union[BaseExpr, str]
    * - TypeListValue
      - Union[BaseExpr, List[Any]]
    * - TypeIndex
      - Union[BaseExpr, int, aerospike.CDTInfinite]
    * - TypeCDT
      - Union[None, List[cdt_ctx._cdt_ctx]]
    * - TypeRank
      - Union[BaseExpr, int, aerospike.CDTInfinite]
    * - TypeCount
      - Union[BaseExpr, int, aerospike.CDTInfinite]
    * - TypeValue
      - Union[BaseExpr, Any]
    * - TypePolicy
      - Union[Dict[str, Any], None]
    * - TypeComparisonArg
      - Union[BaseExpr, int, str, list, dict, aerospike.CDTInfinite]
    * - TypeGeo
      - Union[BaseExpr, aerospike.GeoJSON]
    * - TypeKey
      - Union[BaseExpr, Any]
    * - TypeKeyList
      - Union[BaseExpr, List[Any]]
    * - TypeBitValue
      - Union[bytes, bytearray]

'''


from itertools import chain
from typing import List, Optional, Tuple, Union, Dict, Any
import aerospike
from aerospike_helpers.operations import list_operations as lop
from aerospike_helpers import cdt_ctx


BIN_TYPE_KEY = "bin_type"
BIN_KEY = "bin"
INDEX_KEY = "index"
RETURN_TYPE_KEY = "return_type"
CTX_KEY = "ctx"
VALUE_KEY = "val"
VALUE_BEGIN_KEY = "value_begin"
VALUE_END_KEY = "value_end"
OP_TYPE_KEY = "ot_key"
LIST_POLICY_KEY = "list_policy"
MAP_POLICY_KEY = "map_policy"
LIST_ORDER_KEY = "list_order"
REGEX_OPTIONS_KEY = "regex_options"


class ExprOp:
    EQ = 1
    NE = 2
    GT = 3
    GE = 4
    LT = 5
    LE = 6
    CMP_REGEX = 7
    CMP_GEO = 8

    AND = 16
    OR = 17
    NOT = 18

    META_DIGEST_MOD = 64
    META_DEVICE_SIZE = 65
    META_LAST_UPDATE_TIME = 66
    META_VOID_TIME = 67
    META_TTL = 68
    META_SET_NAME = 69
    META_KEY_EXISTS = 70
    META_SINCE_UPDATE_TIME = 71
    META_IS_TOMBSTONE = 72

    REC_KEY = 80
    BIN = 81
    BIN_TYPE = 82
    BIN_EXISTS = 83

    CALL = 127

    VAL = 128
    PK = 129
    INT = 130
    UINT = 131
    FLOAT = 132
    BOOL = 133
    STR = 134
    BYTES = 135
    RAWSTR = 136
    RTYPE = 137

    NIL = 138

    # virtual ops

    _AS_EXP_CODE_CALL_VOP_START = 139
    _AS_EXP_CODE_CDT_LIST_CRMOD = 140
    _AS_EXP_CODE_CDT_LIST_MOD = 141
    _AS_EXP_CODE_CDT_MAP_CRMOD = 142
    _AS_EXP_CODE_CDT_MAP_CR = 143
    _AS_EXP_CODE_CDT_MAP_MOD = 144

    _AS_EXP_CODE_END_OF_VA_ARGS = 150

    _TRUE = 151
    _FALSE = 152


class ResultType:
    BOOLEAN = 1
    INTEGER = 2
    STRING = 3
    LIST = 4
    MAP = 5
    BLOB = 6
    FLOAT = 7
    GEOJSON = 8
    HLL = 9


class _AtomExpr:
    def _op(self):
        raise NotImplementedError

    def compile(self):
        raise NotImplementedError


TypeResultType = Optional[int]
TypeFixedEle = Union[int, float, str, bytes, dict]
TypeFixed = Optional[Dict[str, TypeFixedEle]]
TypeCompiledOp = Tuple[int, TypeResultType, TypeFixed, int]
TypeExpression = List[TypeCompiledOp]

TypeChild = Union[int, float, str, bytes, _AtomExpr]
TypeChildren = Tuple[TypeChild, ...]


class BaseExpr(_AtomExpr):
    op: int = 0
    rt: TypeResultType = None
    fixed: TypeFixed = None
    children: TypeChildren = ()

    def _op(self) -> TypeCompiledOp:
        return (self.op, self.rt, self.fixed, len(self.children))

    def _vop(self, v) -> TypeCompiledOp:
        op_type = 0

        return (
            0,
            None,
            {VALUE_KEY: v},
            0,
        )

    def compile(self) -> TypeExpression:
        expression: TypeExpression = [self._op()]
        work = chain(self.children)

        while True:
            try:
                item = next(work)
            except StopIteration:
                break

            if isinstance(item, BaseExpr):
                expression.append(item._op())
                work = chain(item.children, work)
            else:
                # Should be a str, bin, int, float, etc.
                expression.append(self._vop(item))

        return expression


class _GenericExpr(BaseExpr):
    
    def __init__(self, op: ExprOp, rt: TypeResultType, fixed: TypeFixed):
        self.op = op
        self.rt = rt
        self.fixed = fixed


###################
# Value Expressions
###################


class ExpTrue(BaseExpr):
    """
    Boolean True for use in aerospike expressions.
    """
    op = ExprOp._TRUE


class ExpFalse(BaseExpr):
    """
    Boolean False for use in aerospike expressions.
    """
    op = ExprOp._FALSE


########################
# Record Key Expressions
########################


class _Key(BaseExpr):
    op = ExprOp.REC_KEY


class KeyInt(_Key):
    """ Create an expression that returns the key as an integer. Returns 'unknown' if
        the key is not an integer.
    """
    rt = ResultType.INTEGER

    def __init__(self):
        """ Create an expression that returns the key as an integer. Returns 'unknown' if
            the key is not an integer.
        
            :return (integer value): Integer value of the key if the key is an integer.

            Example::
                # Integer record key >= 10000.
                expr = GE(KeyInt(), 10000).compile()
        """
        super().__init__()


class KeyStr(_Key):
    """ Create an expression that returns the key as a string. Returns 'unknown' if
        the key is not a string.
    """
    rt = ResultType.STRING

    def __init__(self):
        """ Create an expression that returns the key as a string. Returns 'unknown' if
            the key is not a string.
        
            :return (string value): string value of the key if the key is an string.

            Example::
                # string record key == "aaa".
                expr = Eq(KeyStr(), "aaa").compile()
        """ 
        super().__init__()


class KeyBlob(_Key):
    """ Create an expression that returns the key as a blob. Returns 'unknown' if
        the key is not a blob.
    """
    rt = ResultType.BLOB

    def __init__(self):
        """ Create an expression that returns the key as a blob. Returns 'unknown' if
            the key is not a blob.
        
            :return (blob value): Blob value of the key if the key is a blob.

            Example::
                # blob record key <= bytearray([0x65, 0x65]).
                expr = GE(KeyBlob(), bytearray([0x65, 0x65])).compile()
        """ 
        super().__init__()


class KeyExists(BaseExpr):
    """ Create an expression that returns if the primary key is stored in the record storage
        data as a boolean expression. This would occur on record write, when write policies set the `key` field to
        aerospike.POLICY_KEY_SEND.
    """
    op = ExprOp.META_KEY_EXISTS
    rt = ResultType.BOOLEAN

    def __init__(self):
        """ Create an expression that returns if the primary key is stored in the record storage
            data as a boolean expression. This would occur on record write, when write policies set the `key` field to
            aerospike.POLICY_KEY_SEND.
        
            :return (boolean value): True if the record has a stored key, false otherwise.

            Example::
                # Key exists in record meta data.
                expr = KeyExists().compile()
        """ 
        super().__init__()


#################
# Bin Expressions
#################


class IntBin(BaseExpr):
    """ Create an expression that returns a bin as an integer. Returns 'unknown'
        if the bin is not an integer.
    """
    op = ExprOp.BIN
    rt = ResultType.INTEGER

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as an integer. Returns 'unknown'
            if the bin is not an integer.

            Args:
                bin (str): Bin name.

            :return: (integer bin)
        
            Example::
                # Integer bin "a" == 200.
                expr = Eq(IntBin("a"), 200).compile()
        """        
        self.fixed = {BIN_KEY: bin}


class StrBin(BaseExpr):
    """ Create an expression that returns a bin as a string. Returns 'unknown'
        if the bin is not a string.
    """
    op = ExprOp.BIN
    rt = ResultType.STRING

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a string. Returns 'unknown'
            if the bin is not a string.

            Args:
                bin (str): Bin name.

            :return: (string bin)
        
            Example::
                # String bin "a" == "xyz".
                expr = Eq(StrBin("a"), "xyz").compile()
        """        
        self.fixed = {BIN_KEY: bin}


class FloatBin(BaseExpr):
    """ Create an expression that returns a bin as a float. Returns 'unknown'
        if the bin is not a float.
    """
    op = ExprOp.BIN
    rt = ResultType.FLOAT

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a float. Returns 'unknown'
            if the bin is not a float.

            Args:
                bin (str): Bin name.

            :return: (float bin)
        
            Example::
                # Float bin "a" > 2.71.
                expr = GT(FloatBin("a"), 2.71).compile()
        """        
        self.fixed = {BIN_KEY: bin}


class BlobBin(BaseExpr):
    """ Create an expression that returns a bin as a blob. Returns 'unknown'
        if the bin is not a blob.
    """
    op = ExprOp.BIN
    rt = ResultType.BLOB

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a blob. Returns 'unknown'
            if the bin is not a blob.

            Args:
                bin (str): Bin name.

            :return (blob bin)
        
            Example::
                #. Blob bin "a" == bytearray([0x65, 0x65])
                expr = Eq(BlobBin("a"), bytearray([0x65, 0x65])).compile()
        """        
        self.fixed = {BIN_KEY: bin}


class GeoBin(BaseExpr):
    """ Create an expression that returns a bin as a geojson. Returns 'unknown'
        if the bin is not a geojson.
    """
    op = ExprOp.BIN
    rt = ResultType.GEOJSON

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a geojson. Returns 'unknown'
            if the bin is not a geojson.

            Args:
                bin (str): Bin name.

            :return (geojson bin)
        
            Example::
                #GeoJSON bin "a" contained by GeoJSON bin "b".
                expr = CmpGeo(GeoBin("a"), GeoBin("b")).compile()
        """        
        self.fixed = {BIN_KEY: bin}


class ListBin(BaseExpr):
    """ Create an expression that returns a bin as a list. Returns 'unknown'
        if the bin is not a list.
    """
    op = ExprOp.BIN
    rt = ResultType.LIST

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a list. Returns 'unknown'
            if the bin is not a list.

            Args:
                bin (str): Bin name.

            :return (list bin)
        
            Example::
                # List bin "a" contains at least one item with value "abc".
                expr = GT(ListGetByValue(None, aerospike.LIST_RETURN_COUNT, 
                            ResultType.INTEGER, "abc", ListBin("a")), 
                        0).compile()
        """        
        self.fixed = {BIN_KEY: bin}


class MapBin(BaseExpr):
    """ Create an expression that returns a bin as a map. Returns 'unknown'
        if the bin is not a map.
    """
    op = ExprOp.BIN
    rt = ResultType.MAP

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a map. Returns 'unknown'
            if the bin is not a map.

            Args:
                bin (str): Bin name.

            :return (map bin)
        
            Example::
                # Map bin "a" size > 7.
                expr = GT(MapSize(None, MapBin("a")), 7).compile()
        """        
        self.fixed = {BIN_KEY: bin}


class HLLBin(BaseExpr):
    """ Create an expression that returns a bin as a HyperLogLog. Returns 'unknown'
        if the bin is not a HyperLogLog.
    """
    op = ExprOp.BIN
    rt = ResultType.HLL

    def __init__(self, bin: str):
        """ Create an expression that returns a bin as a HyperLogLog. Returns 'unknown'
            if the bin is not a HyperLogLog.

            Args:
                bin (str): Bin name.

            :return (HyperLogLog bin)
        
            Example::
                # Does HLL bin "a" have a hll_count > 1000000.
                expr = GT(HllGetCount(HllBin("a"), 1000000)).compile()
        """        
        self.fixed = {BIN_KEY: bin}


class BinExists(BaseExpr):
    """Create an expression that returns True if bin exists."""
    op = ExprOp.BIN_EXISTS
    rt = ResultType.BOOLEAN

    def __init__(self, bin: str):
        """ Create an expression that returns True if bin exists.

            Args:
                bin (str): bin name.

            :return (boolean value): True if bin exists, False otherwise.
        
            Example::
                #Bin "a" exists in record.
                expr = BinExists("a").compile()
        """        
        self.fixed = {BIN_KEY: bin}


class BinType(BaseExpr):
    """ Create an expression that returns the type of a bin
        as one of the aerospike :ref:`bin types <aerospike_bin_types>`
    """
    op = ExprOp.BIN_TYPE
    rt = ResultType.INTEGER

    def __init__(self, bin: str):
        """ Create an expression that returns the type of a bin
            as one of the aerospike :ref:`bin types <aerospike_bin_types>`.

            Args:
                bin (str): bin name.

            :return (integer value): returns the bin type.
        
            Example::
                # bin "a" == type string.
                expr = Eq(BinType("a"), aerospike.AS_BYTES_STRING).compile()
        """        
        self.fixed = {BIN_KEY: bin}


####################
# Record Expressions
####################


class SetName(BaseExpr):
    """ Create an expression that returns record set name string.
        This expression usually evaluates quickly because record
        meta data is cached in memory.
    """
    op = ExprOp.META_SET_NAME
    rt = ResultType.STRING

    def __init__(self):
        """ Create an expression that returns record set name string.
            This expression usually evaluates quickly because record
            meta data is cached in memory.

            :return (string value): Name of the set this record belongs to.
        
            Example::
                # Record set name == "myset".
                expr = Eq(SetName(), "myset").compile()
        """        
        super().__init__()


class DeviceSize(BaseExpr):
    """ Create an expression that returns record size on disk. If server storage-engine is
        memory, then zero is returned. This expression usually evaluates quickly
        because record meta data is cached in memory.
    """
    op = ExprOp.META_DEVICE_SIZE
    rt = ResultType.INTEGER

    def __init__(self):
        """ Create an expression that returns record size on disk. If server storage-engine is
            memory, then zero is returned. This expression usually evaluates quickly
            because record meta data is cached in memory.

            :return (integer value): Uncompressed storage size of the record.
        
            Example::
                # Record device size >= 100 KB.
                expr = GE(DeviceSize(), 100 * 1024).compile()
        """        
        super().__init__()


class LastUpdateTime(BaseExpr):
    """ Create an expression that the returns record last update time expressed as 64 bit
        integer nanoseconds since 1970-01-01 epoch.
    """
    op = ExprOp.META_LAST_UPDATE_TIME
    rt = ResultType.INTEGER

    def __init__(self):
        """ Create an expression that the returns record last update time expressed as 64 bit
            integer nanoseconds since 1970-01-01 epoch.

            :return (integer value): When the record was last updated.
        
            Example::
                # Record last update time >= 2020-01-15.
                expr = GE(LastUpdateTime(), 1577836800).compile()
        """        
        super().__init__()


class SinceUpdateTime(BaseExpr):
    """ Create an expression that returns milliseconds since the record was last updated.
        This expression usually evaluates quickly because record meta data is cached in memory.
    """
    op = ExprOp.META_SINCE_UPDATE_TIME
    rt = ResultType.INTEGER

    def __init__(self):
        """ Create an expression that returns milliseconds since the record was last updated.
            This expression usually evaluates quickly because record meta data is cached in memory.

            :return (integer value): Number of milliseconds since last updated.
        
            Example::
                # Record last updated more than 2 hours ago.
                expr = GT(SinceUpdateTime(), 2 * 60 * 1000).compile()
        """        
        super().__init__()    


class VoidTime(BaseExpr):
    """ Create an expression that returns record expiration time expressed as 64 bit
        integer nanoseconds since 1970-01-01 epoch.
    """
    op = ExprOp.META_VOID_TIME
    rt = ResultType.INTEGER

    def __init__(self):
        """ Create an expression that returns record expiration time expressed as 64 bit
            integer nanoseconds since 1970-01-01 epoch.

            :return (integer value): Expiration time in nanoseconds since 1970-01-01.
        
            Example::
                # Record expires on 2021-01-01.
                expr = And(
                        GE(VoidTime(), 1609459200),
                        LT(VoidTime(), 1609545600)).compile()
        """        
        super().__init__()  


class TTL(BaseExpr):
    """ Create an expression that returns record expiration time (time to live) in integer
        seconds.
    """
    op = ExprOp.META_TTL
    rt = ResultType.INTEGER

    def __init__(self):
        """ Create an expression that returns record expiration time (time to live) in integer
            seconds.

            :return (integer value): Number of seconds till the record will expire,
                                    returns -1 if the record never expires.
        
            Example::
                # Record expires in less than 1 hour.
                expr = LT(TTL(), 60 * 60).compile()
        """
        super().__init__()  


class IsTombstone(BaseExpr):
    """ Create an expression that returns if record has been deleted and is still in
        tombstone state. This expression usually evaluates quickly because record
        meta data is cached in memory. NOTE: this is only applicable for XDR filter expressions.
    """
    op = ExprOp.META_IS_TOMBSTONE
    rt = ResultType.BOOLEAN

    def __init__(self):
        """ Create an expression that returns if record has been deleted and is still in
            tombstone state. This expression usually evaluates quickly because record
            meta data is cached in memory. NOTE: this is only applicable for XDR filter expressions.

            :return (boolean value): True if the record is a tombstone, false otherwise.
        
            Example::
                # Detect deleted records that are in tombstone state.
                expr = IsTombstone().compile()
        """
        super().__init__() 


class DigestMod(BaseExpr):
    """Create an expression that returns record digest modulo as integer."""
    op = ExprOp.META_DIGEST_MOD
    rt = ResultType.INTEGER

    def __init__(self, mod: int):
        """ Create an expression that returns record digest modulo as integer.

            Args:
                mod (int): Divisor used to divide the digest to get a remainder.

            :return (integer value): Value in range 0 and mod (exclusive).
        
            Example::
                # Records that have digest(key) % 3 == 1.
                expr = Eq(DigestMod(3), 1).compile()
        """        
        self.fixed = {VALUE_KEY: mod}


########################
# Comparison Expressions
########################


TypeBinName = Union[BaseExpr, str]
TypeListValue = Union[BaseExpr, List[Any]]
TypeIndex = Union[BaseExpr, int, aerospike.CDTInfinite]
TypeCDT = Union[None, List[cdt_ctx._cdt_ctx]]
TypeRank = Union[BaseExpr, int, aerospike.CDTInfinite]
TypeCount = Union[BaseExpr, int, aerospike.CDTInfinite]
TypeValue = Union[BaseExpr, Any]
TypePolicy = Union[Dict[str, Any], None]
TypeComparisonArg = Union[BaseExpr, Any]
TypeGeo = Union[BaseExpr, aerospike.GeoJSON]


class Eq(BaseExpr):
    """Create an equals, (==) expression."""
    op = ExprOp.EQ

    def __init__(self, expr0: TypeComparisonArg, expr1: TypeComparisonArg):
        """ Create an equals, (==) expression.

        Args:
            expr0 (TypeComparisonArg): Left argument to `==`.
            expr1 (TypeComparisonArg): Right argument to `==`.

        :return: (boolean value)

        Example::
            # Integer bin "a" == 11
            expr = Eq(IntBin("a"), 11).compile()
        """        
        self.children = (expr0, expr1)


class NE(BaseExpr):
    """Create a not equals (not ==) expressions."""
    op = ExprOp.NE

    def __init__(self, expr0: TypeComparisonArg, expr1: TypeComparisonArg):
        """ Create a not equals (not ==) expressions.

            Args:
                expr0 (TypeComparisonArg): Left argument to `not ==`.
                expr1 (TypeComparisonArg): Right argument to `not ==`.
        
            :return: (boolean value)

            Example::
                # Integer bin "a" not == 13.
                expr = NE(IntBin("a"), 13).compile()
        """                 
        self.children = (expr0, expr1)


class GT(BaseExpr):
    """Create a greater than (>) expression."""
    op = ExprOp.GT

    def __init__(self, expr0: TypeComparisonArg, expr1: TypeComparisonArg):
        """ Create a greater than (>) expression.

            Args:
                expr0 (TypeComparisonArg): Left argument to `>`.
                expr1 (TypeComparisonArg): Right argument to `>`.
        
            :return: (boolean value)

            Example::
                # Integer bin "a" > 8.
                expr = GT(IntBin("a"), 8).compile()
        """
        self.children = (expr0, expr1)


class GE(BaseExpr):
    """Create a greater than or equal to (>=) expression."""
    op = ExprOp.GE

    def __init__(self, expr0: TypeComparisonArg, expr1: TypeComparisonArg):
        """ Create a greater than or equal to (>=) expression.

            Args:
                expr0 (TypeComparisonArg): Left argument to `>=`.
                expr1 (TypeComparisonArg): Right argument to `>=`.
        
            :return: (boolean value)

            Example::
                # Integer bin "a" >= 88.
                expr = GE(IntBin("a"), 88).compile()
        """
        self.children = (expr0, expr1)


class LT(BaseExpr):
    """Create a less than (<) expression."""
    op = ExprOp.LT

    def __init__(self, expr0: TypeComparisonArg, expr1: TypeComparisonArg):
        """ Create a less than (<) expression.

            Args:
                expr0 (TypeComparisonArg): Left argument to `<`.
                expr1 (TypeComparisonArg): Right argument to `<`.
        
            :return: (boolean value)

            Example::
                # Integer bin "a" < 1000.
                expr = LT(IntBin("a"), 1000).compile()
        """
        self.children = (expr0, expr1)


class LE(BaseExpr):
    """Create a less than or equal to (<=) expression."""
    op = ExprOp.LE

    def __init__(self, expr0: TypeComparisonArg, expr1: TypeComparisonArg):
        """ Create a less than or equal to (<=) expression.

            Args:
                expr0 (TypeComparisonArg): Left argument to `<=`.
                expr1 (TypeComparisonArg): Right argument to `<=`.
        
            :return: (boolean value)

            Example::
                # Integer bin "a" <= 1.
                expr = LE(IntBin("a"), 1).compile()
        """
        self.children = (expr0, expr1)


class CmpRegex(BaseExpr):
    """ Create an expression that performs a regex match on a string bin or value expression."""
    op = ExprOp.CMP_REGEX

    def __init__(self, options: int, regex_str: str, cmp_str: Union[BaseExpr, str]):
        """ Create an expression that performs a regex match on a string bin or value expression.

            Args:
                options (int) :ref:`regex_constants`: One of the aerospike regex constants, :ref:`regex_constants`.
                regex_str (str): POSIX regex string.
                cmp_str (Union[BaseExpr, str]): String expression to compare against.
        
            :return: (boolean value)

            Example::
                # Select string bin "a" that starts with "prefix" and ends with "suffix".
                # Ignore case and do not match newline.
                expr = CmpRegex(aerospike.REGEX_ICASE | aerospike.REGEX_NEWLINE, "prefix.*suffix", BinStr("a")).compile()
        """        
        self.children = (cmp_str,)
        self.fixed = {REGEX_OPTIONS_KEY: options, VALUE_KEY: regex_str}


class CmpGeo(BaseExpr):
    """Create a point within region or region contains point expression."""
    op = ExprOp.CMP_GEO

    def __init__(self, expr0: TypeGeo, expr1: TypeGeo):
        """ Create a point within region or region contains point expression.

            Args:
                expr0 (TypeGeo): Left expression in comparrison.
                expr1 (TypeGeo): Right expression in comparrison.
        
            :return: (boolean value)

            Example::
                # Geo bin "point" is within geo bin "region".
                expr = CmpGeo(GeoBin("point"), GeoBin("region")).compile()
        """        
        self.children = (expr0, expr1)


#####################
# Logical Expressions
#####################


class Not(BaseExpr):
    """Create a "not" (not) operator expression."""
    op = ExprOp.NOT

    def __init__(self, *exprs):
        """ Create a "not" (not) operator expression.

            Args:
                `*exprs` (BaseExpr): Variable amount of expressions to be negated.
        
            :return: (boolean value)

            Example::
                # not (a == 0 or a == 10)
                expr = Not(Or(
                            Eq(IntBin("a"), 0),
                            Eq(IntBin("a"), 10))).compile()
        """        
        self.children = exprs


class And(BaseExpr):
    """Create an "and" operator that applies to a variable amount of expressions."""
    op = ExprOp.AND

    def __init__(self, *exprs: BaseExpr):
        """ Create an "and" operator that applies to a variable amount of expressions.

        Args:
            `*exprs` (BaseExpr): Variable amount of expressions to be ANDed together.

        :return: (boolean value)

        Example::
            # (a > 5 || a == 0) && b < 3
            expr = And(
                Or(
                    GT(IntBin("a"), 5),
                    Eq(IntBin("a"), 0)),
                LT(IntBin("b"), 3)).compile()
        """
        self.children = exprs


class Or(BaseExpr):
    """Create an "or" operator that applies to a variable amount of expressions."""
    op = ExprOp.OR

    def __init__(self, *exprs):
        """ Create an "or" operator that applies to a variable amount of expressions.

        Args:
            `*exprs` (BaseExpr): Variable amount of expressions to be ORed together.

        :return: (boolean value)

        Example::
            # (a == 0 || b == 0)
            expr = Or(
                    Eq(IntBin("a"), 0),
                    Eq(IntBin("b"), 0)).compile()
        """ 
        self.children = exprs


######################
# List Mod Expressions
######################


class ListAppend(BaseExpr):
    """Create an expression that appends value to end of list."""
    op = aerospike.OP_LIST_APPEND

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, value: TypeValue, bin: TypeBinName):
        """ Create an expression that appends value to end of list.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                policy (TypePolicy): Optional dictionary of list write options :ref:`list write options <aerospike_list_policies>`.
                value (TypeValue): Value or value expression to append to list.
                bin (TypeBinName): List bin or list expression.

            :return: List expression.
        
            Example::
                # Check if length of list bin "a" is > 5 after appending 1 item.
                expr = GT(
                        ListSize(None, ListAppend(None, None, 3, ListBin("a"))),
                        5).compile()
        """        
        self.children = (
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_CRMOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[LIST_POLICY_KEY] = policy


class ListAppendItems(BaseExpr):
    """Create an expression that appends a list of items to the end of a list."""
    op = aerospike.OP_LIST_APPEND_ITEMS

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, value: TypeValue, bin: TypeBinName):
        """ Create an expression that appends a list of items to the end of a list.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                policy (TypePolicy): Optional dictionary of list write options :ref:`list write options <aerospike_list_policies>`.
                value (TypeValue): List or list expression of items to be appended.
                bin (TypeBinName): Bin name or list expression.

            :return: List expression.
        
            Example::
                # Check if length of list bin "a" is > 5 after appending multiple items.
                expr = GT(
                        ListSize(None, ListAppendItems(None, None, [3, 2], ListBin("a"))),
                        5).compile()
        """        
        self.children = (
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_CRMOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[LIST_POLICY_KEY] = policy


class ListInsert(BaseExpr):
    """Create an expression that inserts value to specified index of list."""
    op = aerospike.OP_LIST_INSERT

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, index: TypeIndex, value: TypeValue, bin: TypeBinName):
        """ Create an expression that inserts value to specified index of list.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                policy (TypePolicy): Optional dictionary of list write options :ref:`list write options <aerospike_list_policies>`.
                index (TypeIndex): Target index for insertion, integer or integer expression.
                value (TypeValue): Value or value expression to be inserted.
                bin (TypeBinName): Bin name or list expression.

            :return: List expression.
        
            Example::
                # Check if list bin "a" has length > 5 after insert.
                expr = GT(
                        ListSize(None, ListInsert(None, None, 0, 3, ListBin("a"))),
                        5).compile()
        """        
        self.children = (
            index,
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_MOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[LIST_POLICY_KEY] = policy


class ListInsertItems(BaseExpr):
    """Create an expression that inserts each input list item starting at specified index of list."""
    op = aerospike.OP_LIST_INSERT_ITEMS

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, index: TypeIndex, values: TypeListValue, bin: TypeBinName):
        """ Create an expression that inserts each input list item starting at specified index of list.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                policy (TypePolicy): Optional dictionary of list write options :ref:`list write options <aerospike_list_policies>`.Optional list write policy.
                index (TypeIndex): Target index where item insertion will begin, integer or integer expression.
                values (TypeListValue): List or list expression of items to be inserted.
                bin (TypeBinName): Bin name or list  expression.

            :return: List expression.
        
            Example::
                # Check if list bin "a" has length > 5 after inserting items.
                expr = GT(
                        ListSize(None, ListInsertItems(None, None, 0, [4, 7], ListBin("a"))),
                        5).compile()
        """        
        self.children = (
            index,
            values,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_MOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[LIST_POLICY_KEY] = policy


class ListIncrement(BaseExpr):
    """Create an expression that increments list[index] by value."""
    op = aerospike.OP_LIST_INCREMENT

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, index: TypeIndex, value: TypeValue, bin: TypeBinName):
        """ Create an expression that increments list[index] by value.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                policy (TypePolicy): Optional dictionary of list write options :ref:`list write options <aerospike_list_policies>`.Optional list write policy.
                index (TypeIndex): Index of value to increment.
                value (TypeValue): Value or value expression.
                bin (TypeBinName): Bin name or list expression.

            :return: List expression.
        
            Example::
                # Check if incremented value in list bin "a" is the largest in the list.
                expr = Eq(
                        ListGetByRank(None, aerospike.LIST_RETURN_VALUE, ResultType.INTEGER, -1, #rank of -1 == largest element.
                            ListIncrement(None, None, 1, 5, ListBin("a"))),
                        ListGetByIndex(None, aerospike.LIST_RETURN_VALUE, ResultType.INTEGER, 1,
                            ListIncrement(None, None, 1, 5, ListBin("a")))
                ).compile()
        """        
        self.children = (
            index,
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_CRMOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[LIST_POLICY_KEY] = policy


class ListSet(BaseExpr):
    """Create an expression that sets item value at specified index in list."""
    op = aerospike.OP_LIST_SET

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, index: TypeIndex, value: TypeValue, bin: TypeBinName):
        """ Create an expression that sets item value at specified index in list.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                policy (TypePolicy): Optional dictionary of list write options :ref:`list write options <aerospike_list_policies>`.Optional list write policy.
                index (TypeIndex): index of value to set.
                value (TypeValue): value or value expression to set index in list to.
                bin (TypeBinName): bin name or list expression.

            :return: List expression.
        
            Example::
                # Get smallest element in list bin "a" after setting index 1 to 10.
                expr = ListGetByRank(None, aerospike.LIST_RETURN_VALUE, ResultType.INTEGER, 0,
                                ListSet(None, None, 1, 10, ListBin("a"))).compile()
        """        
        self.children = (
            index,
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_MOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[LIST_POLICY_KEY] = policy


class ListClear(BaseExpr):
    """Create an expression that removes all items in a list."""
    op = aerospike.OP_LIST_CLEAR

    def __init__(self, ctx: TypeCDT, bin: TypeBinName):
        """ Create an expression that removes all items in a list.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                bin (TypeBinName): List bin or list expression to clear.

            :return: List expression.
        
            Example::
                # Clear list value of list nested in list bin "a" index 1.
                from aerospike_helpers import cdt_ctx
                expr = ListClear([cdt_ctx.cdt_ctx_list_index(1)], "a").compile()
        """        
        self.children = (
            bin if isinstance(bin, BaseExpr) else ListBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListSort(BaseExpr):
    """Create an expression that sorts a list."""
    op = aerospike.OP_LIST_SORT

    def __init__(self, ctx: TypeCDT, order: int, bin: TypeBinName):
        """ Create an expression that sorts a list.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                order (int): Optional flags modifiying the behavior of list_sort. This should be constructed by bitwise or'ing together values from :ref:`aerospike_list_sort_flag`.
                bin (TypeBinName): List bin name or list expression.

            :return: list expression.
        
            Example::
                # Get value of sorted list bin "a".
                expr = ListSort(None, aerospike.LIST_SORT_DEFAULT, "a").compile()
        """        
        self.children = (
            bin if isinstance(bin, BaseExpr) else ListBin(bin),
        )
        self.fixed = {LIST_ORDER_KEY: order}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByValue(BaseExpr):
    """Create an expression that removes list items identified by value."""
    op = aerospike.OP_LIST_REMOVE_BY_VALUE

    def __init__(self, ctx: TypeCDT, value: TypeValue, bin: TypeBinName):
        """ Create an expression that removes list items identified by value.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                value (TypeValue): Value or value expression to remove.
                bin (TypeBinName): bin name or bin expression.

            :return: list expression.
        
            Example::
                # See if list bin "a", with `3` removed, is equal to list bin "b".
                expr = Eq(ListRemoveByValue(None, 3, ListBin("a")), ListBin("b")).compile()
        """        
        self.children = (
            value,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByValueList(BaseExpr):
    """Create an expression that removes list items identified by values."""
    op = aerospike.OP_LIST_REMOVE_BY_VALUE_LIST

    def __init__(self, ctx: TypeCDT, values: TypeListValue, bin: TypeBinName):
        """ Create an expression that removes list items identified by values.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                values (TypeListValue): List of values or list expression.
                bin (TypeBinName): List bin name or list expression.

            :return: list expression.
        
            Example::
                # Remove elements with values [1, 2, 3] from list bin "a".
                expr = ListRemoveByValueList(None, [1, 2, 3], ListBin("a")).compile()
        """        
        self.children = (
            values,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByValueRange(BaseExpr):
    """ Create an expression that removes list items identified by value range
        (begin inclusive, end exclusive). If begin is None, the range is less than end.
        If end is None, the range is greater than or equal to begin.
    """
    op = aerospike.OP_LIST_REMOVE_BY_VALUE_RANGE

    def __init__(self, ctx: TypeCDT, begin: TypeValue, end: TypeValue, bin: TypeBinName):
        """ Create an expression that removes list items identified by value range
            (begin inclusive, end exclusive). If begin is None, the range is less than end.
            If end is None, the range is greater than or equal to begin.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                begin (TypeValue): Begin value or value expression for range.
                end (TypeValue): End value or value expression for range.
                bin (TypeBinName): List bin name or list expression.

            :return: list expression.
        
            Example::
                # Remove list of items with values >= 3 and < 7 from list bin "a".
                expr = ListRemoveByValueRange(None, 3, 7, ListBin("a")).compile()
        """        
        self.children = (
            begin,
            end,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByValueRelRankToEnd(BaseExpr):
    """Create an expression that removes list items nearest to value and greater by relative rank."""
    op = aerospike.OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, value: TypeValue, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that removes list items nearest to value and greater by relative rank.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                value (TypeValue): Start value or value expression.
                rank (TypeRank): Rank integer or integer expression.
                bin (TypeBinName): List bin name or list expression.

            :return: list expression.
        
            Example::
                # Remove elements larger than 4 by relative rank in list bin "a".
                expr = ListRemoveByValueRelRankToEnd(None, 4, 1, ListBin("a")).compile()
        """        
        self.children = (
            value,
            rank,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByValueRelRankRange(BaseExpr):
    """ Create an expression that removes list items nearest to value and greater by relative rank with a
        count limit.
    """
    op = aerospike.OP_LIST_REMOVE_BY_REL_RANK_RANGE

    def __init__(self, ctx: TypeCDT, value: TypeValue, rank: TypeRank, count: TypeCount, bin: TypeBinName):
        """ Create an expression that removes list items nearest to value and greater by relative rank with a
            count limit.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                value (TypeValue): Start value or value expression.
                rank (TypeRank): Rank integer or integer expression.
                count (TypeCount): How many elements to remove.
                bin (TypeBinName): List bin name or list expression.

            :return: list expression.
        
            Example::
                # After removing the 3 elements larger than 4 by relative rank, does list bin "a" include 9?.
                expr = GT(
                        ListGetByValue(None, aerospike.LIST_RETURN_COUNT, 9,
                            ListRemoveByValueRelRankRange(None, 4, 1, 0, ListBin("a"))),
                        0).compile()
        """        
        self.children = (
            value,
            rank,
            count,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByIndex(BaseExpr):
    """Create an expression that removes "count" list items starting at specified index."""
    op = aerospike.OP_LIST_REMOVE_BY_INDEX

    def __init__(self, ctx: TypeCDT, index: TypeIndex, bin: TypeBinName):
        """ Create an expression that removes "count" list items starting at specified index.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                index (TypeIndex): Index integer or integer expression of element to remove.
                bin (TypeBinName): List bin name or list expression.

            :return: list expression.
        
            Example::
                # Get size of list bin "a" after index 3 has been removed.
                expr = ListSize(None, ListRemoveByIndex(None, 3, ListBin("a"))).compile()
        """        
        self.children = (
            index,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByIndexRangeToEnd(BaseExpr):
    """Create an expression that removes list items starting at specified index to the end of list."""
    op = aerospike.OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, index: TypeIndex, bin: TypeBinName):
        """ Create an expression that removes list items starting at specified index to the end of list.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                index (TypeIndex): Starting index integer or integer expression of elements to remove.
                bin (TypeBinName): List bin name or list expression.

            :return: list expression.
        
            Example::
                # Remove all elements starting from index 3 in list bin "a".
                expr = ListRemoveByIndexRangeToEnd(None, 3, ListBin("a")).compile()
        """        
        self.children = (
            index,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByIndexRange(BaseExpr):
    """Create an expression that removes "count" list items starting at specified index."""
    op = aerospike.OP_LIST_REMOVE_BY_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, index: TypeIndex, count: TypeCount, bin: TypeBinName):
        """ Create an expression that removes "count" list items starting at specified index.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                index (TypeIndex): Starting index integer or integer expression of elements to remove.
                count (TypeCount): Integer or integer expression, how many elements to remove.
                bin (TypeBinName): List bin name or list expression.

            :return: list expression.
        
            Example::
                # Get size of list bin "a" after index 3, 4, and 5 have been removed.
                expr = ListSize(None, ListRemoveByIndexRange(None, 3, 3, ListBin("a"))).compile()
        """        
        self.children = (
            index,
            count,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByRank(BaseExpr):
    """Create an expression that removes list item identified by rank."""
    op = aerospike.OP_LIST_REMOVE_BY_RANK

    def __init__(self, ctx: TypeCDT, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that removes list item identified by rank.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                rank (TypeRank): Rank integer or integer expression of element to remove.
                bin (TypeBinName): List bin name or list expression.

            :return: list expression.
        
            Example::
                # Remove smallest value in list bin "a".
                expr = ListRemoveByRank(None, 0, ListBin("a")).compile()
        """        
        self.children = (
            rank,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByRankRangeToEnd(BaseExpr):
    """Create an expression that removes list items starting at specified rank to the last ranked item."""
    op = aerospike.OP_LIST_REMOVE_BY_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that removes list items starting at specified rank to the last ranked item.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                rank (TypeRank): Rank integer or integer expression of element to start removing at.
                bin (TypeBinName): List bin name or list expression.

            :return: list expression.
        
            Example::
                # Remove the 2 largest elements from List bin "a".
                expr = ListRemoveByRankRangeToEnd(None, -2, ListBin("a")).compile()
        """        
        self.children = (
            rank,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByRankRange(BaseExpr):
    """Create an expression that removes "count" list items starting at specified rank."""
    op = aerospike.OP_LIST_REMOVE_BY_RANK_RANGE

    def __init__(self, ctx: TypeCDT, rank: TypeRank, count: TypeCount, bin: TypeBinName):
        """ Create an expression that removes "count" list items starting at specified rank.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                rank (TypeRank): Rank integer or integer expression of element to start removing at.
                count (TypeCount): Count integer or integer expression of elements to remove.
                bin (TypeBinName): List bin name or list expression.

            :return: list expression.
        
            Example::
                # Remove the 3 smallest items from list bin "a".
                expr = ListRemoveByRankRange(None, 0, 3, ListBin("a")).compile()
        """        
        self.children = (
            rank,
            count,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


#######################
# List Read Expressions
#######################


class ListSize(BaseExpr):
    """Create an expression that returns list size."""
    op = aerospike.OP_LIST_SIZE

    def __init__(self, ctx: TypeCDT, bin: TypeBinName):
        """ Create an expression that returns list size.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                bin (TypeBinName): List bin name or list expression.

            :return: Integer expression.
        
            Example::
                #Take the size of list bin "a".
                expr = ListSize(None, ListBin("a")).compile()
        """        
        self.children = (
            bin if isinstance(bin, BaseExpr) else ListBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByValue(BaseExpr):
    """ Create an expression that selects list items identified by value and returns selected
        data specified by return_type.
    """
    op = aerospike.OP_LIST_GET_BY_VALUE

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeValue, bin: TypeBinName):
        """ Create an expression that selects list items identified by value and returns selected
            data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values  One of the aerospike list return types.
                value (TypeValue): Value or value expression of element to get.
                bin (TypeBinName): List bin name or list expression.

            :return: Expression.
        
            Example::
                # Get the index of the element with value, 3, in list bin "a".
                expr = ListGetByValue(None, aerospike.LIST_RETURN_INDEX, 3, ListBin("a")).compile()
        """        
        self.children = (
            value,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByValueRange(BaseExpr):
    """ Create an expression that selects list items identified by value range and returns selected
        data specified by return_type.
    """
    op = aerospike.OP_LIST_GET_BY_VALUE_RANGE

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_begin: TypeValue,
        value_end: TypeValue,
        bin: TypeBinName
    ):
        """ Create an expression that selects list items identified by value range and returns selected
            data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values  One of the aerospike list return types.
                value_begin (TypeValue): Value or value expression of first element to get.
                value_end (TypeValue): Value or value expression of ending element.
                bin (TypeBinName): List bin name or list expression.

            :return: Expression.
        
            Example::
                # Get rank of values between 3 (inclusive) and 7 (exclusive) in list bin "a".
                expr = ListGetByValueRange(None, aerospike.LIST_RETURN_RANK, 3, 7, ListBin("a")).compile()
        """        
        self.children = (
            value_begin,
            value_end,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByValueList(BaseExpr):
    """ Create an expression that selects list items identified by values and returns selected
        data specified by return_type.
    """
    op = aerospike.OP_LIST_GET_BY_VALUE_LIST

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeListValue, bin: TypeBinName):
        """ Create an expression that selects list items identified by values and returns selected
            data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values  Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values  One of the aerospike list return types.
                value (TypeListValue): List or list expression of values of elements to get.
                bin (TypeBinName): List bin name or list expression.

            :return: Expression.
        
            Example::
                #Get the indexes of the the elements in list bin "a" with values [3, 6, 12].
                expr = ListGetByValueList(None, aerospike.LIST_RETURN_INDEX, [3, 6, 12], ListBin("a")).compile()
        """        
        self.children = (
            value,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByValueRelRankRangeToEnd(BaseExpr):
    """Create an expression that selects list items nearest to value and greater by relative rank"""
    op = aerospike.OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeValue, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that selects list items nearest to value and greater by relative rank
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values  One of the aerospike list return types.
                value (TypeValue): Value or vaule expression to get items relative to.
                rank (TypeRank): Rank intger expression. rank relative to "value" to start getting elements.
                bin (TypeBinName): List bin name or list expression.

            :return: Expression.
        
            Example::
                # Get the values of all elements in list bin "a" larger than 3.
                expr = ListGetByValueRelRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 3, 1, ListBin("a")).compile()
        """        
        self.children = (
            value,
            rank,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByValueRelRankRange(BaseExpr):
    """ Create an expression that selects list items nearest to value and greater by relative rank with a
        count limit and returns selected data specified by return_type.
    """
    op = aerospike.OP_LIST_GET_BY_VALUE_RANK_RANGE_REL

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeValue, rank: TypeRank, count: TypeCount, bin: TypeBinName):
        """ Create an expression that selects list items nearest to value and greater by relative rank with a
            count limit and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values  One of the aerospike list return types.
                value (TypeValue): Value or vaule expression to get items relative to.
                rank (TypeRank): Rank intger expression. rank relative to "value" to start getting elements.
                count (TypeCount): Integer value or integer value expression, how many elements to get.
                bin (TypeBinName): List bin name or list expression.

            :return: Expression.
        
            Example::
                # Get the next 2 values in list bin "a" larger than 3.
                expr = ListGetByValueRelRankRange(None, aerospike.LIST_RETURN_VALUE, 3, 1, 2, ListBin("a")).compile()
        """        
        self.children = (
            value,
            rank,
            count,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByIndex(BaseExpr):
    """ Create an expression that selects list item identified by index
        and returns selected data specified by return_type.
    """
    op = aerospike.OP_LIST_GET_BY_INDEX

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_type: int,
        index: TypeIndex,
        bin: TypeBinName,
    ):
        """ Create an expression that selects list item identified by index
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values 
                value_type (int): The value type that will be returned by this expression (ResultType).
                index (TypeIndex): Integer or integer expression of index to get element at.
                bin (TypeBinName): List bin name or list expression.

            :return: Expression.
        
            Example::
                # Get the value at index 0 in list bin "a". (assume this value is an integer)
                expr = ListGetByIndex(None, aerospike.LIST_RETURN_VALUE, ResultType.INTEGER, 0, ListBin("a")).compile()
        """    
        self.children = (
            index,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {BIN_TYPE_KEY: value_type, RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByIndexRangeToEnd(BaseExpr):
    """ Create an expression that selects list items starting at specified index to the end of list
        and returns selected data specified by return_type.
    """
    op = aerospike.OP_LIST_GET_BY_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, index: TypeIndex, bin: TypeBinName):
        """ Create an expression that selects list items starting at specified index to the end of list
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values  One of the aerospike list return types.
                index (TypeIndex): Integer or integer expression of index to start getting elements at.
                bin (TypeBinName): List bin name or list expression.

            :return: Expression.
        
            Example::
                # Get element 5 to end from list bin "a".
                expr = ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 5, ListBin("a")).compile()
        """        
        self.children = (
            index,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByIndexRange(BaseExpr):
    """ Create an expression that selects "count" list items starting at specified index
        and returns selected data specified by return_type.
    """
    op = aerospike.OP_LIST_GET_BY_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, index: TypeIndex, count: TypeCount, bin: TypeBinName):
        """ Create an expression that selects "count" list items starting at specified index
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values  One of the aerospike list return types.
                index (TypeIndex): Integer or integer expression of index to start getting elements at.
                count (TypeCount): Integer or integer expression for count of elements to get.
                bin (TypeBinName): List bin name or list expression.

            :return: Expression.
        
            Example::
                # Get elements at indexes 3, 4, 5, 6 in list bin "a".
                expr = ListGetByIndexRange(None, aerospike.LIST_RETURN_VALUE, 3, 4, ListBin("a")).compile()
        """        
        self.children = (
            index,
            count,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByRank(BaseExpr):
    """ Create an expression that selects list item identified by rank
        and returns selected data specified by return_type.
    """
    op = aerospike.OP_LIST_GET_BY_RANK

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_type: int,
        rank: TypeRank,
        bin: TypeBinName,
    ):
        """ Create an expression that selects list item identified by rank
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values  One of the aerospike list return types.
                value_type (int): The value type that will be returned by this expression (ResultType).
                rank (TypeRank): Rank integer or integer expression of element to get.
                bin (TypeBinName): List bin name or list expression.

            :return: Expression.
        
            Example::
                # Get the smallest element in list bin "a".
                expr = ListGetByRank(None, aerospike.LIST_RETURN_VALUE, aerospike.ResultType.INTEGER, 0, ListBin("a")).compile()
        """        
        self.children = (
            rank,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {BIN_TYPE_KEY: value_type, RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByRankRangeToEnd(BaseExpr):
    """ Create an expression that selects list items starting at specified rank to the last ranked item
        and returns selected data specified by return_type.
    """
    op = aerospike.OP_LIST_GET_BY_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that selects list items starting at specified rank to the last ranked item
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values  One of the aerospike list return types.
                rank (TypeRank): Rank integer or integer expression of first element to get.
                bin (TypeBinName): List bin name or list expression.

            :return: Expression.
        
            Example::
                # Get the three largest elements in list bin "a".
                expr = ListGetByRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, -3, ListBin("a")).compile()
        """        
        self.children = (
            rank,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByRankRange(BaseExpr):
    """ Create an expression that selects "count" list items starting at specified rank
        and returns selected data specified by return_type.
    """
    op = aerospike.OP_LIST_GET_BY_RANK_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, rank: TypeRank, count: TypeCount, bin: TypeBinName):
        """ Create an expression that selects "count" list items starting at specified rank
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`list_return_types` values  One of the aerospike list return types.
                rank (TypeRank): Rank integer or integer expression of first element to get.
                count (TypeCount): Count integer or integer expression for how many elements to get.
                bin (TypeBinName): List bin name or list expression.

            :return: Expression.
        
            Example::
                # Get the 3 smallest elements in list bin "a".
                expr = ListGetByRankRange(None, aerospike.LIST_RETURN_VALUE, 0, 3, ListBin("a")).compile()
        """        
        self.children = (
            rank,
            count,
            bin if isinstance(bin, BaseExpr) else ListBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


########################
# Map Modify Expressions
########################


TypeKey = Union[BaseExpr, Any]
TypeKeyList = Union[BaseExpr, List[Any]]


class MapPut(BaseExpr):
    """Create an expression that writes key/val to map bin."""
    op = aerospike.OP_MAP_PUT

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, key: TypeKey, value: TypeValue, bin: TypeBinName):
        """ Create an expression that writes key/val to map bin.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                policy (TypePolicy): Optional map write policy.
                key (TypeKey): Key value or value expression to put into map.
                value (TypeValue): Value or value expression to put into map.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::
                # Put {27: 'key27'} into map bin "b".
                expr = MapPut(None, None, 27, 'key27', MapBin("b")).compile()
        """        
        self.children = (
            key,
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_MAP_CRMOD, 0, {MAP_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[MAP_POLICY_KEY] = policy


class MapPutItems(BaseExpr):
    """Create an expression that writes each map item to map bin."""
    op = aerospike.OP_MAP_PUT_ITEMS

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, map: map, bin: TypeBinName):
        """ Create an expression that writes each map item to map bin.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                policy (TypePolicy): Optional map write policy.
                map (map): Map or map expression of items to put into target map.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::
                # Put {27: 'key27', 28: 'key28'} into map bin "b".
                expr = MapPut(None, None, {27: 'key27', 28: 'key28'}, MapBin("b")).compile()
        """        
        self.children = (
            map,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_MAP_CRMOD, 0, {MAP_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[MAP_POLICY_KEY] = policy


class MapIncrement(BaseExpr):
    """ Create an expression that increments a map value, by value, for all items identified by key.
        Valid only for numbers.
    """
    op = aerospike.OP_MAP_INCREMENT

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, key: TypeKey, value: TypeValue, bin: TypeBinName):
        """ Create an expression that increments a map value, by value, for all items identified by key.
            Valid only for numbers.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                policy (TypePolicy): Optional map write policy.
                key (TypeKey): Key value or value expression element to increment.
                value (TypeValue): Increment element by value expression.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::
                # Increment element at 'vageta' in map bin "b" by 9000.
                expr = MapIncrement(None, None, 'vageta', 9000, MapBin("b")).compile()
        """        
        self.children = (
            key,
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_MAP_CRMOD, 0, {MAP_POLICY_KEY: policy} if policy is not None else {}),
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[MAP_POLICY_KEY] = policy


class MapClear(BaseExpr):
    """Create an expression that removes all items in map."""
    op = aerospike.OP_MAP_CLEAR

    def __init__(self, ctx: TypeCDT, bin: TypeBinName):
        """ Create an expression that removes all items in map.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::
                # Clear map bin "b".
                expr = MapClear(None, MapBin("b")).compile()
        """        
        self.children = (
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByKey(BaseExpr):
    """Create an expression that removes a map item identified by key."""
    op = aerospike.OP_MAP_REMOVE_BY_KEY

    def __init__(self, ctx: TypeCDT, key: TypeKey, bin: TypeBinName):
        """ Create an expression that removes a map item identified by key.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                key (TypeKey): Key value or value expression of key to element to remove.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::
                # Remove element at key 1 in map bin "b".
                expr = MapRemoveByKey(None, 1, MapBin("b")).compile()
        """        
        self.children = (
            key,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByKeyList(BaseExpr):
    """Create an expression that removes map items identified by keys."""
    op = aerospike.OP_MAP_REMOVE_BY_KEY_LIST

    def __init__(self, ctx: TypeCDT, keys: List[TypeKey], bin: TypeBinName):
        """ Create an expression that removes map items identified by keys.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                key (List[TypeKey]): List of key values or a list expression of keys to elements to remove.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::
                # Remove elements at keys [1, 2] in map bin "b".
                expr = MapRemoveByKeyList(None, [1, 2], MapBin("b")).compile()
        """        
        self.children = (
            keys,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByKeyRange(BaseExpr):
    """ Create an expression that removes map items identified by key range 
        (begin inclusive, end exclusive). If begin is None, the range is less than end.
        If end is None, the range is greater than equal to begin.
    """
    op = aerospike.OP_MAP_REMOVE_BY_KEY_RANGE

    def __init__(self, ctx: TypeCDT, begin: TypeValue, end: TypeValue, bin: TypeBinName):
        """ Create an expression that removes map items identified by key range 
            (begin inclusive, end exclusive). If begin is None, the range is less than end.
            If end is None, the range is greater than equal to begin.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                begin (TypeValue): Begin value expression.
                end (TypeValue): End value expression.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::
                # Remove elements at keys between 1 and 10 in map bin "b".
                expr = MapRemoveByKeyRange(None, 1, 10 MapBin("b")).compile()
        """        
        self.children = (
            begin,
            end,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByKeyRelIndexRangeToEnd(BaseExpr):
    """Create an expression that removes map items nearest to key and greater by index."""
    op = aerospike.OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, key: TypeKey, index: TypeIndex, bin: TypeBinName):
        """ Create an expression that removes map items nearest to key and greater by index.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                key (TypeKey): Key value or expression for key to start removing from.
                index (TypeIndex): Index integer or integer expression.
                bin (TypeBinName): Map bin or map expression.

            :return: Map expression.
        
            Example::
                # Map bin "b" has {"key1": 1, "key2": 2, "key3": 3, "key4": 4}.
                # Remove each element where the key has greater index than "key1".
                expr = MapRemoveByKeyRelIndexRangeToEnd(None, "key1", 1, MapBin("b")).compile()
        """        
        self.children = (
            key,
            index,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByKeyRelIndexRange(BaseExpr):
    """Create an expression that removes map items nearest to key and greater by index with a count limit."""
    op = aerospike.OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, key: TypeKey, index: TypeIndex, count: TypeCount, bin: TypeBinName):
        """ Create an expression that removes map items nearest to key and greater by index with a count limit.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                key (TypeKey): Key value or expression for key to start removing from.
                index (TypeIndex): Index integer or integer expression.
                count (TypeCount): Integer expression for how many elements to remove.
                bin (TypeBinName): Map bin or map expression.

            :return: [Map expression.
        
            Example::
                # Remove 3 elements with keys greater than "key1" from map bin "b".
                expr = MapRemoveByKeyRelIndexRange(None, "key1", 1, 3, MapBin("b")).compile()
        """        
        self.children = (
            key,
            index,
            count,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByValue(BaseExpr):
    """Create an expression that removes map items identified by value."""
    op = aerospike.OP_MAP_REMOVE_BY_VALUE

    def __init__(self, ctx: TypeCDT, value: TypeValue, bin: TypeBinName):
        """ Create an expression that removes map items identified by value.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                value (TypeValue): Value or value expression to remove.
                bin (TypeBinName): Bin name or map expression.

            :return: Map expression.
        
            Example::
                # Remove {"key1": 1} from map bin "b".
                expr = MapRemoveByValue(None, 1, MapBin("b")).compile()
        """        
        self.children = (
            value,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByValueList(BaseExpr):
    """Create an expression that removes map items identified by values."""
    op = aerospike.OP_MAP_REMOVE_BY_VALUE_LIST

    def __init__(self, ctx: TypeCDT, values: TypeListValue, bin: TypeBinName):
        """ Create an expression that removes map items identified by values.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                values (TypeListValue): List of values or list expression.
                bin (TypeBinName): Bin name or map expression.

            :return: Map expression.
        
            Example::
                # Remove elements with values 1, 2, 3 from map bin "b".
                expr = MapRemoveByValueList(None, [1, 2, 3], MapBin("b")).compile()
        """        
        self.children = (
            values,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByValueRange(BaseExpr):
    """ Create an expression that removes map items identified by value range
        (begin inclusive, end exclusive). If begin is nil, the range is less than end.
        If end is aerospike.CDTInfinite(), the range is greater than equal to begin.
    """
    op = aerospike.OP_MAP_REMOVE_BY_VALUE_RANGE

    def __init__(self, ctx: TypeCDT, begin: TypeValue, end: TypeValue, bin: TypeBinName):
        """ Create an expression that removes map items identified by value range
            (begin inclusive, end exclusive). If begin is nil, the range is less than end.
            If end is aerospike.CDTInfinite(), the range is greater than equal to begin.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                begin (TypeValue): Begin value or value expression for range.
                end (TypeValue): End value or value expression for range.
                bin (TypeBinName): Bin name or map expression.

            :return: Map expression.
        
            Example::
                # Remove list of items with values >= 3 and < 7 from map bin "b".
                expr = MapRemoveByValueRange(None, 3, 7, MapBin("b")).compile()
        """        
        self.children = (
            begin,
            end,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByValueRelRankRangeToEnd(BaseExpr):
    """Create an expression that removes map items nearest to value and greater by relative rank."""
    op = aerospike.OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, value: TypeValue, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that removes map items nearest to value and greater by relative rank.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                value (TypeValue): Value or value expression to start removing from.
                rank (TypeRank): Integer or integer expression of rank.
                bin (TypeBinName): Bin name or map expression.

            :return: Map expression.
        
            Example::
                # Remove all elements with values larger than 3 from map bin "b".
                expr = MapRemoveByValueRelRankRangeToEnd(None, 3, 1, MapBin("b")).compile()
        """        
        self.children = (
            value,
            rank,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByValueRelRankRange(BaseExpr):
    """ Create an expression that removes map items nearest to value and greater by relative rank with a
        count limit.
    """
    op = aerospike.OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE

    def __init__(self, ctx: TypeCDT, value: TypeValue, rank: TypeRank, count: TypeCount, bin: TypeBinName):
        """ Create an expression that removes map items nearest to value and greater by relative rank with a
            count limit.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                value (TypeValue): Value or value expression to start removing from.
                rank (TypeRank): Integer or integer expression of rank.
                count (TypeCount): Integer count or integer expression for how many elements to remove.
                bin (TypeBinName): Bin name or map expression.

            :return: Map expression.
        
            Example::
                # Remove the next 4 elements larger than 3 from map bin "b".
                expr = MapRemoveByValueRelRankRangeToEnd(None, 3, 1, 4, MapBin("b")).compile()
        """        
        self.children = (
            value,
            rank,
            count,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByIndex(BaseExpr):
    """Create an expression that removes map item identified by index."""
    op = aerospike.OP_MAP_REMOVE_BY_INDEX

    def __init__(self, ctx: TypeCDT, index: TypeIndex, bin: TypeBinName):
        """ Create an expression that removes map item identified by index.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                index (TypeIndex): Index integer or integer expression of element to remove.
                bin (TypeBinName): Bin name or map expression.

            :return: Map expression.
        
            Example::
                # Remove element with smallest key from map bin "b".
                expr = MapRemoveByIndex(None, 0, MapBin("b")).compile()
        """        
        self.children = (
            index,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByIndexRangeToEnd(BaseExpr):
    """Create an expression that removes map items starting at specified index to the end of map."""
    op = aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, index: TypeIndex, bin: TypeBinName):
        """ Create an expression that removes map items starting at specified index to the end of map.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                index (TypeIndex): Starting index integer or integer expression of elements to remove.
                bin (TypeBinName): Map bin name or map expression.

            :return: Map expression.
        
            Example::
                # Remove all elements starting from index 3 in map bin "b".
                expr = MapRemoveByIndexRangeToEnd(None, 3, MapBin("b")).compile()
        """        
        self.children = (
            index,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByIndexRange(BaseExpr):
    """Create an expression that removes count map items starting at specified index."""
    op = aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, index: TypeIndex, count: TypeCount, bin: TypeBinName):
        """ Create an expression that removes count map items starting at specified index.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                index (TypeIndex): Starting index integer or integer expression of elements to remove.
                count (TypeCount): Integer or integer expression, how many elements to remove.
                bin (TypeBinName): Map bin name or map expression.

            :return: Map expression.
        
            Example::
                # Get size of map bin "b" after index 3, 4, and 5 have been removed.
                expr = MapSize(None, MapRemoveByIndexRange(None, 3, 3, MapBin("b"))).compile()
        """        
        self.children = (
            index,
            count,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByRank(BaseExpr):
    """Create an expression that removes map item identified by rank."""
    op = aerospike.OP_MAP_REMOVE_BY_RANK

    def __init__(self, ctx: TypeCDT, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that removes map item identified by rank.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                rank (TypeRank): Rank integer or integer expression of element to remove.
                bin (TypeBinName): Map bin name or map expression.

            :return: Map expression.
        
            Example::
                # Remove smallest value in map bin "b".
                expr = MapRemoveByRank(None, 0, MapBin("b")).compile()
        """        
        self.children = (
            rank,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByRankRangeToEnd(BaseExpr):
    """Create an expression that removes map items starting at specified rank to the last ranked item."""
    op = aerospike.OP_MAP_REMOVE_BY_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that removes map items starting at specified rank to the last ranked item.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                rank (TypeRank): Rank integer or integer expression of element to start removing at.
                bin (TypeBinName): Map bin name or map expression.

            :return: Map expression.
        
            Example::
                # Remove the 2 largest elements from map bin "b".
                expr = MapRemoveByRankRangeToEnd(None, -2, MapBin("b")).compile()
        """        
        self.children = (
            rank,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByRankRange(BaseExpr):
    """Create an expression that removes "count" map items starting at specified rank."""
    op = aerospike.OP_MAP_REMOVE_BY_RANK_RANGE

    def __init__(self, ctx: TypeCDT, rank: TypeRank, count: TypeCount, bin: TypeBinName):
        """ Create an expression that removes "count" map items starting at specified rank.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                rank (TypeRank): Rank integer or integer expression of element to start removing at.
                count (TypeCount): Count integer or integer expression of elements to remove.
                bin (TypeBinName): Map bin name or map expression.

            :return: Map expression.
        
            Example::
                # Remove the 3 smallest items from map bin "b".
                expr = MapRemoveByRankRange(None, 0, 3, MapBin("b")).compile()
        """        
        self.children = (
            rank,
            count,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


######################
# Map Read Expressions
######################


class MapSize(BaseExpr):
    """Create an expression that returns map size."""
    op = aerospike.OP_MAP_SIZE

    def __init__(self, ctx: TypeCDT, bin: TypeBinName):
        """ Create an expression that returns map size.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                bin (TypeBinName): Map bin name or map expression.

            :return: Integer expression.
        
            Example::
                #Take the size of map bin "b".
                expr = MapSize(None, MapBin("b")).compile()
        """        
        self.children = (
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByKey(BaseExpr):
    """ Create an expression that selects map item identified by key
        and returns selected data specified by return_type.
    """
    op = aerospike.OP_MAP_GET_BY_KEY

    def __init__(self, ctx: TypeCDT, return_type: int, value_type: int, key: TypeKey, bin: TypeBinName):
        """ Create an expression that selects map item identified by key
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value_type (int): The value type that will be returned by this expression (ResultType).
                key (TypeKey): Key value or value expression of element to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get the value at key "key0" in map bin "b". (assume the value at key0 is an integer)
                expr = MapGetByKey(None, aerospike.MAP_RETURN_VALUE, ResultType.INTEGER, "key0", MapBin("b")).compile()
        """        
        self.children = (
            key,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {BIN_TYPE_KEY: value_type, RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByKeyRange(BaseExpr):
    """Create an expression that selects map items identified by key range."""
    op = aerospike.OP_MAP_GET_BY_KEY_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, begin: TypeKey, end: TypeKey, bin: TypeBinName):
        """ Create an expression that selects map items identified by key range
            (begin inclusive, end exclusive). If begin is nil, the range is less than end.
            If end is aerospike.CDTInfinite(), the range is greater than equal to begin.
            Expression returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                begin (TypeKey): Key value or expression.
                end (TypeKey): Key value or expression.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get elements at keys "key3", "key4", "key5", "key6" in map bin "b".
                expr = MapGetByKeyRange(None, aerospike.MAP_RETURN_VALUE, "key3", "key7", MapBin("b")).compile()
        """        
        self.children = (
            begin,
            end,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByKeyList(BaseExpr):
    """ Create an expression that selects map items identified by keys
        and returns selected data specified by return_type.
    """
    op = aerospike.OP_MAP_GET_BY_KEY_LIST

    def __init__(self, ctx: TypeCDT, return_type: int, keys: TypeKeyList, bin: TypeBinName):
        """ Create an expression that selects map items identified by keys
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                keys (TypeKeyList): List of key values or list expression.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get elements at keys "key3", "key4", "key5" in map bin "b".
                expr = MapGetByKeyList(None, aerospike.MAP_RETURN_VALUE, ["key3", "key4", "key5"], MapBin("b")).compile()
        """        
        self.children = (
            keys,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByKeyRelIndexRangeToEnd(BaseExpr):
    """Create an expression that selects map items nearest to key and greater by index with a count limit."""
    op = aerospike.OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, key: TypeKey, index: TypeIndex, bin: TypeBinName):
        """ Create an expression that selects map items nearest to key and greater by index with a count limit.
            Expression returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                key (TypeKey): Key value or value expression.
                index (TypeIndex): Index integer or integer value expression.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get elements with keys larger than "key2" from map bin "b".
                expr = MapGetByKeyRelIndexRangeToEnd(None, aerospike.MAP_RETURN_VALUE, "key2", 1, MapBin("b")).compile()
        """        
        self.children = (
            key,
            index,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByKeyRelIndexRange(BaseExpr):
    """Create an expression that selects map items nearest to key and greater by index with a count limit."""
    op = aerospike.OP_MAP_GET_BY_KEY_REL_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, key: TypeKey, index: TypeIndex, count: TypeCount, bin: TypeBinName):
        """ Create an expression that selects map items nearest to key and greater by index with a count limit.
            Expression returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                key (TypeKey): Key value or value expression.
                index (TypeIndex): Index integer or integer value expression.
                count (TypeCount): Integer count or integer value expression.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get the next 2 elements with keys larger than "key3" from map bin "b".
                expr = MapGetByKeyRelIndexRange(None, aerospike.MAP_RETURN_VALUE, "key3", 1, 2, MapBin("b")).compile()
        """        
        self.children = (
            key,
            index,
            count,
            bin if isinstance(bin, BaseExpr) else MapBin(bin),
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByValue(BaseExpr):
    """Create an expression that selects map items identified by value."""
    op = aerospike.OP_MAP_GET_BY_VALUE

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeValue, bin: TypeBinName):
        """ Create an expression that selects map items identified by value
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value (TypeValue): Value or value expression of element to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get the rank of the element with value, 3, in map bin "b".
                expr = MapGetByValue(None, aerospike.MAP_RETURN_RANK, 3, MapBin("b")).compile()
        """        
        self.children = (
            value,
            bin if isinstance(bin, BaseExpr) else MapBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByValueRange(BaseExpr):
    """ Create an expression that selects map items identified by value range."""
    op = aerospike.OP_MAP_GET_BY_VALUE_RANGE

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_begin: TypeValue,
        value_end: TypeValue,
        bin: TypeBinName
    ):
        """ Create an expression that selects map items identified by value range
            (begin inclusive, end exclusive). If begin is None, the range is less than end.
            If end is None, the range is greater than equal to begin.
            Expression returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value_begin (TypeValue): Value or value expression of first element to get.
                value_end (TypeValue): Value or value expression of ending element.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get elements with values between 3 and 7 from map bin "b".
                expr = MapGetByValueRange(None, aerospike.MAP_RETURN_VALUE, 3, 7, MapBin("b")).compile()
        """        
        self.children = (
            value_begin,
            value_end,
            bin if isinstance(bin, BaseExpr) else MapBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByValueList(BaseExpr):
    """Create an expression that selects map items identified by values."""
    op = aerospike.OP_MAP_GET_BY_VALUE_LIST

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeListValue, bin: TypeBinName):
        """ Create an expression that selects map items identified by values
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value (TypeListValue): List or list expression of values of elements to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get the indexes of the the elements in map bin "b" with values [3, 6, 12].
                expr = MapGetByValueList(None, aerospike.MAP_RETURN_INDEX, [3, 6, 12], MapBin("b")).compile()
        """        
        self.children = (
            value,
            bin if isinstance(bin, BaseExpr) else MapBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByValueRelRankRangeToEnd(BaseExpr):
    """Create an expression that selects map items nearest to value and greater by relative rank."""
    op = aerospike.OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeValue, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that selects map items nearest to value and greater by relative rank.
            Expression returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value (TypeValue): Value or vaule expression to get items relative to.
                rank (TypeRank): Rank intger expression. rank relative to "value" to start getting elements.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get the values of all elements in map bin "b" larger than 3.
                expr = MapGetByValueRelRankRangeToEnd(None, aerospike.MAP_RETURN_VALUE, 3, 1, MapBin("b")).compile()
        """        
        self.children = (
            value,
            rank,
            bin if isinstance(bin, BaseExpr) else MapBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByValueRelRankRange(BaseExpr):
    """ Create an expression that selects map items nearest to value and greater by relative rank with a
        count limit. Expression returns selected data specified by return_type.
    """
    op = aerospike.OP_MAP_GET_BY_VALUE_RANK_RANGE_REL

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeValue, rank: TypeRank, count: TypeCount, bin: TypeBinName):
        """ Create an expression that selects map items nearest to value and greater by relative rank with a
            count limit. Expression returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike list return types.
                value (TypeValue): Value or vaule expression to get items relative to.
                rank (TypeRank): Rank intger expression. rank relative to "value" to start getting elements.
                count (TypeCount): Integer value or integer value expression, how many elements to get.
                bin (TypeBinName): List bin name or list expression.

            :return: Expression.
        
            Example::
                # Get the next 2 values in map bin "b" larger than 3.
                expr = MapGetByValueRelRankRange(None, aerospike.MAP_RETURN_VALUE, 3, 1, 2, MapBin("b")).compile()
        """        
        self.children = (
            value,
            rank,
            count,
            bin if isinstance(bin, BaseExpr) else MapBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByIndex(BaseExpr):
    """ Create an expression that selects map item identified by index
        and returns selected data specified by return_type.
    """
    op = aerospike.OP_MAP_GET_BY_INDEX

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_type: int,
        index: TypeIndex,
        bin: TypeBinName,
    ):
        """ Create an expression that selects map item identified by index
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value_type (int): The value type that will be returned by this expression (ResultType).
                index (TypeIndex): Integer or integer expression of index to get element at.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get the value at index 0 in map bin "b". (assume this value is an integer)
                expr = MapGetByIndex(None, aerospike.MAP_RETURN_VALUE, ResultType.INTEGER, 0, MapBin("b")).compile()
        """        
        self.children = (
            index,
            bin if isinstance(bin, BaseExpr) else MapBin(bin)
        )
        self.fixed = {BIN_TYPE_KEY: value_type, RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByIndexRangeToEnd(BaseExpr):
    """Create an expression that selects map items starting at specified index to the end of map."""
    op = aerospike.OP_MAP_GET_BY_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, index: TypeIndex, bin: TypeBinName):
        """ Create an expression that selects map items starting at specified index to the end of map
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                index (TypeIndex): Integer or integer expression of index to start getting elements at.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get element at index 5 to end from map bin "b".
                expr = MapGetByIndexRangeToEnd(None, aerospike.MAP_RETURN_VALUE, 5, MapBin("b")).compile()
        """        
        self.children = (
            index,
            bin if isinstance(bin, BaseExpr) else MapBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByIndexRange(BaseExpr):
    """Create an expression that selects "count" map items starting at specified index."""
    op = aerospike.OP_MAP_GET_BY_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, index: TypeIndex, count: TypeCount, bin: TypeBinName):
        """ Create an expression that selects "count" map items starting at specified index
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                index (TypeIndex): Integer or integer expression of index to start getting elements at.
                count (TypeCount): Integer or integer expression for count of elements to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get elements at indexes 3, 4, 5, 6 in map bin "b".
                expr = MapGetByIndexRange(None, aerospike.MAP_RETURN_VALUE, 3, 4, MapBin("b")).compile()
        """        
        self.children = (
            index,
            count,
            bin if isinstance(bin, BaseExpr) else MapBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByRank(BaseExpr):
    """ Create an expression that selects map items identified by rank
        and returns selected data specified by return_type.
    """
    op = aerospike.OP_MAP_GET_BY_RANK

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_type: int,
        rank: TypeRank,
        bin: TypeBinName,
    ):
        """ Create an expression that selects map items identified by rank
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                value_type (int): The value type that will be returned by this expression (ResultType).
                rank (TypeRank): Rank integer or integer expression of element to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get the smallest element in map bin "b".
                expr = MapGetByRank(None, aerospike.MAP_RETURN_VALUE, aerospike.ResultType.INTEGER, 0, MapBin("b")).compile()
        """    
        self.children = (
            rank,
            bin if isinstance(bin, BaseExpr) else MapBin(bin)
        )
        self.fixed = {BIN_TYPE_KEY: value_type, RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByRankRangeToEnd(BaseExpr):
    """Create an expression that selects map items starting at specified rank to the last ranked item."""
    op = aerospike.OP_MAP_GET_BY_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, rank: TypeRank, bin: TypeBinName):
        """ Create an expression that selects map items starting at specified rank to the last ranked item
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                rank (TypeRank): Rank integer or integer expression of first element to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get the three largest elements in map bin "b".
                expr = MapGetByRankRangeToEnd(None, aerospike.MAP_RETURN_VALUE, -3, MapBin("b")).compile()
        """        
        self.children = (
            rank,
            bin if isinstance(bin, BaseExpr) else MapBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByRankRange(BaseExpr):
    """Create an expression that selects "count" map items starting at specified rank."""
    op = aerospike.OP_MAP_GET_BY_RANK_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, rank: TypeRank, count: TypeCount, bin: TypeBinName):
        """ Create an expression that selects "count" map items starting at specified rank
            and returns selected data specified by return_type.

            Args:
                ctx (TypeCDT): Optional context path for nested CDT.
                return_type (int): Value specifying what should be returned from the operation.
                    This should be one of the :ref:`map_return_types` values  One of the aerospike map return types.
                rank (TypeRank): Rank integer or integer expression of first element to get.
                count (TypeCount): Count integer or integer expression for how many elements to get.
                bin (TypeBinName): Map bin name or map expression.

            :return: Expression.
        
            Example::
                # Get the 3 smallest elements in map bin "b".
                expr = MapGetByRankRange(None, aerospike.MAP_RETURN_VALUE, 0, 3, MapBin("b")).compile()
        """        
        self.children = (
            rank,
            count,
            bin if isinstance(bin, BaseExpr) else MapBin(bin)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


########################
# Bit Modify Expressions
########################


TypeBitValue = Union[bytes, bytearray]


class BitResize(BaseExpr):
    """Create an expression that performs a bit_resize operation."""
    op = aerospike.OP_BIT_RESIZE

    def __init__(self, policy: TypePolicy, byte_size: int, flags: int, bin: TypeBinName):
        """ Create an expression that performs a bit_resize operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                byte_size (int): Number of bytes the resulting blob should occupy.
                flags (int): One or a combination of bit resize flags.
                bin (TypeBinName): Blob bin name or blob value expression.

            :return: Blob value expression of resized blob bin.
        
            Example::
                # Blob bin "c" == bytearray([1] * 5).
                # Resize blob bin "c" from the front so that the returned value is bytearray([0] * 5 + [1] * 5).
                expr = BitResize(None, 10, aerospike.BIT_RESIZE_FROM_FRONT, BlobBin("c")).compile()
        """        
        self.children = (
            byte_size,
            _GenericExpr(150, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            _GenericExpr(150, 0, {VALUE_KEY: flags} if flags is not None else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitInsert(BaseExpr):
    """Create an expression that performs a bit_insert operation."""
    op = aerospike.OP_BIT_INSERT

    def __init__(self, policy: TypePolicy, byte_offset: int, value: TypeBitValue, bin: TypeBinName):
        """ Create an expression that performs a bit_insert operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                byte_offset (int): Integer byte index of where to insert the value.
                value (TypeBitValue): A bytes value or blob value expression to insert.
                bin (TypeBinName): Blob bin name or blob value expression.

            :return: Resulting blob containing the inserted bytes.
        
            Example::
                # Let blob bin "c" == bytearray([1] * 5).
                # Insert 3 so that returned value is bytearray([1, 3, 1, 1, 1, 1]).
                expr = BitInsert(None, 1, bytearray([3]), BlobBin("c")).compile()
        """        
        self.children = (
            byte_offset,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitRemove(BaseExpr):
    """Create an expression that performs a bit_remove operation."""
    op = aerospike.OP_BIT_REMOVE

    def __init__(self, policy: TypePolicy, byte_offset: int, byte_size: int, bin: TypeBinName):
        """ Create an expression that performs a bit_remove operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                byte_offset (int): Index of value to remove.
                byte_size (int): Number of bytes to remove.
                bin (TypeBinName): Blob bin name or blob value expression.

            :return: Resulting blob containing the remaining bytes.
        
            Example::
                # Let blob bin "c" == bytearray([1] * 5).
                # Remove 1 element so that the returned value is bytearray([1] * 4).
                expr = BitRemove(None, 1, 1, BlobBin("c")).compile()
        """        
        self.children = (
            byte_offset,
            byte_size,
            _GenericExpr(150, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitSet(BaseExpr):
    """Create an expression that performs a bit_set operation."""
    op = aerospike.OP_BIT_SET

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: TypeBitValue, bin: TypeBinName):
        """ Create an expression that performs a bit_set operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start writing.
                bit_size (int): Number of bytes to overwrite.
                value (TypeBitValue): Bytes value or blob expression containing bytes to write.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob expression with the bytes overwritten.
        
            Example::
                # Let blob bin "c" == bytearray([0] * 5).
                # Set bit at offset 7 with size 1 bytes to 1 to make the returned value bytearray([1, 0, 0, 0, 0]).
                expr = BitSet(None, 7, 1, bytearray([1]), BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitOr(BaseExpr):
    """Create an expression that performs a bit_or operation."""
    op = aerospike.OP_BIT_OR

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: TypeBitValue, bin: TypeBinName):
        """ Create an expression that performs a bit_or operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bytes to be operated on.
                value (TypeBitValue): Bytes value or blob expression containing bytes to use in operation.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob with the bytes operated on.
        
            Example::
                # Let blob bin "c" == bytearray([1] * 5).
                # bitwise Or `8` with the first byte of blob bin c so that the returned value is bytearray([9, 1, 1, 1, 1]).
                expr = BitOr(None, 0, 8, bytearray([8]), BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitXor(BaseExpr):
    """Create an expression that performs a bit_xor operation."""
    op = aerospike.OP_BIT_XOR

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: TypeBitValue, bin: TypeBinName):
        """ Create an expression that performs a bit_xor operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bytes to be operated on.
                value (TypeBitValue): Bytes value or blob expression containing bytes to use in operation.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob with the bytes operated on.
        
            Example::
                # Let blob bin "c" == bytearray([1] * 5).
                # bitwise Xor `1` with the first byte of blob bin c so that the returned value is bytearray([0, 1, 1, 1, 1]).
                expr = BitXor(None, 0, 8, bytearray([1]), BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitAnd(BaseExpr):
    """Create an expression that performs a bit_and operation."""
    op = aerospike.OP_BIT_AND

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: TypeBitValue, bin: TypeBinName):
        """ Create an expression that performs a bit_and operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bytes to be operated on.
                value (TypeBitValue): Bytes value or blob expression containing bytes to use in operation.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob with the bytes operated on.
        
            Example::
                # Let blob bin "c" == bytearray([1] * 5).
                # bitwise and `0` with the first byte of blob bin c so that the returned value is bytearray([0, 5, 5, 5, 5]).
                expr = BitAnd(None, 0, 8, bytearray([0]), BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitNot(BaseExpr):
    """Create an expression that performs a bit_not operation."""
    op = aerospike.OP_BIT_NOT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, bin: TypeBinName):
        """ Create an expression that performs a bit_not operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bytes to be operated on.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob with the bytes operated on.
        
            Example::
                # Let blob bin "c" == bytearray([255] * 5).
                # bitwise, not, all of "c" to get bytearray([254] * 5).
                expr = BitNot(None, 0, 40, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            _GenericExpr(150, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitLeftShift(BaseExpr):
    """Create an expression that performs a bit_lshift operation."""
    op = aerospike.OP_BIT_LSHIFT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, shift: int, bin: TypeBinName):
        """ Create an expression that performs a bit_lshift operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bytes to be operated on.
                shift (int): Number of bits to shift by.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob with the bytes operated on.
        
            Example::
                # Let blob bin "c" == bytearray([1] * 5).
                # Bit left shift the first byte of bin "c" to get bytearray([8, 1, 1, 1, 1]).
                expr = BitLeftShift(None, 0, 8, 3, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            shift,
            _GenericExpr(150, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitRightShift(BaseExpr):
    """Create an expression that performs a bit_rshift operation."""
    op = aerospike.OP_BIT_RSHIFT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, shift: int, bin: TypeBinName):
        """ Create an expression that performs a bit_rshift operation.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bytes to be operated on.
                shift (int): Number of bits to shift by.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob with the bytes operated on.
        
            Example::
                # Let blob bin "c" == bytearray([8] * 5).
                # Bit left shift the first byte of bin "c" to get bytearray([4, 8, 8, 8, 8]).
                expr = BitRightShift(None, 0, 8, 1, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            shift,
            _GenericExpr(150, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitAdd(BaseExpr):
    """Create an expression that performs a bit_add operation."""
    op = aerospike.OP_BIT_ADD

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: int, action: int, bin: TypeBinName):
        """ Create an expression that performs a bit_add operation.
            Note: integers are stored big-endian.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bytes to be operated on.
                value (int): Integer value or expression for value to add.
                action (int): An aerospike bit overflow action.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: resulting blob with the bytes operated on.
        
            Example::
                # Let blob bin "c" == bytearray([1] * 5).
                # Bit add the second byte of bin "c" to get bytearray([1, 2, 1, 1, 1])
                expr = BitAdd(None, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            _GenericExpr(150, 0, {VALUE_KEY: action} if action is not None else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitSubtract(BaseExpr):
    """ Create an expression that performs a bit_subtract operation.
        Note: integers are stored big-endian.
    """
    op = aerospike.OP_BIT_SUBTRACT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: int, action: int, bin: TypeBinName):
        """ Create an expression that performs a bit_subtract operation.
            Note: integers are stored big-endian.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start operation.
                bit_size (int): Number of bytes to be operated on.
                value (int): Integer value or expression for value to add.
                action (int): An aerospike bit overflow action.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: resulting blob with the bytes operated on.
        
            Example::
                # Let blob bin "c" == bytearray([1] * 5).
                # Bit subtract the second byte of bin "c" to get bytearray([1, 0, 1, 1, 1])
                expr = BitSubtract(None, 8, 8, 1, aerospike.BIT_OVERFLOW_FAIL).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            _GenericExpr(150, 0, {VALUE_KEY: action} if action is not None else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitSetInt(BaseExpr):
    """ Create an expression that performs a bit_set_int operation.
        Note: integers are stored big-endian.
    """
    op = aerospike.OP_BIT_SET_INT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: int, bin: TypeBinName):
        """ Create an expression that performs a bit_set_int operation.
            Note: integers are stored big-endian.

            Args:
                policy (TypePolicy): An optional aerospike bit policy.
                bit_offset (int): Bit index of where to start writing.
                bit_size (int): Number of bytes to overwrite.
                value (int): Integer value or integer expression containing value to write.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Resulting blob expression with the bytes overwritten.
        
            Example::
                # Let blob bin "c" == bytearray([0] * 5).
                # Set bit at offset 7 with size 1 bytes to 1 to make the returned value bytearray([1, 0, 0, 0, 0]).
                expr = BitSetInt(None, 7, 1, 1, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['bit_write_flags']} if policy is not None and 'bit_write_flags' in policy else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


######################
# Bit Read Expressions
######################


class BitGet(BaseExpr):
    """Create an expression that performs a bit_get operation."""
    op = aerospike.OP_BIT_GET

    def __init__(self, bit_offset: int, bit_size: int, bin: TypeBinName):
        """ Create an expression that performs a bit_get operation.

            Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to get.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Blob, bit_size bits rounded up to the nearest byte size.
        
            Example::
                # Let blob bin "c" == bytearray([1, 2, 3, 4, 5).
                # Get 2 from bin "c".
                expr = BitGet(8, 8, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitCount(BaseExpr):
    """Create an expression that performs a bit_count operation."""
    op = aerospike.OP_BIT_COUNT

    def __init__(self, bit_offset: int, bit_size: int, bin: TypeBinName):
        """ Create an expression that performs a bit_count operation.

            Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to count.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Blob, bit_size bits rounded up to the nearest byte size.
        
            Example::
                # Let blob bin "c" == bytearray([3] * 5).
                # Count set bits starting at 3rd byte in bin "c" to get count of 6.
                expr = BitCount(16, 8 * 3, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitLeftScan(BaseExpr):
    """Create an expression that performs a bit_lscan operation."""
    op = aerospike.OP_BIT_LSCAN

    def __init__(self, bit_offset: int, bit_size: int, value: Union[ExpTrue, ExpFalse], bin: TypeBinName):
        """ Create an expression that performs a bit_lscan operation.

            Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to read.
                value Union[ExpTrue, ExpFalse]: Bit value to check for, ExpTrue for 1 or ExpFalse for 0.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Index of the left most bit starting from bit_offset set to value. Returns -1 if not found.
        
            Example::
                # Let blob bin "c" == bytearray([3] * 5).
                # Scan the first byte of bin "c" for the first bit set to 1. (should get 6)
                expr = BitLeftScan(0, 8, True, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitRightScan(BaseExpr):
    """Create an expression that performs a bit_rscan operation."""
    op = aerospike.OP_BIT_RSCAN

    def __init__(self, bit_offset: int, bit_size: int, value: Union[ExpTrue, ExpFalse], bin: TypeBinName):
        """ Create an expression that performs a bit_rscan operation.

            Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to read.
                value Union[ExpTrue, ExpFalse]: Bit value to check for, ExpTrue for 1 or ExpFalse for 0.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Index of the right most bit starting from bit_offset set to value. Returns -1 if not found.
        
            Example::
                # Let blob bin "c" == bytearray([3] * 5).
                # Scan the first byte of bin "c" for the right most bit set to 1. (should get 7)
                expr = BitRightScan(0, 8, True, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            value,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitGetInt(BaseExpr):
    """Create an expression that performs a bit_get_int operation."""
    op = aerospike.OP_BIT_GET_INT

    def __init__(self, bit_offset: int, bit_size: int, sign: Union[ExpTrue, ExpFalse], bin: TypeBinName):
        """ Create an expression that performs a bit_get_int operation.

            Args:
                bit_offset (int): Bit index of where to start reading.
                bit_size (int): Number of bits to get.
                sign Union[ExpTrue, ExpFalse]: ExpTrue for signed, ExpFalse for unsigned.
                bin (TypeBinName): Blob bin name or blob expression.

            :return: Integer expression.
        
            Example::
                # Let blob bin "c" == bytearray([1, 2, 3, 4, 5).
                # Get 2 as an integer from bin "c".
                expr = BitGetInt(8, 8, True, BlobBin("c")).compile()
        """        
        self.children = (
            bit_offset,
            bit_size,
            0 if not sign or isinstance(sign, ExpFalse) else 1,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


########################
# HLL Modify Expressions
########################


class HLLAdd(BaseExpr):
    """Create an expression that performs an hll_add."""
    op = aerospike.OP_HLL_ADD

    def __init__(self, policy: TypePolicy, list: TypeListValue, index_bit_count: Union[int, None], mh_bit_count: Union[int, None], bin: TypeBinName):
        """ Create an expression that performs an hll_add.

            Args:
                policy (TypePolicy): An optional aerospike HLL policy.
                list (TypeListValue): A list or list expression of elements to add to the HLL.
                index_bit_count (int): Number of index bits. Must be between 4 and 16 inclusive.
                mh_bit_count (int): Number of min hash bits. Must be between 4 and 51 inclusive.
                bin (TypeBinName): A hll bin name or bin expression to apply this function to.

            :return: Returns the resulting hll bin after adding elements from list.
        
            Example::
                # Let HLL bin "d" have the following elements, ['key1', 'key2', 'key3'], index_bits 8, mh_bits 8.
                # Add ['key4', 'key5', 'key6'] so that the returned value is ['key1', 'key2', 'key3', 'key4', 'key5', 'key6']
                expr = HLLAdd(None, ['key4', 'key5', 'key6'], 8, 8, HLLBin("d")).compile()
        """        
        self.children = (
            list,
            -1 if index_bit_count is None else index_bit_count,
            -1 if mh_bit_count is None else mh_bit_count,
            policy['flags'] if policy is not None and 'flags' in policy else aerospike.HLL_WRITE_DEFAULT,
            bin if isinstance(bin, BaseExpr) else HLLBin(bin)
        )


######################
# HLL Read Expressions
######################


class HLLGetCount(BaseExpr):
    """Create an expression that performs an as_operations_hll_get_count."""
    op = aerospike.OP_HLL_GET_COUNT

    def __init__(self, bin: TypeBinName):
        """ Create an expression that performs an as_operations_hll_get_count.

            Args:
                bin (TypeBinName): A hll bin name or bin expression to read from.

            :return: Integer bin, the estimated number of unique elements in an HLL.
        
            Example::
                # Get count from HLL bin "d".
                expr = HLLGetCount(HLLBin("d")).compile()
        """        
        self.children = (
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )


class HLLGetUnion(BaseExpr):
    """Create an expression that performs an hll_get_union."""
    op = aerospike.OP_HLL_GET_UNION

    def __init__(self, list: TypeListValue, bin: TypeBinName):
        """ Create an expression that performs an hll_get_union.

            Args:
                list (TypeListValue): A list or list expression of HLLs to union with.
                bin (TypeBinName): A hll bin name or bin expression to read from.

            :return: HLL bin representing the set union.
        
            Example::
                # Let HLLBin "d" contain keys ['key%s' % str(i) for i in range(10000)].
                # Let list be a list containing HLL objects retrieved from the aerospike database.
                # Find the union of HLL bin "d" and all HLLs in list.
                expr = HLLGetUnion(list, HLLBin("d")).compile()
        """        
        self.children = (
            list,
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )


class HLLGetUnionCount(BaseExpr):
    """Create an expression that performs an as_operations_hll_get_union_count."""
    op = aerospike.OP_HLL_GET_UNION_COUNT

    def __init__(self, list: TypeListValue, bin: TypeBinName):
        """ Create an expression that performs an as_operations_hll_get_union_count.

            Args:
                list (TypeListValue): A list expression of HLLs to union with.
                bin (TypeBinName): A hll bin name or bin expression to read from.

            :return: Integer bin, estimated number of elements in the set union.
        
            Example::
                # Let HLLBin "d" contain keys ['key%s' % str(i) for i in range(10000)].
                # Let list be a list containing one HLL object with keys ['key%s' % str(i) for i in range(5000, 15000)].
                # Find the count of keys in the union of HLL bin "d" and all HLLs in list. (Should be around 15000)
                expr = HLLGetUnionCount(list, HLLBin("d")).compile()
        """        
        self.children = (
            list,
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )


class HLLGetIntersectCount(BaseExpr):
    """Create an expression that performs an as_operations_hll_get_inersect_count."""
    op = aerospike.OP_HLL_GET_INTERSECT_COUNT

    def __init__(self, list: TypeListValue, bin: TypeBinName):
        """ Create an expression that performs an as_operations_hll_get_inersect_count.

            Args:
                list (TypeListValue): A list expression of HLLs to intersect with.
                bin (TypeBinName): A hll bin name or bin expression to read from.

            :return: Integer bin, estimated number of elements in the set intersection.
        
            Example::
                # Let HLLBin "d" contain keys ['key%s' % str(i) for i in range(10000)].
                # Let list be a list containing one HLL object with keys ['key%s' % str(i) for i in range(5000, 15000)].
                # Find the count of keys in the intersection of HLL bin "d" and all HLLs in list. (Should be around 5000)
                expr = HLLGetIntersectCount(list, HLLBin("d")).compile()
        """        
        self.children = (
            list,
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )


class HLLGetSimilarity(BaseExpr):
    """Create an expression that performs an as_operations_hll_get_similarity."""
    op = aerospike.OP_HLL_GET_SIMILARITY

    def __init__(self, list: TypeListValue, bin: TypeBinName):
        """ Create an expression that performs an as_operations_hll_get_similarity.

            Args:
                list (TypeListValue): A list expression of HLLs to calculate similarity with.
                bin (TypeBinName): A hll bin name or bin expression to read from.

            :return: Float bin, stimated similarity between 0.0 and 1.0.
        
            Example::
                # Let HLLBin "d" contain keys ['key%s' % str(i) for i in range(10000)].
                # Let list be a list containing one HLL object with keys ['key%s' % str(i) for i in range(5000, 15000)].
                # Find the similarity the HLL in list to HLL bin "d". (Should be around 0.33)
                # Note that similarity is defined as intersect(A, B, ...) / union(A, B, ...).
                expr = HLLGetSimilarity(list, HLLBin("d")).compile()
        """        
        self.children = (
            list,
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )


class HLLDescribe(BaseExpr):
    """Create an expression that performs an as_operations_hll_describe."""
    op = aerospike.OP_HLL_DESCRIBE

    def __init__(self, bin: TypeBinName):
        """ Create an expression that performs an as_operations_hll_describe.

            Args:
                bin (TypeBinName): A hll bin name or bin expression to read from.

            :return: List bin, a list containing the index_bit_count and minhash_bit_count.
        
            Example::
                # Get description of HLL bin "d".
                expr = HLLDescribe(HLLBin("d")).compile()
        """        
        self.children = (
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )


class HLLMayContain(BaseExpr):
    """ Create an expression that checks if the HLL bin contains any keys in
        list.
    """
    op = aerospike.OP_HLL_MAY_CONTAIN

    def __init__(self, list: TypeListValue, bin: TypeBinName, ):
        """ Create an expression that checks if the HLL bin contains any keys in
            list.

            Args:
                list (TypeListValue): A list expression of keys to check if the HLL may contain them.
                bin (TypeBinName): Integer bin, a hll bin name or bin expression to read from.

            :return: 1 if bin contains any key in list, 0 otherwise.
        
            Example::
                # Check if HLL bin "d" contains any of the keys in `list`.
                expr = HLLMayContain(["key1", "key2", "key3"], HLLBin("d")).compile()
        """        
        self.children = (
            list,
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )

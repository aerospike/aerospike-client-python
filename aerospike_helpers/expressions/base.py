'''
Base expressions include operators, bin, and meta data related expressions.

Example::

    import aerospike_helpers.expressions as exp
    # See if integer bin "bin_name" contains a value equal to 10.
    expr = exp.Eq(exp.IntBin("bin_name"), 10).compile()
'''

from itertools import chain
from typing import List, Optional, Tuple, Union, Dict, Any
import aerospike
from aerospike_helpers import cdt_ctx


VALUE_TYPE_KEY = "value_type"
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

    _AS_EXP_BIT_FLAGS = 153


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
        self.children = exprs + (_GenericExpr(ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)


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
        self.children = exprs + (_GenericExpr(ExprOp._AS_EXP_CODE_END_OF_VA_ARGS, 0, {}),)

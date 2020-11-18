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
BIT_POLICY_KEY = "bit_policy"
BIT_FLAGS_KEY = "policy"
RESIZE_FLAGS_KEY = "resize_flags"
PARAM_COUNT_KEY = "param_count"
EXTRA_PARAM_COUNT_KEY = "extra_param_count"
LIST_ORDER_KEY = "list_order"
REGEX_OPTIONS_KEY = "regex_options"

# TODO change list ops to send call op type and their vtype,
# that way the switch statement in convert_predexp.c can be reduced to 1 template

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
    #LIST_MOD = 139
    # _AS_EXP_CODE_AS_VAL = 134
    # _AS_EXP_CODE_VAL_PK = 135
    # _AS_EXP_CODE_VAL_INT = 136
    # _AS_EXP_CODE_VAL_UINT = 137
    # _AS_EXP_CODE_VAL_FLOAT = 138
    # _AS_EXP_CODE_VAL_BOOL = 139
    # _AS_EXP_CODE_VAL_STR = 140
    # _AS_EXP_CODE_VAL_BYTES = 141
    # _AS_EXP_CODE_VAL_RAWSTR = 142
    # _AS_EXP_CODE_VAL_RTYPE = 143

    _AS_EXP_CODE_CALL_VOP_START = 139
    _AS_EXP_CODE_CDT_LIST_CRMOD = 140
    _AS_EXP_CODE_CDT_LIST_MOD = 141
    _AS_EXP_CODE_CDT_MAP_CRMOD = 142
    _AS_EXP_CODE_CDT_MAP_CR = 143
    _AS_EXP_CODE_CDT_MAP_MOD = 144

    _AS_EXP_CODE_END_OF_VA_ARGS = 150


    # LIST_SORT = 128
    # LIST_APPEND = 129
    # LIST_APPEND_ITEMS = 130
    # LIST_INSERT = 131
    # LIST_INSERT_ITEMS = 132
    # LIST_INCREMENT = 133
    # LIST_SET = 134
    # LIST_REMOVE_BY_VALUE = 135
    # LIST_ = 136
    # LIST_SORT = 137


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


class CallType:
    CDT = 0
    BIT = 1
    HLL = 2

    MODIFY = 0x40


class ListOpType:
	AS_CDT_OP_LIST_SET_TYPE = 0,
	AS_CDT_OP_LIST_APPEND = 1,
	AS_CDT_OP_LIST_APPEND_ITEMS = 2,
	AS_CDT_OP_LIST_INSERT = 3,
	AS_CDT_OP_LIST_INSERT_ITEMS = 4,
	AS_CDT_OP_LIST_POP = 5,
	AS_CDT_OP_LIST_POP_RANGE = 6,
	AS_CDT_OP_LIST_REMOVE = 7,
	AS_CDT_OP_LIST_REMOVE_RANGE = 8,
	AS_CDT_OP_LIST_SET = 9,
	AS_CDT_OP_LIST_TRIM = 10,
	AS_CDT_OP_LIST_CLEAR = 11,
	AS_CDT_OP_LIST_INCREMENT = 12,
	AS_CDT_OP_LIST_SORT = 13,
	AS_CDT_OP_LIST_SIZE = 16,
	AS_CDT_OP_LIST_GET = 17,
	AS_CDT_OP_LIST_GET_RANGE = 18,
	AS_CDT_OP_LIST_GET_BY_INDEX = 19,
	AS_CDT_OP_LIST_GET_BY_RANK = 21,
	AS_CDT_OP_LIST_GET_ALL_BY_VALUE = 22,
	AS_CDT_OP_LIST_GET_BY_VALUE_LIST = 23,
	AS_CDT_OP_LIST_GET_BY_INDEX_RANGE = 24,
	AS_CDT_OP_LIST_GET_BY_VALUE_INTERVAL = 25,
	AS_CDT_OP_LIST_GET_BY_RANK_RANGE = 26,
	AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE = 27,
	AS_CDT_OP_LIST_REMOVE_BY_INDEX = 32,
	AS_CDT_OP_LIST_REMOVE_BY_RANK = 34,
	AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE = 35,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE_LIST = 36,
	AS_CDT_OP_LIST_REMOVE_BY_INDEX_RANGE = 37,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE_INTERVAL = 38,
	AS_CDT_OP_LIST_REMOVE_BY_RANK_RANGE = 39,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE_REL_RANK_RANGE = 40,


class AtomExpr:
    def _op(self):
        raise NotImplementedError

    def compile(self):
        raise NotImplementedError


TypeResultType = Optional[int]
TypeFixedEle = Union[int, float, str, bytes, dict]
TypeFixed = Optional[Dict[str, TypeFixedEle]]
TypeCompiledOp = Tuple[int, TypeResultType, TypeFixed, int]
TypeExpression = List[TypeCompiledOp]

TypeChild = Union[int, float, str, bytes, AtomExpr]
TypeChildren = Tuple[TypeChild, ...]


class BaseExpr(AtomExpr):
    op: int = 0
    rt: TypeResultType = None
    fixed: TypeFixed = None
    # HACK: Couldn't specify BaseExpr, had so I created AtomExpr as a hack.
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


TypeBinName = Union[BaseExpr, str]
TypeListValue = Union[BaseExpr, List[Any]]
TypeIndex = Union[BaseExpr, int, aerospike.CDTInfinite]
TypeCDT = Union[None, List[cdt_ctx._cdt_ctx]]
TypeRank = Union[BaseExpr, int, aerospike.CDTInfinite]
TypeCount = Union[BaseExpr, int, aerospike.CDTInfinite]
TypeValue = Union[BaseExpr, Any]
TypePolicy = Union[Dict[str, Any], None]


class And(BaseExpr):
    op = ExprOp.AND

    def __init__(self, *exprs):
        self.children = exprs


class Or(BaseExpr):
    op = ExprOp.OR

    def __init__(self, *exprs):
        self.children = exprs


class Not(BaseExpr):
    op = ExprOp.NOT

    def __init__(self, *exprs):
        self.children = exprs


class Eq(BaseExpr):
    op = ExprOp.EQ

    def __init__(self, expr0, expr1):
        self.children = (expr0, expr1)


class NE(BaseExpr):
    op = ExprOp.NE

    def __init__(self, expr0, expr1):
        self.children = (expr0, expr1)


class GT(BaseExpr):
    op = ExprOp.GT

    def __init__(self, expr0, expr1):
        self.children = (expr0, expr1)


class GE(BaseExpr):
    op = ExprOp.GE

    def __init__(self, expr0, expr1):
        self.children = (expr0, expr1)


class LT(BaseExpr):
    op = ExprOp.LT

    def __init__(self, expr0, expr1):
        self.children = (expr0, expr1)


class LE(BaseExpr):
    op = ExprOp.LE

    def __init__(self, expr0, expr1):
        self.children = (expr0, expr1)


class CmpRegex(BaseExpr): #TODO needs testing and regex string get in C
    op = ExprOp.CMP_REGEX

    def __init__(self, options, regex_str, cmp_str):
        self.children = (cmp_str,)

        self.fixed = {REGEX_OPTIONS_KEY: options, VALUE_KEY: regex_str}


class CmpGeo(BaseExpr): #TODO needs testing
    op = ExprOp.CMP_GEO

    def __init__(self, expr0, expr1):
        self.children = (expr0, expr1)


# Record Key Expressions TODO tests


class _Key(BaseExpr):
    op = ExprOp.REC_KEY


class KeyInt(_Key):
    rt = ResultType.INTEGER


class KeyStr(_Key):
    rt = ResultType.INTEGER


class KeyBlob(_Key):
    rt = ResultType.INTEGER


class KeyExists(BaseExpr):
    op = ExprOp.META_KEY_EXISTS
    rt = ResultType.INTEGER


# Bin Expressions


class _Bin(BaseExpr):
    op = ExprOp.BIN

    def __init__(self, bin_name: TypeBinName):
        self.fixed = {BIN_KEY: bin_name}


class IntBin(_Bin):
    rt = ResultType.INTEGER


class StrBin(_Bin):
    rt = ResultType.STRING


class FloatBin(_Bin):
    rt = ResultType.FLOAT


class BlobBin(_Bin):
    rt = ResultType.BLOB


class GeoBin(_Bin):
    rt = ResultType.GEOJSON


class HLLBin(_Bin):
    rt = ResultType.HLL


class MapBin(_Bin):
    rt = ResultType.MAP


class ListBin(_Bin):
    rt = ResultType.LIST


class TypeBin(BaseExpr):  # TODO implement
    op = ExprOp.BIN_TYPE
    rt = ResultType.INTEGER

    def __init__(self, bin_name: TypeBinName):
        self.fixed = {BIN_KEY: bin_name}


class DigestMod(BaseExpr):
    op = ExprOp.META_DIGEST_MOD
    rt = ResultType.INTEGER

    def __init__(self, mod: int):
        self.fixed = {VALUE_KEY: mod}


class DeviceSize(BaseExpr):
    op = ExprOp.META_DEVICE_SIZE
    rt = ResultType.INTEGER


class LastUpdateTime(BaseExpr):
    op = ExprOp.META_LAST_UPDATE_TIME
    rt = ResultType.INTEGER


class VoidTime(BaseExpr):
    op = ExprOp.META_VOID_TIME
    rt = ResultType.INTEGER


class TTL(BaseExpr):
    op = ExprOp.META_TTL
    rt = ResultType.INTEGER


class SetName(BaseExpr):
    op = ExprOp.META_SET_NAME
    rt = ResultType.STRING


# LIST MOD EXPRESSIONS


class ListAppend(BaseExpr):
    
    op = aerospike.OP_LIST_EXP_APPEND

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, value: TypeValue, bin_name: TypeBinName):
        self.children = (
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_CRMOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}), #TODO implement these
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[LIST_POLICY_KEY] = policy


class ListAppendItems(BaseExpr):
    op = aerospike.OP_LIST_EXP_APPEND_ITEMS

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, value: TypeValue, bin_name: TypeBinName):
        self.children = (
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_CRMOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[LIST_POLICY_KEY] = policy


class ListInsert(BaseExpr):
    op = aerospike.OP_LIST_EXP_INSERT

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, index: TypeIndex, value: TypeValue, bin_name: TypeBinName):
        self.children = (
            index,
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_MOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[LIST_POLICY_KEY] = policy


class ListInsertItems(BaseExpr):
    op = aerospike.OP_LIST_EXP_INSERT_ITEMS

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, index: TypeIndex, values: TypeListValue, bin_name: TypeBinName):
        self.children = (
            index,
            values,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_MOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}), #TODO implement these MOD expressions in C.
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[LIST_POLICY_KEY] = policy


class ListIncrement(BaseExpr):
    op = aerospike.OP_LIST_EXP_INCREMENT

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, index: TypeIndex, value: TypeValue, bin_name: TypeBinName):
        self.children = (
            index,
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_CRMOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[LIST_POLICY_KEY] = policy


class ListSet(BaseExpr):
    op = aerospike.OP_LIST_EXP_SET

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, index: TypeIndex, value: TypeValue, bin_name: TypeBinName):
        self.children = (
            index,
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_MOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[LIST_POLICY_KEY] = policy


class ListClear(BaseExpr):
    op = aerospike.OP_LIST_EXP_CLEAR

    def __init__(self, ctx: TypeCDT, bin_name: TypeBinName):
        self.children = (
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListSort(BaseExpr):
    op = aerospike.OP_LIST_EXP_SORT

    def __init__(self, ctx: TypeCDT, order: int, bin_name: TypeBinName):
        self.children = (
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name),
        )
        self.fixed = {LIST_ORDER_KEY: order}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByValue(BaseExpr):
    op = aerospike.OP_LIST_EXP_REMOVE_BY_VALUE

    def __init__(self, ctx: TypeCDT, value: TypeValue, bin_name: TypeBinName):
        self.children = (
            value,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByValueList(BaseExpr):
    op = aerospike.OP_LIST_EXP_REMOVE_BY_VALUE_LIST

    def __init__(self, ctx: TypeCDT, values: TypeListValue, bin_name: TypeBinName):
        self.children = (
            values,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByValueRange(BaseExpr):
    op = aerospike.OP_LIST_EXP_REMOVE_BY_VALUE_RANGE

    def __init__(self, ctx: TypeCDT, begin: TypeValue, end: TypeValue, bin_name: TypeBinName):
        self.children = (
            begin,
            end,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByValueRelRankToEnd(BaseExpr):
    op = aerospike.OP_LIST_EXP_REMOVE_BY_REL_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, value: TypeValue, rank: TypeRank, bin_name: TypeBinName):
        self.children = (
            value,
            rank,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByValueRelRankRange(BaseExpr):
    op = aerospike.OP_LIST_EXP_REMOVE_BY_REL_RANK_RANGE

    def __init__(self, ctx: TypeCDT, value: TypeValue, rank: TypeRank, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            value,
            rank,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByIndex(BaseExpr):
    op = aerospike.OP_LIST_EXP_REMOVE_BY_INDEX

    def __init__(self, ctx: TypeCDT, index: TypeIndex, bin_name: TypeBinName):
        self.children = (
            index,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByIndexRangeToEnd(BaseExpr):
    op = aerospike.OP_LIST_EXP_REMOVE_BY_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, index: TypeIndex, bin_name: TypeBinName):
        self.children = (
            index,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByIndexRange(BaseExpr):
    op = aerospike.OP_LIST_EXP_REMOVE_BY_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, index: TypeIndex, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            index,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByRank(BaseExpr):
    op = aerospike.OP_LIST_EXP_REMOVE_BY_RANK

    def __init__(self, ctx: TypeCDT, rank: TypeRank, bin_name: TypeBinName):
        self.children = (
            rank,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByRankRangeToEnd(BaseExpr):
    op = aerospike.OP_LIST_EXP_REMOVE_BY_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, rank: TypeRank, bin_name: TypeBinName):
        self.children = (
            rank,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListRemoveByRankRange(BaseExpr):
    op = aerospike.OP_LIST_EXP_REMOVE_BY_RANK_RANGE

    def __init__(self, ctx: TypeCDT, rank: TypeRank, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            rank,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


# LIST READ EXPRESSIONS


class _AS_EXP_CDT_LIST_READ(BaseExpr):
    op = ExprOp.CALL

    def __init__(self, __type, __rtype, __is_multi):
        self.children = (
            BaseExpr()
        )


class ListSize(BaseExpr): #TODO do tests
    op = aerospike.OP_LIST_EXP_SIZE

    def __init__(self, ctx: TypeCDT, bin_name: TypeBinName):
        self.children = (
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByValue(BaseExpr):
    op = aerospike.OP_LIST_EXP_GET_BY_VALUE

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeValue, bin_name: TypeBinName):
        self.children = (
            value,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByValueRange(BaseExpr):
    op = aerospike.OP_LIST_EXP_GET_BY_VALUE_RANGE

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_begin: TypeValue,
        value_end: TypeValue,
        bin_name: TypeBinName
    ):
        self.children = (
            value_begin,
            value_end,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByValueList(BaseExpr):
    op = aerospike.OP_LIST_EXP_GET_BY_VALUE_LIST

    def __init__(self, ctx: TypeCDT, return_type: int, value: Union[BaseExpr, list], bin_name: TypeBinName):
        self.children = (
            value,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByValueRelRankRangeToEnd(BaseExpr):
    op = aerospike.OP_LIST_EXP_GET_BY_VALUE_RANK_RANGE_REL_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, value: Union[BaseExpr, list], rank: TypeRank, bin_name: TypeBinName):
        self.children = (
            value,
            rank,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByValueRelRankRange(BaseExpr):
    op = aerospike.OP_LIST_EXP_GET_BY_VALUE_RANK_RANGE_REL

    def __init__(self, ctx: TypeCDT, return_type: int, value: Union[BaseExpr, list], rank: TypeRank, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            value,
            rank,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByIndex(BaseExpr):
    op = aerospike.OP_LIST_EXP_GET_BY_INDEX

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_type: int,
        index: TypeIndex,
        bin_name: TypeBinName,
    ):
        self.children = (
            index,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)  # TODO should this be implemented in other places?
        )
        self.fixed = {BIN_TYPE_KEY: value_type, RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByIndexRangeToEnd(BaseExpr):
    op = aerospike.OP_LIST_EXP_GET_BY_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, index: TypeIndex, bin_name: TypeBinName):
        self.children = (
            index,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByIndexRange(BaseExpr):
    op = aerospike.OP_LIST_EXP_GET_BY_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, index: TypeIndex, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            index,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByRank(BaseExpr):
    op = aerospike.OP_LIST_EXP_GET_BY_RANK

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        val_type: int,
        rank: TypeRank,
        bin_name: TypeBinName,
    ):
        self.children = (
            rank,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {BIN_TYPE_KEY: val_type, RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByRankRangeToEnd(BaseExpr):
    op = aerospike.OP_LIST_EXP_GET_BY_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, rank: TypeRank, bin_name: TypeBinName):
        self.children = (
            rank,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByRankRange(BaseExpr):
    op = aerospike.OP_LIST_EXP_GET_BY_RANK_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, rank: TypeRank, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            rank,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


# MAP MODIFIY EXPRESSIONS
TypeKey = Union[BaseExpr, Any]
TypeKeyList = Union[BaseExpr, List[Any]]


class MapPut(BaseExpr):
    op = aerospike.OP_MAP_PUT

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, key: TypeKey, value: TypeValue, bin_name: TypeBinName):
        self.children = (
            key,
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_MAP_CRMOD, 0, {MAP_POLICY_KEY: policy} if policy is not None else {}),
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[MAP_POLICY_KEY] = policy


class MapPutItems(BaseExpr):
    op = aerospike.OP_MAP_PUT_ITEMS

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, map: map, bin_name: TypeBinName):
        self.children = (
            map,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_MAP_CRMOD, 0, {MAP_POLICY_KEY: policy} if policy is not None else {}),
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[MAP_POLICY_KEY] = policy


class MapIncrement(BaseExpr):
    op = aerospike.OP_MAP_INCREMENT

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, key: TypeKey, value: TypeValue, bin_name: TypeBinName):
        self.children = (
            key,
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_MAP_CRMOD, 0, {MAP_POLICY_KEY: policy} if policy is not None else {}),
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[MAP_POLICY_KEY] = policy


class MapClear(BaseExpr):
    op = aerospike.OP_MAP_CLEAR

    def __init__(self, ctx: TypeCDT, bin_name: TypeBinName):
        self.children = (
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByKey(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_KEY

    def __init__(self, ctx: TypeCDT, key: TypeKey, bin_name: TypeBinName):
        self.children = (
            key,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByKeyList(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_KEY_LIST

    def __init__(self, ctx: TypeCDT, keys: List[TypeKey], bin_name: TypeBinName):
        self.children = (
            keys,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByKeyRange(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_KEY_RANGE

    def __init__(self, ctx: TypeCDT, begin: TypeValue, end: TypeValue, bin_name: TypeBinName):
        self.children = (
            begin,
            end,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByKeyRelIndexRangeToEnd(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, key: TypeKey, index: TypeIndex, bin_name: TypeBinName):
        self.children = (
            key,
            index,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByKeyRelIndexRange(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, key: TypeKey, index: TypeIndex, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            key,
            index,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByValue(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_VALUE

    def __init__(self, ctx: TypeCDT, value: TypeValue, bin_name: TypeBinName):
        self.children = (
            value,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByValueList(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_VALUE_LIST

    def __init__(self, ctx: TypeCDT, values: TypeListValue, bin_name: TypeBinName):
        self.children = (
            values,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByValueRange(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_VALUE_RANGE

    def __init__(self, ctx: TypeCDT, begin: TypeValue, end: TypeValue, bin_name: TypeBinName):
        self.children = (
            begin,
            end,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByValueRelRankRangeToEnd(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, value: TypeValue, rank: TypeRank, bin_name: TypeBinName):
        self.children = (
            value,
            rank,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByValueRelRankRange(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE

    def __init__(self, ctx: TypeCDT, value: TypeValue, rank: TypeRank, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            value,
            rank,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByIndex(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_INDEX

    def __init__(self, ctx: TypeCDT, index: TypeIndex, bin_name: TypeBinName):
        self.children = (
            index,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByIndexRangeToEnd(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, index: TypeIndex, bin_name: TypeBinName):
        self.children = (
            index,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByIndexRange(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, index: TypeIndex, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            index,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByRank(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_RANK

    def __init__(self, ctx: TypeCDT, rank: TypeRank, bin_name: TypeBinName):
        self.children = (
            rank,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByRankRangeToEnd(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, rank: TypeRank, bin_name: TypeBinName):
        self.children = (
            rank,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapRemoveByRankRange(BaseExpr):
    op = aerospike.OP_MAP_REMOVE_BY_RANK_RANGE

    def __init__(self, ctx: TypeCDT, rank: TypeRank, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            rank,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


# MAP READ EXPRESSIONS


class MapSize(BaseExpr): #TODO do tests
    op = aerospike.OP_MAP_SIZE

    def __init__(self, ctx: TypeCDT, bin_name: TypeBinName):
        self.children = (
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByKey(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_KEY

    def __init__(self, ctx: TypeCDT, return_type: int, value_type: int, key: TypeKey, bin_name: TypeBinName):
        self.children = (
            key,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {BIN_TYPE_KEY: value_type, RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByKeyRange(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_KEY_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, begin: TypeKey, end: TypeKey, bin_name: TypeBinName):
        self.children = (
            begin,
            end,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByKeyList(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_KEY_LIST

    def __init__(self, ctx: TypeCDT, return_type: int, keys: TypeKeyList, bin_name: TypeBinName):
        self.children = (
            keys,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByKeyRelIndexRangeToEnd(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, key: TypeKey, index: TypeIndex, bin_name: TypeBinName):
        self.children = (
            key,
            index,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByKeyRelIndexRange(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_KEY_REL_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, key: TypeKey, index: TypeIndex, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            key,
            index,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name),
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByValue(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_VALUE

    def __init__(self, ctx: TypeCDT, return_type: int, value: TypeValue, bin_name: TypeBinName):
        self.children = (
            value,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByValueRange(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_VALUE_RANGE

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_begin: TypeValue,
        value_end: TypeValue,
        bin_name: TypeBinName
    ):
        self.children = (
            value_begin,
            value_end,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByValueList(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_VALUE_LIST

    def __init__(self, ctx: TypeCDT, return_type: int, value: Union[BaseExpr, list], bin_name: TypeBinName):
        self.children = (
            value,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByValueRelRankRangeToEnd(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, value: Union[BaseExpr, list], rank: TypeRank, bin_name: TypeBinName):
        self.children = (
            value,
            rank,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByValueRelRankRange(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_VALUE_RANK_RANGE_REL

    def __init__(self, ctx: TypeCDT, return_type: int, value: Union[BaseExpr, list], rank: TypeRank, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            value,
            rank,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByIndex(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_INDEX

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_type: int,
        index: TypeIndex,
        bin_name: TypeBinName,
    ):
        self.children = (
            index,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name)  # TODO should this be implemented in other places?
        )
        self.fixed = {BIN_TYPE_KEY: value_type, RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByIndexRangeToEnd(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, index: TypeIndex, bin_name: TypeBinName):
        self.children = (
            index,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByIndexRange(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_INDEX_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, index: TypeIndex, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            index,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByRank(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_RANK

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        val_type: int,
        rank: TypeRank,
        bin_name: TypeBinName,
    ):
        self.children = (
            rank,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name)
        )
        self.fixed = {BIN_TYPE_KEY: val_type, RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByRankRangeToEnd(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_RANK_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, rank: TypeRank, bin_name: TypeBinName):
        self.children = (
            rank,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class MapGetByRankRange(BaseExpr):
    op = aerospike.OP_MAP_GET_BY_RANK_RANGE

    def __init__(self, ctx: TypeCDT, return_type: int, rank: TypeRank, count: TypeCount, bin_name: TypeBinName):
        self.children = (
            rank,
            count,
            bin_name if isinstance(bin_name, BaseExpr) else MapBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


# BIT MODIFY EXPRESSIONS


TypeBitValue = Union[bytes, bytearray]


class BitResize(BaseExpr):
    op = aerospike.OP_BIT_RESIZE

    def __init__(self, policy: TypePolicy, byte_size: int, flags: int, bin: TypeBinName):
        self.children = (
            byte_size,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            _GenericExpr(150, 0, {VALUE_KEY: flags} if flags is not None else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitInsert(BaseExpr):
    op = aerospike.OP_BIT_INSERT

    def __init__(self, policy: TypePolicy, byte_offset: int, value: TypeBitValue, bin: TypeBinName):
        self.children = (
            byte_offset,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitRemove(BaseExpr):
    op = aerospike.OP_BIT_REMOVE

    def __init__(self, policy: TypePolicy, byte_offset: int, byte_size: int, bin: TypeBinName):
        self.children = (
            byte_offset,
            byte_size,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitSet(BaseExpr):
    op = aerospike.OP_BIT_SET

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: TypeBitValue, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitOr(BaseExpr):
    op = aerospike.OP_BIT_OR

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: TypeBitValue, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitXor(BaseExpr):
    op = aerospike.OP_BIT_XOR

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: TypeBitValue, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitAnd(BaseExpr):
    op = aerospike.OP_BIT_AND

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: TypeBitValue, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitNot(BaseExpr):
    op = aerospike.OP_BIT_NOT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitLeftShift(BaseExpr):
    op = aerospike.OP_BIT_LSHIFT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, shift: int, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            shift,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitRightShift(BaseExpr):
    op = aerospike.OP_BIT_RSHIFT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, shift: int, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            shift,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitAdd(BaseExpr):
    op = aerospike.OP_BIT_ADD

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: int, action: int, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            _GenericExpr(150, 0, {VALUE_KEY: action} if action is not None else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitSubtract(BaseExpr):
    op = aerospike.OP_BIT_SUBTRACT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: int, action: int, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            _GenericExpr(150, 0, {VALUE_KEY: action} if action is not None else {VALUE_KEY: 0}),
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitSetInt(BaseExpr):
    op = aerospike.OP_BIT_SET_INT

    def __init__(self, policy: TypePolicy, bit_offset: int, bit_size: int, value: int, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            value,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


# BIT READ EXPRESSIONS


class BitGet(BaseExpr):
    op = aerospike.OP_BIT_GET

    def __init__(self, bit_offset: int, bit_size: int, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitCount(BaseExpr):
    op = aerospike.OP_BIT_COUNT

    def __init__(self, bit_offset: int, bit_size: int, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitLeftScan(BaseExpr):
    op = aerospike.OP_BIT_LSCAN

    def __init__(self, bit_offset: int, bit_size: int, value: bool, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            value,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitRightScan(BaseExpr):
    op = aerospike.OP_BIT_RSCAN

    def __init__(self, bit_offset: int, bit_size: int, value: bool, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            value,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class BitGetInt(BaseExpr):
    op = aerospike.OP_BIT_GET_INT

    def __init__(self, bit_offset: int, bit_size: int, sign: bool, bin: TypeBinName):
        self.children = (
            bit_offset,
            bit_size,
            sign,
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


# HLL modify expressions


class HLLAddMH(BaseExpr):
    op = aerospike.OP_HLL_ADD

    def __init__(self, policy: TypePolicy, list: TypeListValue, index_bit_count: int, mh_bit_count: int, bin: TypeBinName):
        self.children = (
            list,
            index_bit_count,
            mh_bit_count,
            _GenericExpr(150, 0, {VALUE_KEY: policy['flags']} if policy is not None and 'flags' in policy else {VALUE_KEY: 0}), #TODO: decide if this is best
            bin if isinstance(bin, BaseExpr) else BlobBin(bin)
        )


class HLLAdd(BaseExpr):
    op = aerospike.OP_HLL_ADD

    def __init__(self, policy: TypePolicy, list: TypeListValue, index_bit_count: int, bin: TypeBinName):
        self.children = (
            list,
            index_bit_count,
            -1,
            policy['flags'] if policy is not None and 'flags' in policy else 0, #TODO: decide if this is best
            bin if isinstance(bin, BaseExpr) else HLLBin(bin)
        )


class HLLUpdate(BaseExpr):
    op = aerospike.OP_HLL_ADD

    def __init__(self, policy: TypePolicy, list: TypeListValue, bin: TypeBinName):
        self.children = (
            list,
            -1,
            -1,
            policy['flags'] if policy is not None and 'flags' in policy else 0, #TODO: decide if this is best
            bin if isinstance(bin, BaseExpr) else HLLBin(bin)
        )


# HLL read expressions


class HLLGetCount(BaseExpr):
    op = aerospike.OP_HLL_GET_COUNT

    def __init__(self, bin: TypeBinName):
        self.children = (
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )


class HLLGetUnion(BaseExpr):
    op = aerospike.OP_HLL_GET_UNION

    def __init__(self, bin: TypeBinName, values: TypeListValue):
        self.children = (
            values,
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )


class HLLGetUnionUnionCount(BaseExpr):
    op = aerospike.OP_HLL_GET_UNION_COUNT

    def __init__(self, bin: TypeBinName, values: TypeListValue):
        self.children = (
            values,
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )


class HLLGetIntersectCount(BaseExpr):
    op = aerospike.OP_HLL_GET_INTERSECT_COUNT

    def __init__(self, bin: TypeBinName, values: TypeListValue):
        self.children = (
            values,
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )


class HLLGetSimilarity(BaseExpr):
    op = aerospike.OP_HLL_GET_SIMILARITY

    def __init__(self, bin: TypeBinName, values: TypeListValue):
        self.children = (
            values,
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )


class HLLDescribe(BaseExpr):
    op = aerospike.OP_HLL_DESCRIBE

    def __init__(self, bin: TypeBinName):
        self.children = (
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )


class HLLMayContain(BaseExpr):
    op = aerospike.OP_HLL_MAY_CONTAIN

    def __init__(self, bin: TypeBinName, values: TypeListValue):
        self.children = (
            values,
            bin if isinstance(bin, BaseExpr) else HLLBin(bin),
        )



# def example():
#     expr = And(EQ(IntBin("foo"), 5),
#                EQ(IntBin("bar"), IntBin("baz")),
#                EQ(IntBin("buz"), IntBin("baz")))

#     print(expr.compile())


# if __name__ == "__main__":
#     example()

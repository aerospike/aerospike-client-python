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
PARAM_COUNT_KEY = "param_count"
EXTRA_PARAM_COUNT_KEY = "extra_param_count"
LIST_ORDER_KEY = "list_order"

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
    LIST_MOD = 139
    _AS_EXP_CODE_AS_VAL = 134
    _AS_EXP_CODE_VAL_PK = 135
    _AS_EXP_CODE_VAL_INT = 136
    _AS_EXP_CODE_VAL_UINT = 137
    _AS_EXP_CODE_VAL_FLOAT = 138
    _AS_EXP_CODE_VAL_BOOL = 139
    _AS_EXP_CODE_VAL_STR = 140
    _AS_EXP_CODE_VAL_BYTES = 141
    _AS_EXP_CODE_VAL_RAWSTR = 142
    _AS_EXP_CODE_VAL_RTYPE = 143

    _AS_EXP_CODE_CALL_VOP_START = 144
    _AS_EXP_CODE_CDT_LIST_CRMOD = 145
    _AS_EXP_CODE_CDT_LIST_MOD = 146
    _AS_EXP_CODE_CDT_MAP_CRMOD = 147
    _AS_EXP_CODE_CDT_MAP_CR = 148
    _AS_EXP_CODE_CDT_MAP_MOD = 149

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


class EQ(BaseExpr):
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


class _Bin(BaseExpr):
    op = ExprOp.BIN

    def __init__(self, bin_name: TypeBinName):
        self.fixed = {BIN_KEY: bin_name}


class IntBin(_Bin):
    rt = ResultType.INTEGER


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


class HLLBin(_Bin):
    rt = ResultType.HLL


class TypeBin(BaseExpr):  # TODO implement
    op = ExprOp.BIN_TYPE
    rt = ResultType.INTEGER

    def __init__(self, bin_name: TypeBinName):
        self.fixed = {BIN_KEY: bin_name}


class MetaDigestMod(BaseExpr):
    op = ExprOp.META_DIGEST_MOD
    rt = ResultType.INTEGER

    def __init__(self, mod: int):
        self.fixed = {VALUE_KEY: mod}


class MetaDeviceSize(BaseExpr):
    op = ExprOp.META_DEVICE_SIZE
    rt = ResultType.INTEGER


class MetaLastUpdateTime(BaseExpr):
    op = ExprOp.META_LAST_UPDATE_TIME
    rt = ResultType.INTEGER


class MetaVoidTime(BaseExpr):
    op = ExprOp.META_VOID_TIME
    rt = ResultType.INTEGER


class MetaTTL(BaseExpr):
    op = ExprOp.META_TTL
    rt = ResultType.INTEGER


class MetaSetName(BaseExpr):
    op = ExprOp.META_SET_NAME
    rt = ResultType.STRING


class MetaKeyExists(BaseExpr):
    op = ExprOp.META_KEY_EXISTS
    rt = ResultType.BOOLEAN


class MetaKeyStr(BaseExpr):
    op = ExprOp.REC_KEY
    rt = ResultType.STRING


class MetaKeyInt(BaseExpr):
    op = ExprOp.REC_KEY
    rt = ResultType.INTEGER


class MetaKeyBlobe(BaseExpr):
    op = ExprOp.REC_KEY
    rt = ResultType.BLOB


# LIST MOD EXPRESSIONS


# class ListAppend(BaseExpr):
#     op = aerospike.OP_LIST_EXP_SIZE

#     def __init__(self, ctx: TypeCDT, policy: TypePolicy, value: TypeValue, bin_name: TypeBinName):
#         self.children = (
#             value,
#             _GenericExpr(),
#             bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name),
#         )
#         self.fixed = {OP_TYPE_KEY: ListOpType.AS_CDT_OP_LIST_APPEND, PARAM_COUNT_KEY: 1, EXTRA_PARAM_COUNT_KEY: 2}

#         if ctx is not None:
#             self.fixed[CTX_KEY] = ctx

#         if policy is not None:
#             self.fixed[LIST_POLICY_KEY] = policy


class ListAppend(BaseExpr):
    op = aerospike.OP_LIST_EXP_APPEND

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, value: TypeValue, bin_name: TypeBinName):
        self.children = (
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_CRMOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}), #TODO implement these
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name),
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
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name),
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
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name),
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx

        if policy is not None:
            self.fixed[LIST_POLICY_KEY] = policy


class ListInsertItems(BaseExpr):
    op = aerospike.OP_LIST_EXP_INSERT_ITEMS

    def __init__(self, ctx: TypeCDT, policy: TypePolicy, index: TypeIndex, value: TypeValue, bin_name: TypeBinName):
        self.children = (
            index,
            value,
            _GenericExpr(ExprOp._AS_EXP_CODE_CDT_LIST_MOD, 0, {LIST_POLICY_KEY: policy} if policy is not None else {}),
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name),
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
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name),
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
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name),
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
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListSort(BaseExpr):
    op = aerospike.OP_LIST_EXP_SORT

    def __init__(self, ctx: TypeCDT, order: int, bin_name: TypeBinName):
        self.children = (
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {LIST_ORDER_KEY: order}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListSort(BaseExpr):
    op = aerospike.OP_LIST_EXP_SORT

    def __init__(self, ctx: TypeCDT, order: int, bin_name: TypeBinName):
        self.children = (
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
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


class ListRemoveByValueRelRankRANGE(BaseExpr):
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

    def __init__(self, ctx: TypeCDT, value: TypeValue, return_type: int, bin_name: TypeBinName):
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
        val_type: int,
        ctx: TypeCDT,
        return_type: int,
        index: TypeIndex,
        bin_name: TypeBinName,
    ):
        self.children = (
            index,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)  # TODO should this be implemented in other places?
        )
        self.fixed = {BIN_TYPE_KEY: val_type, RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByIndexRelRankRangeToEnd(BaseExpr):
    op = aerospike.OP_LIST_EXP_GET_BY_INDEX_RANGE_TO_END

    def __init__(self, ctx: TypeCDT, return_type: int, index: TypeIndex, bin_name: TypeBinName):
        self.children = (
            index,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByIndexRelRankRange(BaseExpr):
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
TypeKeyList = Union[BaseExpr, List[TypeKey]]


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

# def example():
#     expr = And(EQ(IntBin("foo"), 5),
#                EQ(IntBin("bar"), IntBin("baz")),
#                EQ(IntBin("buz"), IntBin("baz")))

#     print(expr.compile())


# if __name__ == "__main__":
#     example()

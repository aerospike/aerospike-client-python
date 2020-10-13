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


TypeBinName = Union[BaseExpr, str]
TypeListValue = Union[Any]
TypeIndex = Union[BaseExpr, int, aerospike.CDTInfinite]
TypeCDT = Union[None, List[cdt_ctx._cdt_ctx]]
TypeRank = Union[BaseExpr, int, aerospike.CDTInfinite]
TypeCount = Union[BaseExpr, int, aerospike.CDTInfinite]


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

    def __init__(self, ctx: TypeCDT, value: TypeListValue, return_type: int, bin_name: TypeBinName):
        self.children = (
            value,
            bin_name if isinstance(bin_name, BaseExpr) else ListBin(bin_name)
        )
        self.fixed = {RETURN_TYPE_KEY: return_type}

        if ctx is not None:
            self.fixed[CTX_KEY] = ctx


class ListGetByValueRange(BaseExpr):  # TODO how to mark if bin name is not expression?
    op = aerospike.OP_LIST_EXP_GET_BY_VALUE_RANGE

    def __init__(
        self,
        ctx: TypeCDT,
        return_type: int,
        value_begin: TypeListValue,
        value_end: TypeListValue,
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


# def example():
#     expr = And(EQ(IntBin("foo"), 5),
#                EQ(IntBin("bar"), IntBin("baz")),
#                EQ(IntBin("buz"), IntBin("baz")))

#     print(expr.compile())


# if __name__ == "__main__":
#     example()

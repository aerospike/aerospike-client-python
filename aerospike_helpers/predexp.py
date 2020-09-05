from itertools import chain
from typing import List, Optional, Tuple, Union


class ExprOp():
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


class ResultType():
    BOOLEAN = 1
    INTEGER = 2
    STRING = 3
    LIST = 4
    MAP = 5
    BLOB = 6
    FLOAT = 7
    GEOJSON = 8
    HLL = 9


class CallType():
    CDT = 0
    BIT = 1
    HLL = 2

    MODIFY = 0x40


class AtomExpr():
    def _op(self):
        raise NotImplementedError

    def compile(self):
        raise NotImplementedError


TypeResultType = Optional[int]
TypeFixedEle = Union[int, float, str, bytes]
TypeFixed = Optional[Tuple[TypeFixedEle, ...]]
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
        return (0, None, (v,), 0)

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


class IntBin(BaseExpr):
    op = ExprOp.BIN
    rt = ResultType.INTEGER

    def __init__(self, name: str):
        self.fixed = (name,)


class MetaDigestMod(BaseExpr):
    op = ExprOp.META_DIGEST_MOD
    rt = ResultType.INTEGER

    def __init__(self, mod: int):
        self.fixed = (mod,)


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
    op = ExprOp.META_TTL
    rt = ResultType.STRING


class MetaKeyExists(BaseExpr):
    op = ExprOp.META_KEY_EXISTS
    rt = ResultType.BOOLEAN


# def example():
#     expr = And(EQ(IntBin("foo"), 5),
#                EQ(IntBin("bar"), IntBin("baz")),
#                EQ(IntBin("buz"), IntBin("baz")))

#     print(expr.compile())


# if __name__ == "__main__":
#     example()
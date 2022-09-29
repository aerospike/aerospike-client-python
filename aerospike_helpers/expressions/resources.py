'''
Resources used by all expressions.
'''

#from __future__ import annotations
from itertools import chain
from typing import List, Optional, Tuple, Union, Dict, Any
import aerospike
from aerospike_helpers import cdt_ctx


class _Keys:
    VALUE_TYPE_KEY = "value_type"
    BIN_KEY = "bin"
    RETURN_TYPE_KEY = "return_type"
    CTX_KEY = "ctx"
    VALUE_KEY = "val"
    LIST_POLICY_KEY = "list_policy"
    MAP_POLICY_KEY = "map_policy"
    LIST_ORDER_KEY = "list_order"
    REGEX_OPTIONS_KEY = "regex_options"


class _ExprOp: # TODO replace this with an enum
    UNKNOWN = 0

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
    EXCLUSIVE = 19

    ADD = 20
    SUB = 21
    MUL = 22
    DIV = 23
    POW = 24
    LOG = 25
    MOD = 26
    ABS = 27
    FLOOR = 28
    CEIL = 29

    TO_INT = 30
    TO_FLOAT = 31

    INT_AND = 32
    INT_OR = 33
    INT_XOR = 34
    INT_NOT = 35
    INT_LSHIFT = 36
    INT_RSHIFT = 37
    INT_ARSHIFT = 38
    INT_COUNT = 39
    INT_LSCAN = 40
    INT_RSCAN = 41

    MIN = 50
    MAX = 51

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

    COND = 123
    VAR = 124
    LET = 125
    DEF = 126

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

    VAL = 200


class ResultType:
    """
    Flags used to indicate expression value_type.
    """
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

TypeAny = Union[_AtomExpr, Any]

class _BaseExpr(_AtomExpr):
    _op = 0
    # type: int
    _rt = None
    # type: 'TypeResultType'
    _fixed = None
    # type: 'TypeFixed'
    _children = ()
    # type: 'TypeChildren'

    def _get_op(self) -> TypeCompiledOp:
        return (self._op, self._rt, self._fixed, len(self._children))

    def _vop(self, v) -> TypeCompiledOp:
        return (
            _ExprOp.VAL,
            None,
            {_Keys.VALUE_KEY: v},
            0,
        )

    def compile(self) -> TypeExpression:
        expression = [self._get_op()]
        # type: 'TypeExpression'
        work = chain(self._children)

        while True:
            try:
                item = next(work)
            except StopIteration:
                break

            if isinstance(item, _BaseExpr):
                expression.append(item._get_op())
                work = chain(item._children, work)
            else:
                # Should be a str, bin, int, float, etc.
                expression.append(self._vop(item))

        return expression

    def _overload_op_unary(self, op_type: int):
        if self._op == op_type:
            l = self._children
        else:
            l = (self,)

        r = [] # No right operand.
        return _create_operator_expression(l, r, op_type)

    def _overload_op(self, right: 'TypeAny', op_type: int):
        if self._op == op_type:
            l = self._children
        else:
            l = (self,)

        if isinstance(right, _BaseExpr) and right._op == op_type:
            r = right._children
        else:
            r = (right,)

        return _create_operator_expression(l, r, op_type)

    def _overload_op_va_args(self, right: 'TypeAny', op_type: int):
        expr_end = _BaseExpr()
        expr_end._op = _ExprOp._AS_EXP_CODE_END_OF_VA_ARGS

        if self._op == op_type:
            # Last element of an expression with var args'
            # children will always be _AS_EXP_CODE_END_OF_VA_ARGS.
            l = self._children[:-1]
        else:
            l = (self,)

        if isinstance(right, _BaseExpr) and right._op == op_type:
            r = right._children[:-1]
        else:
            r = (right,)

        return _create_operator_expression(l, r + (expr_end,), op_type)
    
    # unary operators

    def __abs__(self):
        return self._overload_op_unary(_ExprOp.ABS)

    def __floor__(self):
        return self._overload_op_unary(_ExprOp.FLOOR)

    def __ceil__(self):
        return self._overload_op_unary(_ExprOp.CEIL)

    # operators

    def __add__(self, right):
        return self._overload_op_va_args(right, _ExprOp.ADD)

    def __sub__(self, right: 'TypeAny'):
        return self._overload_op_va_args(right, _ExprOp.SUB)

    def __mul__(self, right: 'TypeAny'):
        return self._overload_op_va_args(right, _ExprOp.MUL)

    def __truediv__(self, right: 'TypeAny'):
        return self._overload_op_va_args(right, _ExprOp.DIV)

    def __floordiv__(self, right: 'TypeAny'):
        div_expr = self.__truediv__(right)
        return div_expr.__floor__()

    def __pow__(self, right: 'TypeAny'):
        return self._overload_op(right, _ExprOp.POW)

    def __mod__(self, right: 'TypeAny'):
        return self._overload_op(right, _ExprOp.MOD)

def _create_operator_expression(left_children: 'TypeChildren', right_children: 'TypeChildren', op_type: int):
    new_expr = _BaseExpr()
    new_expr._op = op_type
    new_expr._children = (*left_children, *right_children)
    return new_expr

class _GenericExpr(_BaseExpr):
    
    def __init__(self, op: _ExprOp, rt: 'TypeResultType', fixed: 'TypeFixed'):
        self._op = op
        self._rt = rt
        self._fixed = fixed

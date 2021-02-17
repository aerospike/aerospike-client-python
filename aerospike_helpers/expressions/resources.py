'''
Resources used by all expressions.
'''

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


class _ExprOp:
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


class _BaseExpr(_AtomExpr):
    _op = 0
    # type: int
    _rt = None
    # type: TypeResultType
    _fixed = None
    # type: TypeFixed
    _children = ()
    # type: TypeChildren

    def _get_op(self) -> TypeCompiledOp:
        return (self._op, self._rt, self._fixed, len(self._children))

    def _vop(self, v) -> TypeCompiledOp:
        return (
            0,
            None,
            {_Keys.VALUE_KEY: v},
            0,
        )

    def compile(self) -> TypeExpression:
        expression = [self._get_op()]
        # type: TypeExpression
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


class _GenericExpr(_BaseExpr):
    
    def __init__(self, op: _ExprOp, rt: TypeResultType, fixed: TypeFixed):
        self._op = op
        self._rt = rt
        self._fixed = fixed
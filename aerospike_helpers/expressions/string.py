from aerospike_helpers.expressions.resources import _BaseExpr


class StrLen(_BaseExpr):
    def __init__(self, src: str):
        pass


class SubStr(_BaseExpr):
    def __init__(self, start: int, length: int | None, src: str):
        pass


class CharAt(_BaseExpr):
    def __init__(self, index: int, src: str):
        pass


class Find(_BaseExpr):
    def __init__(self, needle: str, occurrence: int, src: str):
        pass


class Contains(_BaseExpr):
    def __init__(self, needle: str, src: str):
        pass


class StartsWith(_BaseExpr):
    def __init__(self, prefix: str, src: str):
        pass


class EndsWith(_BaseExpr):
    def __init__(self, suffix: str, src: str):
        pass


class ToInteger(_BaseExpr):
    def __init__(self, src: str):
        pass


class ToDouble(_BaseExpr):
    def __init__(self, src: str):
        pass


class ByteLength(_BaseExpr):
    def __init__(self, src: str):
        pass


class IsNumeric(_BaseExpr):
    def __init__(self, numeric_type: int, src: str):
        pass


class ToUpper(_BaseExpr):
    def __init__(self, src: str):
        pass


class ToLower(_BaseExpr):
    def __init__(self, src: str):
        pass

# TODO

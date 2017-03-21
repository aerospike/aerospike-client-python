# data used in fixtures for invalid arguments

INVALID_KEYS = [
    None,
    (None, "demo", 1),
    (1, "demo", 1),
    (False, "demo", 1),
    ([], "demo", 1),
    ("test", 1, 1),
    ("test", False, 1),
    ("test", [], 1),
    ("test", "demo", None),
    ("test", "demo", []),
    ("test", "demo", {})
]

INVALID_BINS = [
    None,
    False,
    [],
]

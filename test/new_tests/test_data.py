try:
    import cPickle as pickle
except Exception:
    import pickle


class SomeClass(object):
    pass


pos_data = [
    (("test", "demo", 1), {"age": 1, "name": "name1"}),
    (("test", "demo", 2), {"age": 2, "name": "Mr John", "bmi": 3.55}),
    (("test", "demo", "boolean_key"), {"is_present": True}),
    (("test", "demo", "string"), {"place": "New York", "name": "John"}),
    (("test", "demo", "bb"), {"a": ["aa", 2, "aa", 4, "cc", 3, 2, 1]}),
    (("test", "demo", 1), {"age": 1, "name": "name1"}),
    (("test", "unknown_set", 1), {"a": {"k": [bytearray("askluy3oijs", "utf-8")]}}),
    # Bytearray
    (("test", "demo", bytearray("asd;as[d'as;d", "utf-8")), {"name": "John"}),
    (("test", "demo", "bytes_key"), {"bytes": bytearray("John", "utf-8")}),
    # List Data
    (("test", "demo", "list_key"), {"names": ["John", "Marlen", "Steve"]}),
    (("test", "demo", "list_key"), {"names": [1, 2, 3, 4, 5]}),
    (("test", "demo", "list_key"), {"names": [1.5, 2.565, 3.676, 4, 5.89]}),
    (("test", "demo", "list_key"), {"names": ["John", "Marlen", 1024]}),
    (("test", "demo", "list_key_unicode"), {"a": ["aa", "bb", 1, "bb", "aa"]}),
    (("test", "demo", "objects"), {"objects": [pickle.dumps(SomeClass()), pickle.dumps(SomeClass())]}),
    # Map Data
    (("test", "demo", "map_key"), {"names": {"name": "John", "age": 24}}),
    (("test", "demo", "map_key_float"), {"double_map": {"1": 3.141, "2": 4.123, "3": 6.285}}),
    (("test", "demo", "map_key_unicode"), {"a": {"aa": "11"}, "b": {"bb": "22"}}),
    #        (('test', 'demo', 1),
    #            {'odict': OrderedDict(sorted({'banana': 3, 'apple': 4, 'pear': 1, 'orange': 2}.items(),
    #                key=lambda t: t[0]))}),
    # Hybrid
    (
        ("test", "demo", "multiple_bins"),
        {
            "i": ["nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8")],
            "s": {"key": "asd';q;'1';"},
            "b": 1234,
            "l": "!@#@#$QSDAsd;as",
        },
    ),
    (
        ("test", "demo", "list_map_key"),
        {
            "names": ["John", "Marlen", "Steve"],
            "names_and_age": [{"name": "John", "age": 24}, {"name": "Marlen", "age": 25}],
        },
    ),
]

key_neg = [
    ((None, "demo", 1), -2, "namespace must be a string"),
    ((12.34, "demo", 1), -2, "namespace must be a string"),
    ((35, "demo", 1), -2, "namespace must be a string"),
    (([], "demo", 1), -2, "namespace must be a string"),
    (({}, "demo", 1), -2, "namespace must be a string"),
    (((), "demo", 1), -2, "namespace must be a string"),
    (None, -2, "key is invalid"),
    (["test", "demo", "key_as_list"], -2, "key is invalid"),
    (("test", 123, 1), -2, "set must be a string"),
    (("test", 12.36, 1), -2, "set must be a string"),
    (("test", [], 1), -2, "set must be a string"),
    (("test", {}, 1), -2, "set must be a string"),
    (("test", (), 1), -2, "set must be a string"),
    (("test", "demo", None), -2, "either key or digest is required"),
    (("test", "demo"), -2, "key tuple must be (Namespace, Set, Key) or (Namespace, Set, None, Digest)"),
]

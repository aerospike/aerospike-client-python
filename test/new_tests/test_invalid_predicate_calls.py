# -*- coding: utf-8 -*-
import pytest
from aerospike import predexp as pe

one_arg_predicates = [
    pe.integer_bin,
    pe.string_bin,
    pe.geojson_bin,
    pe.map_bin,
    pe.list_bin,
    pe.predexp_and,
    pe.predexp_or,
    pe.rec_digest_modulo,
    pe.integer_value,
    pe.string_value,
    pe.geojson_value,
    pe.integer_var,
    pe.string_var,
    pe.geojson_var,
    pe.list_iterate_or,
    pe.list_iterate_and,
    pe.mapkey_iterate_or,
    pe.mapkey_iterate_and,
    pe.mapval_iterate_and,
    pe.mapval_iterate_or,
]

# Note string_regex takes 0, 1, or many so it isn't tested like the others


no_arg_predicates = [
    pe.predexp_not,
    pe.geojson_contains,
    pe.geojson_within,
    pe.integer_equal,
    pe.integer_unequal,
    pe.integer_greater,
    pe.integer_greatereq,
    pe.integer_less,
    pe.integer_lesseq,
    pe.string_equal,
    pe.string_unequal,
    pe.rec_device_size,
    pe.rec_void_time,
    pe.rec_last_update
]


@pytest.mark.parametrize('func', one_arg_predicates)
def test_calling_with_too_few_arguments(func):
    with pytest.raises(TypeError):
        func()


@pytest.mark.parametrize('func', no_arg_predicates)
def test_calling_with_too_many_arguments(func):
    with pytest.raises(TypeError):
        func("argument")


def test_string_regex_with_non_int():
    with pytest.raises(TypeError):
        pe.string_regex(1, "insensitive", "extended")

from aerospike import predicates as as_predicates
# Explicitly test that we can import submodule this way
import aerospike.predicates  # noqa: F401
import pytest

PREDICATE_METHDOS = [
    as_predicates.equals,
    as_predicates.contains,
    as_predicates.between,
    as_predicates.range,
    as_predicates.geo_contains_geojson_point,
    as_predicates.geo_contains_point,
    as_predicates.geo_within_geojson_region,
    as_predicates.geo_within_radius,
]


@pytest.mark.parametrize("predicate", PREDICATE_METHDOS)
def test_invalid_predicate_use(predicate):
    with pytest.raises(TypeError):
        predicate()

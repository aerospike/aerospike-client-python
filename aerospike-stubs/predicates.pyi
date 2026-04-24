"""Secondary index predicates for :class:`aerospike.Query`.

These helpers return tuples consumed by :meth:`aerospike.Query.where`. Geo
predicates require a geo2dsphere index and supported server versions; see the
client documentation for details.

Full reference: https://aerospike-python-client.readthedocs.io/en/latest/predicates.html
"""

from typing import Union, Optional

def between(bin: Optional[str], min: int, max: int) -> tuple:
    """Bin value BETWEEN *min* AND *max* (numeric index).

    Args:
        bin: Bin name, or None depending on usage described in the docs.
        min: Lower bound (inclusive).
        max: Upper bound (inclusive).

    Returns:
        Predicate tuple for :meth:`aerospike.Query.where`.
    """

def equals(bin: Optional[str], val: Union[str, int]) -> tuple:
    """Bin value equals *val* (string or integer index).

    Args:
        bin: Bin name, or None depending on usage described in the docs.
        val: Value to match.

    Returns:
        Predicate tuple for :meth:`aerospike.Query.where`.
    """

def geo_within_geojson_region(bin: Optional[str], shape: str, index_type = ...) -> tuple:
    """Points in *bin* within the GeoJSON region *shape* (geo2dsphere index).

    Requires a geo2dsphere index on a bin containing :class:`aerospike.GeoJSON`
    point data. Server >= 3.7.0.

    Args:
        bin: Bin name.
        shape: Region as a GeoJSON string.
        index_type: Optional index type (see index type constants in ``aerospike``).

    Returns:
        Predicate tuple for :meth:`aerospike.Query.where`.
    """

def geo_within_radius(bin: Optional[str], long: float, lat: float, radius_meters: float, index_type = ...) -> tuple:
    """Points in *bin* within a circle (center *long*/*lat*, radius in meters).

    Builds an AeroCircle GeoJSON region internally. Server >= 3.8.1.

    Args:
        bin: Bin name.
        long: Longitude of the circle center.
        lat: Latitude of the circle center.
        radius_meters: Radius in meters.
        index_type: Optional index type constant.

    Returns:
        Predicate tuple for :meth:`aerospike.Query.where`.
    """

def geo_contains_geojson_point(bin: Optional[str], point: str, index_type = ...) -> tuple:
    """Regions in *bin* that contain the GeoJSON point *point*.

    Server >= 3.7.0.

    Args:
        bin: Bin name.
        point: Point as a GeoJSON string.
        index_type: Optional index type constant.

    Returns:
        Predicate tuple for :meth:`aerospike.Query.where`.
    """

def geo_contains_point(bin: Optional[str], long: float, lat: float, index_type = ...) -> tuple:
    """Regions in *bin* that contain the point (*long*, *lat*).

    Server >= 3.7.0.

    Args:
        bin: Bin name.
        long: Point longitude.
        lat: Point latitude.
        index_type: Optional index type constant.

    Returns:
        Predicate tuple for :meth:`aerospike.Query.where`.
    """

def contains(bin: Optional[str], index_type, val: Union[str, int]) -> tuple:
    """*bin* CONTAINS *val* for list/map secondary indexes.

    Server >= 3.8.1.

    Args:
        bin: Bin name.
        index_type: ``INDEX_TYPE_LIST``, ``INDEX_TYPE_MAPKEYS``, or ``INDEX_TYPE_MAPVALUES``.
        val: Value that must appear in the collection.

    Returns:
        Predicate tuple for :meth:`aerospike.Query.where`.
    """

def range(bin: Optional[str], index_type, min: int, max: int) -> tuple:
    """*bin* CONTAINS values BETWEEN *min* AND *max* for list/map indexes.

    Server >= 3.8.1.

    Args:
        bin: Bin name.
        index_type: Collection index type constant.
        min: Lower bound.
        max: Upper bound.

    Returns:
        Predicate tuple for :meth:`aerospike.Query.where`.
    """

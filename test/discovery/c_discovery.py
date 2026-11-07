"""ctypes bindings to the C client's discovery conformance entry points (as_discovery.h).

The C client is linked into the aerospike extension, which exports its symbols.
"""

import ctypes
from ctypes import (
    CFUNCTYPE,
    POINTER,
    Structure,
    byref,
    c_bool,
    c_char,
    c_char_p,
    c_int,
    c_uint16,
    c_uint32,
    c_uint64,
    c_void_p,
)

import aerospike

AEROSPIKE_OK = 0
AS_ERROR_MESSAGE_MAX_SIZE = 1024

TRANSLATOR_KINDS = {"static-map": 0, "port-map": 1, "pass-through": 2}
MISS_POLICIES = {"pass_through": 0, "strict": 1}
MERGE_POLICIES = {"merge": 0, "replace": 1, "discovery_only": 2}
ENDPOINT_SOURCES = {"standard": 0, "alternate": 1}
DNS_RECORD_TYPES = {"a": 0, "srv": 1}


class AsError(Structure):
    _fields_ = [
        ("code", c_int),
        ("subcode", c_uint32),
        ("message", c_char * AS_ERROR_MESSAGE_MAX_SIZE),
        ("func", c_char_p),
        ("file", c_char_p),
        ("line", c_uint32),
        ("in_doubt", c_bool),
    ]


# The strings are void pointers so hosts the C client allocates can be released with cf_free
class AsHost(Structure):
    _fields_ = [("name", c_void_p), ("tls_name", c_void_p), ("port", c_uint16)]


class AsVector(Structure):
    _fields_ = [
        ("list", c_void_p),
        ("capacity", c_uint32),
        ("size", c_uint32),
        ("item_size", c_uint32),
        ("flags", c_uint32),
    ]


class AsAddrMap(Structure):
    _fields_ = [("orig", c_char_p), ("alt", c_char_p)]


class AsConformanceTranslation(Structure):
    _fields_ = [
        ("kind", c_int),
        ("on_miss", c_int),
        ("map", POINTER(AsAddrMap)),
        ("map_size", c_uint32),
    ]


class AsEndpoint(Structure):
    _fields_ = [
        ("node_name", c_char_p),
        ("tls_name", c_char_p),
        ("host", c_char_p),
        ("port", c_uint16),
        ("source", c_int),
    ]


class AsSeedCandidates(Structure):
    _fields_ = [("hosts", POINTER(AsVector))]


class AsSeedCandidateContext(Structure):
    _fields_ = [
        ("cluster_name", c_char_p),
        ("configured_seeds", POINTER(AsVector)),
        ("tend_iteration", c_uint64),
    ]


RefreshFunction = CFUNCTYPE(
    c_int, POINTER(AsError), POINTER(AsSeedCandidateContext), POINTER(AsSeedCandidates), c_void_p
)


class AsSeedProvider(Structure):
    _fields_ = [
        ("size", c_uint32),
        ("api_version", c_uint32),
        ("name", c_char_p),
        ("refresh", RefreshFunction),
        ("needs_periodic_refresh", CFUNCTYPE(c_bool, c_void_p)),
        ("supports_deadline", CFUNCTYPE(c_bool, c_void_p)),
        ("destroy", CFUNCTYPE(None, c_void_p)),
        ("udata", c_void_p),
    ]


class AsDnsSeedProviderConfig(Structure):
    _fields_ = [
        ("names", POINTER(c_char_p)),
        ("names_size", c_uint32),
        ("record_type", c_int),
        ("port", c_uint16),
        ("tls_name", c_char_p),
        ("timeout_ms", c_uint32),
        ("servers", POINTER(c_char_p)),
        ("servers_size", c_uint32),
    ]


def _load():
    lib = ctypes.CDLL(aerospike.__file__)
    signatures = {
        "as_discovery_conformance_translate": (
            c_int,
            [POINTER(AsError), POINTER(AsConformanceTranslation), POINTER(AsEndpoint), POINTER(AsHost)],
        ),
        "as_discovery_conformance_merge": (
            c_int,
            [
                POINTER(AsError),
                c_int,
                POINTER(AsVector),
                POINTER(POINTER(AsVector)),
                c_uint32,
                POINTER(AsSeedCandidates),
            ],
        ),
        "as_dns_seed_provider_config_init": (None, [POINTER(AsDnsSeedProviderConfig)]),
        "as_discovery_dns_seed_provider": (
            c_int,
            [POINTER(AsError), POINTER(AsDnsSeedProviderConfig), POINTER(AsSeedProvider)],
        ),
        "as_seed_candidates_init": (c_bool, [POINTER(AsSeedCandidates), c_uint32]),
        "as_seed_candidates_destroy": (None, [POINTER(AsSeedCandidates)]),
        "cf_free": (None, [c_void_p]),
    }
    for name, (restype, argtypes) in signatures.items():
        function = getattr(lib, name)
        function.restype = restype
        function.argtypes = argtypes
    return lib


lib = _load()


class CError(Exception):
    """A C entry point returned an error."""

    def __init__(self, err):
        self.code = err.code
        self.message = err.message.decode()
        super().__init__(f"{self.code}: {self.message}")


def _encode(text):
    if text is None:
        return None
    return str(text).encode()


def _decode(pointer):
    if not pointer:
        return None
    return ctypes.string_at(pointer).decode()


class HostVector:
    """An as_vector of as_host in Python memory, for the C client to read."""

    def __init__(self, hosts):
        self._strings = []
        self._array = (AsHost * max(len(hosts), 1))()
        for i, host in enumerate(hosts):
            self._array[i].name = self._keep(host["host"])
            self._array[i].tls_name = self._keep(host.get("tlsName"))
            self._array[i].port = host["port"]
        self.vector = AsVector(
            ctypes.cast(self._array, c_void_p), len(hosts), len(hosts), ctypes.sizeof(AsHost), 0
        )

    def _keep(self, text):
        if text is None:
            return None
        buffer = ctypes.create_string_buffer(_encode(text))
        self._strings.append(buffer)
        return ctypes.addressof(buffer)


def read_hosts(vector_pointer):
    """Return the hosts of an as_vector of as_host as fixture host dicts."""
    if not vector_pointer:
        return []
    vector = vector_pointer.contents
    array = ctypes.cast(vector.list, POINTER(AsHost))
    return [
        {"host": _decode(array[i].name), "port": array[i].port, "tlsName": _decode(array[i].tls_name)}
        for i in range(vector.size)
    ]


def translate(kind, on_miss, entries, endpoint):
    """Run one translate row. Return the effective host dict, or raise CError."""
    map_array = (AsAddrMap * max(len(entries), 1))()
    for i, (orig, alt) in enumerate(entries.items()):
        map_array[i] = AsAddrMap(_encode(orig), _encode(alt))
    translation = AsConformanceTranslation(
        TRANSLATOR_KINDS[kind], MISS_POLICIES[on_miss], map_array, len(entries)
    )
    c_endpoint = AsEndpoint(
        _encode(endpoint["nodeName"]),
        _encode(endpoint.get("tlsName")),
        _encode(endpoint["host"]),
        endpoint["port"],
        ENDPOINT_SOURCES[endpoint["sourceList"]],
    )
    err = AsError()
    out = AsHost()
    status = lib.as_discovery_conformance_translate(byref(err), byref(translation), byref(c_endpoint), byref(out))
    if status != AEROSPIKE_OK:
        raise CError(err)
    result = {"host": _decode(out.name), "port": out.port, "tlsName": _decode(out.tls_name)}
    lib.cf_free(out.name)
    lib.cf_free(out.tls_name)
    return result


def merge(policy, static_hosts, results):
    """Return the seed snapshot after each discovery result. A None result is a failed refresh."""
    statics = HostVector(static_hosts)
    vectors = [None if result is None else HostVector(result) for result in results]
    pointers = (POINTER(AsVector) * max(len(results), 1))()
    for i, vector in enumerate(vectors):
        if vector is not None:
            pointers[i] = ctypes.pointer(vector.vector)
    snapshots = (AsSeedCandidates * max(len(results), 1))()
    err = AsError()
    status = lib.as_discovery_conformance_merge(
        byref(err), MERGE_POLICIES[policy], byref(statics.vector), pointers, len(results), snapshots
    )
    if status != AEROSPIKE_OK:
        raise CError(err)
    seeds = []
    for i in range(len(results)):
        seeds.append(read_hosts(snapshots[i].hosts))
        lib.as_seed_candidates_destroy(byref(snapshots[i]))
    return seeds


class DnsSeedProvider:
    """The C client's built-in DNS seed provider."""

    def __init__(self, dns):
        names = dns.get("names") or []
        servers = dns.get("servers") or []
        self._names = (c_char_p * max(len(names), 1))(*[_encode(name) for name in names])
        self._servers = (c_char_p * max(len(servers), 1))(*[_encode(server) for server in servers])
        self._tls_name = _encode(dns.get("tls_name"))

        config = AsDnsSeedProviderConfig()
        lib.as_dns_seed_provider_config_init(byref(config))
        config.names = self._names
        config.names_size = len(names)
        config.servers = self._servers
        config.servers_size = len(servers)
        if "record_type" in dns:
            config.record_type = DNS_RECORD_TYPES[dns["record_type"].lower()]
        if "port" in dns:
            config.port = dns["port"]
        if self._tls_name is not None:
            config.tls_name = self._tls_name
        if "timeout_ms" in dns:
            config.timeout_ms = dns["timeout_ms"]

        self._provider = AsSeedProvider()
        err = AsError()
        status = lib.as_discovery_dns_seed_provider(byref(err), byref(config), byref(self._provider))
        if status != AEROSPIKE_OK:
            raise CError(err)
        self._configured_seeds = HostVector([])

    def refresh(self, tend_iteration):
        """Return the refreshed hosts, or raise CError."""
        candidates = AsSeedCandidates()
        if not lib.as_seed_candidates_init(byref(candidates), 8):
            raise MemoryError("as_seed_candidates_init failed")
        context = AsSeedCandidateContext(None, ctypes.pointer(self._configured_seeds.vector), tend_iteration)
        err = AsError()
        try:
            status = self._provider.refresh(byref(err), byref(context), byref(candidates), self._provider.udata)
            if status != AEROSPIKE_OK:
                raise CError(err)
            return read_hosts(candidates.hosts)
        finally:
            lib.as_seed_candidates_destroy(byref(candidates))

    def close(self):
        if self._provider.destroy:
            self._provider.destroy(self._provider.udata)
        self._provider = None

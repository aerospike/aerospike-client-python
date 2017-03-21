import pytest
import socket
import time
from . import invalid_data
from .test_base_class import TestBaseClass
aerospike = pytest.importorskip("aerospike")


def wait_for_port(address, port, interval=0.1, timeout=60):
    """Wait for a TCP / IP port to accept a connection.

    : param port: The port to check.
    : param interval: The interval(seconds) between checks.
    : param timeout: The total time(seconds) to check before quiting.
    """
    start = time.time()
    while True:
        current = time.time()
        if current - start >= timeout:
            break
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((address, port))
            s.close()
            return True
        except Exception as e:
            pass
        time.sleep(interval)
    return False


@pytest.fixture(scope="class")
def as_connection(request):
    hostlist, user, password = TestBaseClass.get_hosts()
    config = {'hosts': hostlist}
    as_client = None

    for (a, p) in hostlist:
        wait_for_port(a, p)

    if user is None and password is None:
        as_client = aerospike.client(config).connect()
    else:
        as_client = aerospike.client(config).connect(user, password)

    request.cls.skip_old_server = True
    versioninfo = as_client.info('version')
    for keys in versioninfo:
        for value in versioninfo[keys]:
            if value is not None:
                versionlist = value[value.find("build") +
                                    6:value.find("\n")].split(".")
                if int(versionlist[0]) >= 3 and int(versionlist[1]) >= 6:
                    request.cls.skip_old_server = False

    request.cls.as_connection = as_client

    def close_connection():
        as_client.close()

    request.addfinalizer(close_connection)
    return as_client


@pytest.fixture(scope='class')
def connection_with_udf(request, as_connection):
    """
    Injects an as_client() as a dependency, loads a udf to it,
    and at the end of the test class removes it.
    Note: if the requesting class does not have a class attr:
    `udf_to_load`, this is essentially a noop
    """
    udf_status = {'loaded': False, 'name': None}
    # if the class doesn't have the correct information,
    # don't bother loading a UDF
    if hasattr(request.cls, 'udf_to_load'):
        udf_status['name'] = request.cls.udf_to_load
        as_connection.udf_put(udf_status['name'], 0, {})
        udf_status['loaded'] = True

    # Yield to the requesting context
    yield as_connection

    # If a UDF has been loaded, remove it
    if udf_status['loaded']:
        try:
            as_connection.udf_remove(udf_status['name'])
        except:  # If this fails, it has already been removed
            pass


@pytest.fixture(scope='class')
def connection_with_config_funcs(request, as_connection):
    """
    Injects a connected as_client() as a dependency, and runs arbitrary
    setup functions on it, ideally these would be expensive functions
    which should only be run once per test category: indexing
    loading udfs. Then yields the connection back to the requesting context
    """
    setup_status = False
    if hasattr(request.cls, 'connection_setup_functions'):
        for func in request.cls.connection_setup_functions:
            func(as_connection)
        setup_status = True

    # Yield to the requesting context
    yield as_connection

    # If any setup was done, run the corresponding teardown functions
    if setup_status and hasattr(request.cls, 'connection_teardown_functions'):
        for func in request.cls.connection_teardown_functions:
            func(as_connection)
        setup_status = False


@pytest.fixture()
def put_data(request):
    put_data.key = None
    put_data.keys = []
    put_data.client = None

    def put_key(client, _key, _record, _meta=None, _policy=None):
        put_data.key = _key
        put_data.client = client
        try:
            client.remove(_key)
        except:
            pass
        put_data.keys.append(put_data.key)
        return client.put(_key, _record, _meta, _policy)

    def remove_key():
        try:
            # pytest.set_trace()
            for key in put_data.keys:
                put_data.client.remove(key)
        except:
            pass

    request.addfinalizer(remove_key)
    return put_key


@pytest.fixture(scope="class")
def connection_config(request):
    """
    Sets the class attribute to be the config object passed in
     to create the as_connection
    """
    hostlist, _, _ = TestBaseClass.get_hosts()
    config = {'hosts': hostlist}
    request.cls.connection_config = config


@pytest.fixture(params=invalid_data.INVALID_KEYS)
def invalid_key(request):
    yield request.param

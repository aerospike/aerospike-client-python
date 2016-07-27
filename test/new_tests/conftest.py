import pytest
import socket
import time
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

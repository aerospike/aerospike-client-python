import pytest
from .test_base_class import TestBaseClass
aerospike = pytest.importorskip("aerospike")


@pytest.fixture(scope="class")
def as_connection(request):
    hostlist, user, password = TestBaseClass.get_hosts()
    config = {'hosts': hostlist}
    as_client = None
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

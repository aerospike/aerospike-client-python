import pytest
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestInvalidClientConfig(object):

    def test_no_config(self):
        with pytest.raises(e.ParamError) as err:
            client = aerospike.client()
        assert "No config argument" in err.value.msg

    def test_config_not_dict(self):
        with pytest.raises(e.ParamError) as err:
            client = aerospike.client([])
        assert "Config must be a dict" in err.value.msg

    def test_no_host_in_config(self):
        with pytest.raises(e.ParamError) as err:
            client = aerospike.client({})
        assert "Hosts must be a list" in err.value.msg

    def test_wrong_host_type(self):
        with pytest.raises(e.ParamError) as err:
            client = aerospike.client({'hosts': (())})
        assert "Hosts must be a list" in err.value.msg

    def test_empty_host_in_config(self):
        with pytest.raises(e.ParamError) as err:
            client = aerospike.client({'hosts': []})
        assert "Hosts must not be empty" in err.value.msg

    def test_invalid_host_in_list(self):
        with pytest.raises(e.ParamError) as err:
            client = aerospike.client(
                {'hosts': [("localhost", 3000), ()]})
        assert "Invalid host" in err.value.msg

    def test_lua_user_path_too_long(self):
        with pytest.raises(e.ParamError) as err:
            client = aerospike.client(
                {
                    'hosts': [
                        ("localhost", 3000)],
                    'lua': {'user_path': 'a' * 256}
                })
        assert "Lua user path too long" in err.value.msg

    def test_lua_sys_path_too_long(self):
        with pytest.raises(e.ParamError) as err:
            client = aerospike.client(
                {
                    'hosts': [
                        ("localhost", 3000)],
                    'lua': {'system_path': 'a' * 256}
                })
        assert "Lua system path too long" in err.value.msg

    def test_non_callable_serializer(self):
        with pytest.raises(e.ParamError) as err:
            client = aerospike.client(
                {
                    'hosts': [
                        ("localhost", 3000)],
                    'serialization': (5, lambda x: 5)
                })
        assert "Serializer must be callable" in err.value.msg

    def test_non_callable_deserializer(self):
        with pytest.raises(e.ParamError) as err:
            client = aerospike.client(
                {
                    'hosts': [
                        ("localhost", 3000)],
                    'serialization': (lambda x: 5, 5)
                })
        assert "Deserializer must be callable" in err.value.msg

    def test_negative_threshold_value(self):
        with pytest.raises(e.ParamError) as err:
            client = aerospike.client(
                {
                    'hosts': [
                        ("localhost", 3000)],
                    'compression_threshold': -1
                })
        assert "Compression value must not be negative" in err.value.msg

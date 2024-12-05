import pytest

from aerospike import ClientConfigDict


class TestClientConfigDict(object):
    def test_api(self):
        config = ClientConfigDict()
        config["hosts"] = []
        with pytest.raises(KeyError) as excinfo:
            config["invalid_key"] = 1
        assert excinfo.value == 'invalid_key is an invalid key for client config dictionary'

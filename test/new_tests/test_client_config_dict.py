import pytest

from aerospike import FixedKeyDict


class TestFixedKeyDict(object):
    def test_api(self):
        config = FixedKeyDict()
        config["hosts"] = []
        with pytest.raises(KeyError) as excinfo:
            config["invalid_key"] = 1
        assert excinfo.value == 'invalid_key is an invalid key for client config dictionary'

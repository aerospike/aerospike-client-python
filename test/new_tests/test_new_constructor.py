# -*- coding: utf-8 -*-

import pytest
from .test_base_class import TestBaseClass
import aerospike
from aerospike import exception as e
from aerospike_helpers.operations import operations
from aerospike_helpers.batch.records import Write, BatchRecords
import copy

gconfig = {}
gconfig = TestBaseClass.get_connection_config()


def test_setting_key():
    key_val = aerospike.POLICY_KEY_SEND
    read_policy = {"key": key_val}
    policies = {"read": read_policy}
    config = copy.deepcopy(gconfig)
    config["policies"].update(policies)

    aerospike.client(config)


# TODO: duplicate test name
@pytest.mark.xfail(reason="Had a duplicate test name, but fails when renamed.")
def test_setting_consistency_duplicate():
    cons_val = aerospike.POLICY_CONSISTENCY_ONE
    read_policy = {"consistency_level": cons_val}
    policies = {"read": read_policy}
    config = copy.deepcopy(gconfig)
    config["policies"].update(policies)
    aerospike.client(config)


def test_setting_consistency():
    replica_val = aerospike.POLICY_REPLICA_MASTER
    read_policy = {"replica": replica_val}
    policies = {"read": read_policy}
    config = copy.deepcopy(gconfig)
    config["policies"].update(policies)
    aerospike.client(config)


def test_setting_conmmit_level():
    commit_val = aerospike.POLICY_COMMIT_LEVEL_ALL
    write_policy = {"commit_level": commit_val}
    policies = {"write": write_policy}
    config = copy.deepcopy(gconfig)
    config["policies"].update(policies)
    aerospike.client(config)


def test_setting_exists():
    exists_val = aerospike.POLICY_EXISTS_CREATE
    write_policy = {"commit_level": exists_val}
    policies = {"write": write_policy}
    config = copy.deepcopy(gconfig)
    config["policies"].update(policies)
    aerospike.client(config)


def test_setting_gen():
    gen_val = aerospike.POLICY_GEN_IGNORE
    write_policy = {"commit_level": gen_val}
    policies = {"write": write_policy}
    config = copy.deepcopy(gconfig)
    config["policies"].update(policies)
    aerospike.client(config)


def test_setting_wrong_type():
    write_policy = {"commit_level": [1, 2, 3]}
    policies = {"write": write_policy}
    config = copy.deepcopy(gconfig)
    config["policies"].update(policies)
    with pytest.raises(e.ParamError):
        aerospike.client(config)


def test_setting_rack_aware_and_rack_id():
    config = copy.deepcopy(gconfig)
    config["rack_aware"] = True
    config["rack_id"] = 0x1234
    client = aerospike.client(config)
    assert client is not None


def test_setting_rack_aware_and_rack_ids():
    config = copy.deepcopy(gconfig)
    config["rack_aware"] = True
    config["rack_ids"] = [1, 3, 4]
    client = aerospike.client(config)
    assert client is not None


@pytest.mark.parametrize(
    "rack_ids",
    (
        4,  # Not a list
        ["str", 4],  # Invalid rack id
    )
)
def test_neg_setting_rack_ids(rack_ids):
    config = copy.deepcopy(gconfig)
    config["rack_ids"] = rack_ids
    with pytest.raises(e.ParamError):
        aerospike.client(config)


def test_setting_use_services_alternate():
    config = copy.deepcopy(gconfig)
    config["use_services_alternate"] = True
    client = aerospike.client(config)
    assert client is not None


def test_setting_rack_aware_non_bool():
    config = copy.deepcopy(gconfig)
    config["rack_aware"] = "True"
    with pytest.raises(e.ParamError):
        aerospike.client(config)


@pytest.mark.parametrize(
    "rack_id",
    (
        "test_id",  # String
        "3.14",  # Float
        -(1 << 40),  # Too small
        (1 << 32),  # Too large
    ),
)
def test_setting_rack_id_wrong_type(rack_id):
    config = copy.deepcopy(gconfig)
    config["rack_id"] = rack_id
    with pytest.raises(e.ParamError):
        aerospike.client(config)


def test_setting_wrong_type_services_alternate():
    """
    'use_services_alternate' should be a boolean
    """
    config = copy.deepcopy(gconfig)
    config["use_services_alternate"] = "True"
    with pytest.raises(e.ParamError):
        aerospike.client(config)


def test_setting_rack_aware():
    config = copy.deepcopy(gconfig)
    config["rack_aware"] = True
    config["rack_id"] = 1
    config["policies"]["batch"]["replica"] = aerospike.POLICY_REPLICA_PREFER_RACK
    config["policies"]["scan"]["replica"] = aerospike.POLICY_REPLICA_PREFER_RACK
    config["policies"]["query"]["replica"] = aerospike.POLICY_REPLICA_PREFER_RACK
    aerospike.client(config)


def test_setting_batch_remove_gen():
    config = copy.deepcopy(gconfig)
    config["policies"]["batch_remove"] = {
        "generation": 24
    }
    aerospike.client(config)


def test_setting_batch_remove_gen_invalid_type():
    config = copy.deepcopy(gconfig)
    config["policies"]["batch_remove"] = {
        "generation": 0.3
    }
    with pytest.raises(e.ParamError) as excinfo:
        aerospike.client(config)
    assert excinfo.value.msg == "Invalid Policy setting value"


def test_setting_batch_remove_gen_too_large():
    config = copy.deepcopy(gconfig)
    config["policies"]["batch_remove"] = {
        # Larger than max size for 16-bit unsigned integer
        "generation": 2**16
    }
    with pytest.raises(e.ParamError) as excinfo:
        aerospike.client(config)
    assert excinfo.value.msg == "Invalid Policy setting value"


def test_setting_batch_remove_gen_neg_value():
    config = copy.deepcopy(gconfig)
    config["policies"]["batch_remove"] = {
        "generation": -1
    }
    with pytest.raises(e.ParamError) as excinfo:
        aerospike.client(config)
    assert excinfo.value.msg == "Invalid Policy setting value"


def test_setting_batch_policies():
    config = copy.deepcopy(gconfig)
    policies = ["batch_remove", "batch_apply", "batch_write", "batch_parent_write"]
    for policy in policies:
        config["policies"][policy] = {}
    aerospike.client(config)


class TestConfigTTL:
    @pytest.fixture
    def config_ttl_setup(self, policy_name: str):
        config = copy.deepcopy(gconfig)
        self.new_ttl = 9000
        config["policies"][policy_name] = {
            "ttl": self.new_ttl
        }
        self.client = aerospike.client(config)
        self.key = ("test", "demo", 0)

        yield

        # Teardown
        if policy_name == "apply":
            self.client.udf_remove("test_record_udf.lua")

    def check_ttl(self):
        _, meta = self.client.exists(self.key)
        clock_skew_tolerance_secs = 50
        assert meta["ttl"] in range(self.new_ttl - clock_skew_tolerance_secs, self.new_ttl + clock_skew_tolerance_secs)

    @pytest.mark.parametrize("policy_name", ["write"])
    def test_setting_write_ttl(self, config_ttl_setup):
        # Call without setting the ttl in the transaction metadata dict
        self.client.put(self.key, bins={"a": 1})
        self.check_ttl()

    @pytest.mark.parametrize("policy_name", ["operate"])
    def test_setting_operate_ttl(self, config_ttl_setup):
        # Call without setting the ttl in the transaction metadata dict
        ops = [
            operations.write("a", 1)
        ]
        self.client.operate(self.key, ops)
        self.check_ttl()

    @pytest.mark.parametrize("policy_name", ["apply"])
    def test_setting_apply_ttl(self, config_ttl_setup):
        # Setup
        self.client.udf_put("test_record_udf.lua")
        self.client.put(self.key, {"bin": "a"})

        # Call without setting the ttl in the transaction's apply policy
        # Args: bin name, str
        self.client.apply(self.key, module="test_record_udf", function="bin_udf_operation_string", args=["bin", "a"])
        self.check_ttl()

    @pytest.mark.parametrize("policy_name", ["batch_write"])
    def test_setting_batch_write_ttl(self, config_ttl_setup):
        # Call without setting the ttl in the Write BatchRecord's metadata dict
        ops = [
            operations.write("bin", 1)
        ]
        batch_records = BatchRecords([
            Write(self.key, ops=ops)
        ])
        self.client.batch_write(batch_records)

        self.check_ttl()

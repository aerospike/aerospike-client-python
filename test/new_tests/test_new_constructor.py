# -*- coding: utf-8 -*-

import pytest
from .test_base_class import TestBaseClass
import aerospike
from aerospike import exception as e
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
    aerospike.client(config)


def test_setting_batch_remove_gen_too_large():
    config = copy.deepcopy(gconfig)
    config["policies"]["batch_remove"] = {
        # Larger than max size for 16-bit unsigned integer
        "generation": 2**16
    }
    aerospike.client(config)


def test_setting_batch_remove_gen_neg_value():
    config = copy.deepcopy(gconfig)
    config["policies"]["batch_remove"] = {
        "generation": -1
    }
    aerospike.client(config)

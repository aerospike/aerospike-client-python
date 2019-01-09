# -*- coding: utf-8 -*-

import pytest
import sys
import json
from .test_base_class import TestBaseClass
from aerospike import exception as e

host, user, password = TestBaseClass.get_hosts()
using_auth = user or password
aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


def test_setting_key():
    key_val = aerospike.POLICY_KEY_SEND
    read_policy = {'key': key_val}
    policies = {'read': read_policy}
    config = {'hosts': host, 'policies': policies}
    client = aerospike.client(config)


def test_setting_consistency():
    cons_val = aerospike.POLICY_CONSISTENCY_ONE
    read_policy = {'consistency_level': cons_val}
    policies = {'read': read_policy}
    config = {'hosts': host, 'policies': policies}
    client = aerospike.client(config)


def test_setting_consistency():
    replica_val = aerospike.POLICY_REPLICA_MASTER
    read_policy = {'replica': replica_val}
    policies = {'read': read_policy}
    config = {'hosts': host, 'policies': policies}
    client = aerospike.client(config)


def test_setting_consistency():
    replica_val = aerospike.POLICY_REPLICA_MASTER
    read_policy = {'replica': replica_val}
    policies = {'read': read_policy}
    config = {'hosts': host, 'policies': policies}
    client = aerospike.client(config)


def test_setting_conmmit_level():
    commit_val = aerospike.POLICY_COMMIT_LEVEL_ALL
    write_policy = {'commit_level': commit_val}
    policies = {'write': write_policy}
    config = {'hosts': host, 'policies': policies}
    client = aerospike.client(config)


def test_setting_exists():
    exists_val = aerospike.POLICY_EXISTS_CREATE
    write_policy = {'commit_level': exists_val}
    policies = {'write': write_policy}
    config = {'hosts': host, 'policies': policies}
    client = aerospike.client(config)


def test_setting_gen():
    gen_val = aerospike.POLICY_GEN_IGNORE
    write_policy = {'commit_level': gen_val}
    policies = {'write': write_policy}
    config = {'hosts': host, 'policies': policies}
    client = aerospike.client(config)


def test_setting_wrong_type():
    write_policy = {'commit_level': [1, 2, 3]}
    policies = {'write': write_policy}
    config = {'hosts': host, 'policies': policies}
    with pytest.raises(e.ParamError):
        client = aerospike.client(config)

def test_setting_rack_aware_and_rack_id():
    config = {'hosts': host, 'rack_aware': True, 'rack_id': 0x1234}
    client = aerospike.client(config)
    assert client is not None

def test_setting_rack_aware_non_bool():
    config = {'hosts': host, 'rack_aware': "True"}
    with pytest.raises(e.ParamError):
        client = aerospike.client(config)

@pytest.mark.parametrize(
    "rack_id",
    (
        'test_id', # String
        '3.14', # Float
        -(1 << 40), # Too small
        (1 << 32), # Too large
    )    
)
def test_setting_rack_id_wrong_type(rack_id):
    config = {'hosts': host, 'rack_id': rack_id}
    with pytest.raises(e.ParamError):
        client = aerospike.client(config)

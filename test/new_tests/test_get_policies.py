# -*- coding: utf-8 -*-
import copy

import pytest

import aerospike
from .test_base_class import TestBaseClass

gconfig = TestBaseClass.get_connection_config()

TOP_LEVEL_POLICY_KEYS = {
    "read", "write", "apply", "remove", "query", "scan", "operate", "info",
    "admin", "batch_apply", "batch_remove", "batch_write", "batch",
    "batch_parent_write", "txn_verify", "txn_roll",
}

BASE_POLICY_FIELDS = {
    "total_timeout", "connect_timeout", "socket_timeout", "timeout_delay",
    "max_retries", "sleep_between_retries", "compress",
    "error_detail_verbosity",
}


class TestGetPolicies(object):
    """
    Test cases for aerospike.Client.get_policies
    """

    def test_pos_get_policies_default(self):
        """
        get_policies() returns every documented top-level policy key,
        even before connect() is called.
        """
        config = copy.deepcopy(gconfig)
        client = aerospike.client(config)

        policies = client.get_policies()

        assert isinstance(policies, dict)
        assert set(policies.keys()) == TOP_LEVEL_POLICY_KEYS
        for sub_policy in policies.values():
            assert isinstance(sub_policy, dict)

    def test_pos_get_policies_base_fields_flattened(self):
        """
        Base policy fields are merged directly into each sub-policy dict,
        not nested under a separate key.
        """
        config = copy.deepcopy(gconfig)
        client = aerospike.client(config)

        policies = client.get_policies()

        for policy_name in ("read", "write", "apply", "remove", "query",
                            "scan", "operate", "batch"):
            assert BASE_POLICY_FIELDS.issubset(policies[policy_name].keys())

        # info/admin/batch_apply/batch_write/batch_remove have no base policy
        for policy_name in ("info", "admin"):
            assert policies[policy_name].keys() == {"timeout"}

    def test_pos_get_policies_reflects_config(self):
        """
        Values explicitly set at construction time are round-tripped by
        get_policies(), including enum-like fields (returned as plain int)
        and boolean fields.
        """
        config = copy.deepcopy(gconfig)
        config["policies"]["read"]["total_timeout"] = 4321
        config["policies"]["read"]["key"] = aerospike.POLICY_KEY_SEND
        config["policies"]["write"]["gen"] = aerospike.POLICY_GEN_EQ
        config["policies"]["write"]["durable_delete"] = True
        config["policies"]["info"]["timeout"] = 777
        config["policies"]["batch_remove"] = {"generation": 24}

        client = aerospike.client(config)
        policies = client.get_policies()

        assert policies["read"]["total_timeout"] == 4321
        assert policies["read"]["key"] == aerospike.POLICY_KEY_SEND
        assert isinstance(policies["read"]["key"], int)

        assert policies["write"]["gen"] == aerospike.POLICY_GEN_EQ
        assert policies["write"]["durable_delete"] is True

        assert policies["info"]["timeout"] == 777

        assert policies["batch_remove"]["generation"] == 24

    def test_pos_get_policies_bool_fields_are_bool(self):
        """
        Boolean policy fields come back as bool, not as raw ints.
        """
        config = copy.deepcopy(gconfig)
        client = aerospike.client(config)

        policies = client.get_policies()

        assert isinstance(policies["read"]["compress"], bool)
        assert isinstance(policies["read"]["deserialize"], bool)
        assert isinstance(policies["write"]["durable_delete"], bool)
        assert isinstance(policies["batch"]["concurrent"], bool)

    def test_pos_get_policies_shared_batch_policy_keys(self):
        """
        batch, batch_parent_write, txn_verify, and txn_roll all use the
        as_policy_batch shape.
        """
        config = copy.deepcopy(gconfig)
        client = aerospike.client(config)

        policies = client.get_policies()

        batch_keys = policies["batch"].keys()
        for policy_name in ("batch_parent_write", "txn_verify", "txn_roll"):
            assert policies[policy_name].keys() == batch_keys

    @pytest.mark.usefixtures("as_connection")
    def test_pos_get_policies_after_connect(self):
        """
        get_policies() also works normally on a connected client.
        """
        policies = self.as_connection.get_policies()
        assert set(policies.keys()) == TOP_LEVEL_POLICY_KEYS

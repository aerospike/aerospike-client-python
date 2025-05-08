# -*- coding: utf-8 -*-

import pytest
from .test_base_class import TestBaseClass
import aerospike
from aerospike import exception as e
from aerospike import ConfigProvider
from aerospike_helpers.operations import operations
from aerospike_helpers.batch.records import Write, BatchRecords
from aerospike_helpers.metrics import MetricsPolicy
from .test_scan_execute_background import wait_for_job_completion
import copy
from contextlib import nullcontext
import time
import glob
import re
import os

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
    policies = ["batch_remove", "batch_apply", "batch_write", "batch_parent_write", "txn_verify", "txn_roll"]
    for policy in policies:
        config["policies"][policy] = {}
    aerospike.client(config)


def test_setting_metrics_policy():
    config = copy.deepcopy(gconfig)
    BUCKET_COUNT = 3
    METRICS_LOG_FILES = "./metrics-*.log"

    # Enable dynamic config to apply config-level metrics policy
    # Invalid report_size_limit value from config-level metrics policy should not be applied
    config["policies"]["metrics"] = MetricsPolicy(latency_columns=BUCKET_COUNT, report_size_limit=1)
    config["config_provider"] = ConfigProvider("./dyn_config.yml")
    client = aerospike.client(config)
    try:
        client.enable_metrics()
        time.sleep(2)
        client.disable_metrics()

        metrics_log_filenames = glob.glob(METRICS_LOG_FILES)
        try:
            with open(metrics_log_filenames[0]) as f:
                # Second line has data
                f.readline()
                data = f.readline()
            regex = re.search(pattern=r"conn\[([0-9]|,)+\]", string=data)
            buckets = regex.group(0)
            # <bucket>,<bucket>,...
            bucket_count = len(buckets.split(','))
            assert bucket_count == BUCKET_COUNT
        finally:
            for item in metrics_log_filenames:
                os.remove(item)
    finally:
        client.close()


@pytest.mark.parametrize(
    "policy, expected_exc_class",
    [
        # Common error is to leave a comma at the end
        # MetricsPolicy(),
        (tuple([MetricsPolicy()]), e.ParamError),
        (MetricsPolicy(report_size_limit=2**64), e.ClientError)
    ]
)
def test_setting_invalid_metrics_policy(policy, expected_exc_class):
    config = copy.deepcopy(gconfig)
    config["policies"]["metrics"] = policy
    with pytest.raises(expected_exc_class) as excinfo:
        aerospike.client(config)
    if expected_exc_class == e.ParamError:
        assert excinfo.value.msg == "metrics must be an aerospike_helpers.metrics.MetricsPolicy class instance. But "\
            "a tuple was received instead"


def test_query_invalid_expected_duration():
    config = copy.deepcopy(gconfig)
    config["policies"]["query"]["expected_duration"] = "1"
    with pytest.raises(e.ParamError) as excinfo:
        aerospike.client(config)
    assert excinfo.value.msg == "Invalid Policy setting value"


class TestConfigTTL:
    NEW_TTL = 9000

    @pytest.fixture
    def config_ttl_setup(self, policy_name: str):
        config = copy.deepcopy(gconfig)
        config["policies"][policy_name] = {
            "ttl": self.NEW_TTL
        }
        self.client = aerospike.client(config)
        self.key = ("test", "demo", 0)

        if "apply" in policy_name:
            self.client.udf_put("test_record_udf.lua")

        yield

        # Teardown

        if "apply" in policy_name:
            try:
                self.client.udf_remove("test_record_udf.lua")
            except e.UDFError:
                # In case UDF module does not exist
                pass

        try:
            self.client.remove(self.key)
        except e.RecordNotFound:
            pass

    def check_ttl(self):
        _, meta = self.client.exists(self.key)
        clock_skew_tolerance_secs = 50
        assert meta["ttl"] in range(self.NEW_TTL - clock_skew_tolerance_secs, self.NEW_TTL + clock_skew_tolerance_secs)

    @pytest.mark.parametrize("policy_name", ["write"])
    @pytest.mark.parametrize(
        "meta",
        [None, {"ttl": aerospike.TTL_CLIENT_DEFAULT}, {"gen": 10}],
        ids=["no metadata", "metadata with special ttl value", "metadata without ttl"]
    )
    def test_setting_write_ttl(self, config_ttl_setup, meta):
        self.client.put(self.key, bins={"a": 1}, meta=meta)
        self.check_ttl()

    @pytest.mark.parametrize("policy_name", ["operate"])
    @pytest.mark.parametrize(
        "meta",
        # The reason we also test a metadata dict without ttl for operate()
        # is the codepath that handles the metadata dict for operate() is different
        # from that for put()
        [None, {"ttl": aerospike.TTL_CLIENT_DEFAULT}, {"gen": 10}],
        ids=["no metadata", "metadata with special ttl value", "metadata without ttl"]
    )
    def test_setting_operate_ttl(self, config_ttl_setup, meta):
        ops = [
            operations.write("a", 1)
        ]
        self.client.operate(self.key, ops, meta=meta)
        self.check_ttl()

    @pytest.mark.parametrize("policy_name", ["apply"])
    def test_setting_apply_ttl(self, config_ttl_setup):
        # Setup
        self.client.put(self.key, {"bin": "a"})

        # Call without setting the ttl in the transaction's apply policy
        # Args: bin name, str
        self.client.apply(self.key, module="test_record_udf", function="bin_udf_operation_string", args=["bin", "a"])
        self.check_ttl()

    @pytest.mark.parametrize("policy_name", ["batch_write"])
    @pytest.mark.parametrize(
        "meta",
        [None, {"ttl": aerospike.TTL_CLIENT_DEFAULT}],
        ids=["no metadata", "metadata with special ttl value"]
    )
    def test_setting_batch_write_ttl_with_batch_write(self, config_ttl_setup, meta):
        ops = [
            operations.write("bin", 1)
        ]
        batch_records = BatchRecords([
            Write(self.key, ops=ops, meta=meta)
        ])
        brs = self.client.batch_write(batch_records)
        # assert brs.result == 0
        for br in brs.batch_records:
            assert br.result == 0

        self.check_ttl()

    @pytest.mark.parametrize("policy_name", ["batch_write"])
    @pytest.mark.parametrize(
        "ttl",
        [None, aerospike.TTL_CLIENT_DEFAULT],
    )
    def test_setting_batch_write_ttl_with_batch_operate(self, config_ttl_setup, ttl):
        ops = [
            operations.write("bin", 1)
        ]
        keys = [self.key]
        brs = self.client.batch_operate(keys, ops, ttl=ttl)
        # assert brs.result == 0
        for br in brs.batch_records:
            assert br.result == 0

        self.check_ttl()

    @pytest.mark.parametrize("policy_name", ["batch_apply"])
    def test_setting_batch_apply_ttl(self, config_ttl_setup):
        # Setup
        self.client.put(self.key, {"bin": "a"})

        # Call without setting the ttl in batch_apply()'s batch apply policy
        keys = [
            self.key
        ]
        self.client.batch_apply(keys, module="test_record_udf", function="bin_udf_operation_string", args=["bin", "a"])
        self.check_ttl()

    @pytest.mark.parametrize("policy_name", ["scan"])
    def test_setting_scan_ttl(self, config_ttl_setup):
        # Setup
        self.client.put(self.key, {"bin": "a"})

        # Tell scan to use client config's scan policy ttl
        scan = self.client.scan("test", "demo")
        scan.ttl = aerospike.TTL_CLIENT_DEFAULT
        ops = [
            operations.append("bin", "a")
        ]
        scan.add_ops(ops)
        job_id = scan.execute_background()

        wait_for_job_completion(self.client, job_id)

        self.check_ttl()

    @pytest.mark.parametrize("policy_name", ["write"])
    def test_query_client_default_ttl(self, config_ttl_setup):
        # Setup
        self.client.put(self.key, {"bin": "a"}, meta={"ttl": 90})

        # Tell scan to use client config's write policy ttl
        query = self.client.query("test", "demo")
        query.ttl = aerospike.TTL_CLIENT_DEFAULT
        ops = [
            operations.append("bin", "a")
        ]
        query.add_ops(ops)
        job_id = query.execute_background()

        wait_for_job_completion(self.client, job_id)

        self.check_ttl()

    @pytest.mark.parametrize(
        "policy_name",
        [
            "read",
            "operate",
            "batch",
        ]
    )
    def test_invalid_read_touch_ttl_percent(self, policy_name: str):
        config = copy.deepcopy(gconfig)
        config["policies"][policy_name]["read_touch_ttl_percent"] = "fail"
        with pytest.raises(e.ParamError) as excinfo:
            aerospike.client(config)
        assert excinfo.value.msg == "Invalid Policy setting value"

    @pytest.mark.parametrize(
        "config, context",
        [
            (
                gconfig,
                nullcontext()
            ),
            (
                {
                    "hosts": [("invalid-host", 4000)]
                },
                # Tests that fail to connect should expect any of these exceptions
                pytest.raises((e.ConnectionError, e.TimeoutError, e.ClientError))
            )
        ]
    )
    def test_client_class_constructor(self, config: dict, context):
        with context:
            aerospike.Client(config)

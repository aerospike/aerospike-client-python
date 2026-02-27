# -*- coding: utf-8 -*-

import pytest
from .test_base_class import TestBaseClass
import aerospike
from aerospike import exception as e
from aerospike_helpers.operations import operations
from aerospike_helpers.batch.records import Write, BatchRecords
from aerospike_helpers.metrics import MetricsPolicy
import copy
from contextlib import nullcontext
import time
import glob
import re
import os
from .conftest import verify_record_ttl, wait_for_job_completion
import warnings

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


@pytest.mark.parametrize(
    "setting",
    [
        "use_services_alternate",
        "force_single_node",
        "fail_if_not_connected"
     ]
)
def test_bool_settings(setting):
    config = copy.deepcopy(gconfig)
    config[setting] = True
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


@pytest.mark.parametrize(
    "compress, expected_cm",
    [
        (True, nullcontext()),
        (0.2, pytest.raises(e.ParamError))
    ]
)
def test_setting_compress(compress, expected_cm):
    config = copy.deepcopy(gconfig)
    config["policies"]["read"]["compress"] = compress
    with expected_cm:
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

    config["policies"]["metrics"] = MetricsPolicy(latency_columns=BUCKET_COUNT)
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

# We want to make sure that these options are allowed when config["validate_keys"] is True
# Some of these options may not be documented, but they are allowed in the code and customers may be using them
def test_config_level_misc_options():
    config = copy.deepcopy(gconfig)
    config["policies"]["total_timeout"] = 1
    config["policies"]["max_retries"] = 1
    config["policies"]["exists"] = aerospike.POLICY_EXISTS_CREATE
    config["policies"]["replica"] = aerospike.POLICY_REPLICA_MASTER
    config["policies"]["read_mode_ap"] = aerospike.POLICY_READ_MODE_AP_ALL
    config["policies"]["commit_level"] = aerospike.POLICY_COMMIT_LEVEL_ALL
    config["policies"]["max_threads"] = 16
    config["policies"]["thread_pool_size"] = 16
    config["policies"]["socket_timeout"] = 0
    config["thread_pool_size"] = 16
    config["max_threads"] = 16
    config["max_conns_per_node"] = 16
    config["connect_timeout"] = 16
    config["use_shared_connection"] = False
    config["compression_threshold"] = 50
    config["cluster_name"] = "test"
    config["max_socket_idle"] = 20
    config["fail_if_not_connected"] = True
    if "shm" not in config:
        config["shm"] = {}
    config["shm"] = {}
    config["shm"]["max_namespaces"] = 8
    config["shm"]["max_nodes"] = 3
    config["shm"]["takeover_threshold_sec"] = 30
    config["tls"]["crl_check"] = True
    config["tls"]["crl_check_all"] = True
    config["tls"]["log_session_info"] = True
    config["tls"]["for_login_only"] = True
    config["tls"]["cafile"] = "./dummy"
    config["tls"]["capath"] = "./dummy"
    config["tls"]["protocols"] = "blaah"
    config["tls"]["cipher_suite"] = "aes_256"
    config["tls"]["cert_blacklist"] = "aes_256"
    config["tls"]["keyfile"] = "aes_256"
    config["tls"]["certfile"] = "aes_256"
    config["tls"]["keyfile_pw"] = "aes_256"
    config["validate_keys"] = True

    # We don't care if the client connects or not
    # We just make sure that the above options are allowed as dict keys
    try:
        aerospike.client(config)
    except e.ParamError as exc:
        raise exc
    except:
        pass

KEY = ("test", "demo", 0)

class TestConfigTTL:
    NEW_TTL = 9000

    @pytest.fixture
    def config_ttl_setup(self, policy_name: str):
        config = copy.deepcopy(gconfig)
        config["policies"][policy_name] = {
            "ttl": self.NEW_TTL
        }
        self.client = aerospike.client(config)
        self.client.put(KEY, {"a": "a", "b": "b", "c": 1})

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
            self.client.remove(KEY)
        except e.RecordNotFound:
            pass

        self.client.close()

    ttl_param = pytest.mark.parametrize(
        "kwargs_with_ttl",
        [
            {"meta": None},
            {"meta": {"gen": 10}},
            {"meta": {"ttl": aerospike.TTL_CLIENT_DEFAULT, "gen": 10}},
            {"policy": None},
            # {"policy": {}},
        ]
    )

    # Don't bother testing for DeprecationWarnings here since running Python with -W error flag can
    # cause ClientError to be raised. It's too complicated to check both cases
    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    @ttl_param
    @pytest.mark.parametrize("api_method, kwargs, policy_name", [
        (
            aerospike.Client.put,
            {"key": KEY, "bins": {"a": 1}},
            "write"
        ),
        (
            aerospike.Client.remove_bin,
            {"key": KEY, "list": ["a"]},
            "write"
        ),
        (
            aerospike.Client.operate,
            {"key": KEY, "list": [operations.write("a", 1)]}, "operate"
        ),
        (
            aerospike.Client.operate_ordered,
            {"key": KEY, "list": [operations.write("a", 1)]}, "operate"
        ),
        (
            aerospike.Client.increment,
            {"key": KEY, "bin": "c", "offset": 1},
            "operate"
        ),
        (
            aerospike.Client.prepend,
            {"key": KEY, "bin": "a", "val": "a"},
            "operate"
        ),
        (
            aerospike.Client.append,
            {"key": KEY, "bin": "a", "val": "a"},
            "operate"
        ),
    ])
    def test_apis_with_meta_parameter(self, config_ttl_setup, api_method, kwargs: dict, kwargs_with_ttl: dict):
        kwargs |= kwargs_with_ttl
        try:
            api_method(self.client, **kwargs)
        except e.ClientError as exc:
            # ClientError can be raised if the user runs Python with warnings treated as errors.
            assert exc.msg == "meta[\"ttl\"] is deprecated and will be removed in the next client major release"

        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)

    # Don't bother testing for DeprecationWarnings here since running Python with -W error flag can
    # cause ClientError to be raised. It's too complicated to check both cases
    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    @ttl_param
    @pytest.mark.parametrize("policy_name", ["batch_write"])
    def test_batch_write(self, config_ttl_setup, kwargs_with_ttl):
        ops = [
            operations.write("bin", 1)
        ]
        batch_records = BatchRecords([
            Write(KEY, ops=ops, **kwargs_with_ttl)
        ])
        try:
            brs = self.client.batch_write(batch_records)
        except e.ClientError as exc:
            assert exc.msg == "meta[\"ttl\"] is deprecated and will be removed in the next client major release"

        # assert brs.result == 0
        for br in brs.batch_records:
            assert br.result == 0

        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"ttl": None},
            {"ttl": aerospike.TTL_CLIENT_DEFAULT}
        ],
    )
    @pytest.mark.parametrize("policy_name", ["batch_write"])
    def test_apis_with_ttl_parameter(self, config_ttl_setup, kwargs):
        ops = [
            operations.write("bin", 1)
        ]
        keys = [KEY]
        brs = self.client.batch_operate(keys, ops, **kwargs)
        # assert brs.result == 0
        for br in brs.batch_records:
            assert br.result == 0

        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)

    @pytest.mark.parametrize("api_method, kwargs, policy_name", [
        (
            aerospike.Client.apply,
            {"key": KEY},
            "apply",
        ),
        (
            aerospike.Client.batch_apply,
            {"keys": [KEY]},
            "batch_apply",
        ),
    ])
    def test_apis_with_policy_parameter(self, config_ttl_setup, api_method, kwargs):
        # Setup
        self.client.put(KEY, {"bin": "a"})

        kwargs |= {"module": "test_record_udf", "function": "bin_udf_operation_string", "args": ["bin", "a"]}
        api_method(self.client, **kwargs)
        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)

    @pytest.mark.parametrize("policy_name", ["scan"])
    def test_setting_scan_ttl(self, config_ttl_setup):
        # Setup
        self.client.put(KEY, {"bin": "a"})

        # Tell scan to use client config's scan policy ttl
        scan = self.client.scan("test", "demo")
        scan.ttl = aerospike.TTL_CLIENT_DEFAULT
        ops = [
            operations.append("bin", "a")
        ]
        scan.add_ops(ops)
        job_id = scan.execute_background()

        wait_for_job_completion(self.client, job_id, job_module=aerospike.JOB_SCAN)

        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)

    @pytest.mark.parametrize("policy_name", ["write"])
    def test_query_client_default_ttl(self, config_ttl_setup):
        # Setup
        self.client.put(KEY, {"bin": "a"}, policy={"ttl": 90})

        # Tell scan to use client config's write policy ttl
        query = self.client.query("test", "demo")
        query.ttl = aerospike.TTL_CLIENT_DEFAULT
        ops = [
            operations.append("bin", "a")
        ]
        query.add_ops(ops)
        job_id = query.execute_background()

        wait_for_job_completion(self.client, job_id)

        verify_record_ttl(self.client, KEY, expected_ttl=self.NEW_TTL)

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

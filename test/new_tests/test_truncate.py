# -*- coding: utf-8 -*-
import pytest
import sys
import time
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestTruncate(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        self.keys = []
        self.truncated_keys = []
        self.un_truncated_keys = []
        for i in range(45):
            key = ("test", "truncate", i)
            rec = {"field": i}
            as_connection.put(key, rec)
            self.keys.append(key)
            self.truncated_keys.append(key)

        for i in range(45):
            key = ("test", "un_trunc", i)
            rec = {"removed": i}
            as_connection.put(key, rec)
            self.keys.append(key)
            self.un_truncated_keys.append(key)
        # Give some time for time differences to resolve themselves
        time.sleep(1.1)
        self.truncate_threshold = int(time.time()) * 10 ** 9

        def teardown():
            """
            Teardown Method
            """
            for key in self.keys:
                try:
                    as_connection.remove(key)
                except:
                    pass

        request.addfinalizer(teardown)

    def _assert_truncation_status(self, keys, exists=False):
        # Give the server some time to finish the call
        # If a truncation was expected, give more time
        # to finish it

        attempts = 5
        if not exists:
            while attempts > 0:
                attempts = attempts - 1
                truncated = True
                time.sleep(1)
                for key in keys:
                    _, meta = self.as_connection.exists(key)
                    if meta is not None:
                        truncated = False
                if truncated:
                    break
            assert truncated
        else:
            # Wait for a potential truncation command to come in
            time.sleep(1)
            for key in keys:
                _, meta = self.as_connection.exists(key)
                assert meta is not None

    def test_whole_set_truncation(self):
        self.as_connection.truncate("test", "truncate", 0)

        self._assert_truncation_status(self.truncated_keys, exists=False)

    def test_truncate_does_not_change_entire_namespace(self):
        self.as_connection.truncate("test", "truncate", 0)

        self._assert_truncation_status(self.un_truncated_keys, exists=True)

    def test_truncate_entire_namespace(self):
        '''
        Verify that passing None as the set will
        truncate the entire namespace
        '''
        self.as_connection.truncate("test", None, 0)
#
        self._assert_truncation_status(self.truncated_keys, exists=False)
        self._assert_truncation_status(self.un_truncated_keys, exists=False)

    def test_whole_set_truncation_with_policy(self):
        policy = {'timeout': 1000}
        self.as_connection.truncate("test", "truncate", 0, policy)

        self._assert_truncation_status(self.truncated_keys, exists=False)

    def test_whole_set_truncation_with_none_policy(self):
        policy = None
        self.as_connection.truncate("test", "truncate", 0, policy)

        self._assert_truncation_status(self.truncated_keys, exists=False)

    def test_whole_set_truncation_with_created_threshold(self):
        time.sleep(5)
        threshold = int(time.time()) * 10 ** 9
        time.sleep(5)

        for i in range(45, 90):
            key = ("test", "truncate", i)
            rec = {"field": i}
            self.as_connection.put(key, rec)
            self.keys.append(key)

        self.as_connection.truncate("test", "truncate", threshold)

        self._assert_truncation_status(self.truncated_keys[:45], exists=False)

        # Items created after the LUT should still exist
        self._assert_truncation_status(self.truncated_keys[45:], exists=True)

    def test_truncate_with_lut_before_all_records(self):
        before_lut = self.truncate_threshold - 10 ** 11
        self.as_connection.truncate("test", "truncate", before_lut)

        self._assert_truncation_status(self.truncated_keys, exists=True)

    @pytest.mark.parametrize(
        "namespace, test_set",
        (
            (u'test', 'truncate'),
            ('test', u'truncate'),
            (u'test', u'truncate')
        )
    )
    def test_whole_set_unicode_truncation(self, namespace, test_set):
        self.as_connection.truncate(namespace, test_set, 0)

        self._assert_truncation_status(self.truncated_keys, exists=False)

    @pytest.mark.parametrize(
        "fake_namespace, fake_set",
        (
            ("fake_namespace", "truncate"),
            ("test", "fake_set")
        )
    )
    def test_truncate_non_existent_containers(self, fake_namespace, fake_set):
        ret_code = self.as_connection.truncate(fake_namespace, fake_set, 0)
        assert ret_code == 0

    @pytest.mark.parametrize(
        "invalid_ns",
        (
            1, .5, None, False, {}, (), []
        )
    )
    def test_invalid_ns_argument_to_truncate(self, invalid_ns):
        with pytest.raises(e.ParamError):
            self.as_connection.truncate(invalid_ns, "truncate", 0)

    @pytest.mark.parametrize(
        "invalid_set",
        (
            1, .5, False, {}, (), []
        )
    )
    def test_invalid_set_argument_to_truncate(self, invalid_set):
        with pytest.raises(e.ParamError):
            self.as_connection.truncate("test", invalid_set, 0)

    @pytest.mark.parametrize(
        "invalid_nanos",
        (
            "zero", .5, None, {}, (), []
        )
    )
    def test_invalid_nanos_argument_to_truncate(self, invalid_nanos):
        with pytest.raises(e.ParamError):
            self.as_connection.truncate("test", "truncate", invalid_nanos)

    @pytest.mark.parametrize(
        "fake_namespace, fake_set",
        (
            ("", "truncate"),
            ("test", "")
        )
    )
    def test_truncate_empty_container_names(self, fake_namespace, fake_set):
        with pytest.raises(e.ClientError):
            ret_code = self.as_connection.truncate(fake_namespace, fake_set, 0)
        self._assert_truncation_status(self.truncated_keys, exists=True)

    def test_nanos_argument_before_cf_epoch(self):
        with pytest.raises(e.ClientError):
            self.as_connection.truncate("test", "truncate", 1)

    def test_nanos_argument_too_large(self):
        with pytest.raises(e.ParamError):
            self.as_connection.truncate("test", "truncate", 2 ** 64)

    def test_nanos_argument_between_int64_and_uint64(self):
        # This may stop raising a client error in 2264
        # as the value will no longer be in the future then
        with pytest.raises(e.ClientError) as ex:
            self.as_connection.truncate("test", "truncate", 2 ** 63 + 1)

    def test_nanos_argument_between_int32_and_uint32(self):
        with pytest.raises(e.ClientError) as ex:
            self.as_connection.truncate("test", "truncate", 2 ** 31 + 1)

    def test_nanos_argument_negative(self):
        with pytest.raises(e.ParamError):
            self.as_connection.truncate(
                "test", "truncate", -self.truncate_threshold)

    def test_nanos_argument_in_future(self):
        future_time = self.truncate_threshold + 10 ** 12  # 1000 seconds more
        with pytest.raises(e.ClientError):
            self.as_connection.truncate(
                "test", "truncate", future_time)

    def test_no_nanos_arg(self):
        with pytest.raises(TypeError):
            self.as_connection.truncate("test", "truncate")

    def test_only_set_arg(self):
        with pytest.raises(TypeError):
            self.as_connection.truncate("test")

    def test_truncate_with_no_args(self):
        with pytest.raises(TypeError):
            self.as_connection.truncate()

    def test_whole_set_truncation_with_invalid_policy(self):
        policy = {'timeout': 0.5}
        with pytest.raises(e.ClientError):
            self.as_connection.truncate("test", "truncate", 0, policy)

    @pytest.mark.parametrize(
        "policy",
        (
            "timeout",
            5,
            False,
            [],
            ()
        )
    )
    def test_whole_set_truncation_with_invalid_policy_type(self, policy):
        with pytest.raises(e.ClientError):
            self.as_connection.truncate("test", "truncate", 0, policy)

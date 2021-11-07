# -*- coding: utf-8 -*-

import pytest
import sys
import asyncio
import time

try:
    import cPickle as pickle
except:
    import pickle
from . import test_data

# from collections import OrderedDict
from .test_base_class import TestBaseClass
aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike import exception as e
    from aerospike_helpers.awaitable import io
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class SomeClass(object):
    pass

aerospike.init_async()

@pytest.mark.usefixtures("as_connection")
class TestPut():

    @pytest.mark.asyncio
    async def test_pos_put_with_policy_exists_create_or_replace(self):
        """
            Invoke put() for a record with create or replace policy positive
            If record exists and will be replaced
        """
        key = ('test', 'demo', 1)

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'total_timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        async def async_io(key=None, rec=None, meta=None, policy=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'total_timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
        assert 0 == self.as_connection.put(key, rec, meta, policy)

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins
        self.as_connection.remove(key)

    @pytest.mark.asyncio
    async def test_pos_put_with_policy_exists_ignore_create(self):
        """
            Invoke put() for a record with ignore.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'total_timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_IGNORE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }

        async def async_io(key=None, rec=None, meta=None, policy=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins
        self.as_connection.remove(key)

    @pytest.mark.asyncio
    async def test_pos_put_with_policy_exists_ignore_update(self):
        """
            Invoke put() for a record with ignore.
            Ignore for an existing records should update without any error
        """
        key = ('test', 'demo', 1)

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'total_timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_IGNORE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }
    
        async def async_io(key=None, rec=None, meta=None, policy=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins

        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'total_timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_IGNORE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }

        async def async_io(key=None, rec=None, meta=None, policy=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins
        self.as_connection.remove(key)

    @pytest.mark.asyncio
    async def test_pos_put_with_policy_replace(self):
        """
            Invoke put() for a record with replace positive.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'total_timeout': 1000,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
        }

        async def async_io(key=None, rec=None, meta=None, policy=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)

        assert rec == bins

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'total_timeout': 1000, 'exists': aerospike.POLICY_EXISTS_REPLACE}

        async def async_io(key=None, rec=None, meta=None, policy=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)

        assert rec == bins
        self.as_connection.remove(key)

    @pytest.mark.asyncio
    async def test_pos_put_with_policy_exists_update_positive(self):
        """
            Invoke put() for a record with all policies.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'total_timeout': 1000,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
        }

        async def async_io(key=None, rec=None, meta=None, policy=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)

        assert rec == bins

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'total_timeout': 1000, 'exists': aerospike.POLICY_EXISTS_UPDATE}
    
        async def async_io(key=None, rec=None, meta=None, policy=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)

        assert rec == bins
        self.as_connection.remove(key)

    @pytest.mark.asyncio
    async def test_pos_put_with_policy_gen_GT(self):
        """
            Invoke put() for a record with generation as GT positive
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {}

        async def async_io(key=None, rec=None, meta=None, policy=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)

        assert rec == bins
        gen = meta['gen']
        assert gen == 1
        rec = {"name": "Smith"}
        policy = {'gen': aerospike.POLICY_GEN_GT}
        meta = {'gen': gen + 5}

        async def async_io(key=None, rec=None, meta=None, policy=None):
            await io.put(self.as_connection, key, rec, meta, policy)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins
        self.as_connection.remove(key)

    @pytest.mark.asyncio
    async def test_pos_put_with_policy_gen_ignore(self):
        """
            Invoke put() for a record with generation as gen_ignore
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {}

        async def async_io(key=None, rec=None, meta=None, policy=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)

        assert rec == bins

        gen = meta['gen']
        rec = {"name": "Smith"}
        policy = {'gen': aerospike.POLICY_GEN_IGNORE}
        meta = {'gen': gen}

        async def async_io(key=None, rec=None, meta=None, policy=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)
        assert rec == bins
        self.as_connection.remove(key)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("key, record, meta, policy", [
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': True, 'ttl': 25000}, {'timeout': 1000}),
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': 3, 'ttl': True}, {'timeout': 1000}),
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': True, 'ttl': True}, {'timeout': 1000}),
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': True, 'ttl': aerospike.TTL_NAMESPACE_DEFAULT}, {'timeout': 1000}),
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': True, 'ttl': aerospike.TTL_NEVER_EXPIRE}, {'timeout': 1000}),
    ])
    async def test_pos_put_with_metadata_bool(
            self, key, record, meta, policy, put_data):
        """
            Invoke put() for a record with generation as boolean.
        """
        put_data(self.as_connection, key, record, meta, policy)

        async def async_io(key=None, policy=None):
            (key, meta, bins) = await io.get(self.as_connection, key)
            assert bins == record
            assert meta['gen'] is not None
            assert meta['ttl'] is not None
        await asyncio.gather(async_io(key))

    @pytest.mark.asyncio
    async def test_pos_put_user_serializer_no_deserializer(self):
        """
            Invoke put() for float data record with user serializer is
            registered, but deserializer is not registered.
        """
        key = ('test', 'demo', 'put_user_serializer')

        rec = {"pi": 3.14}

        def serialize_function(val):
            return pickle.dumps(val)

        aerospike.set_serializer(serialize_function)

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, rec, {}, {}, aerospike.SERIALIZER_USER))

        _, _, bins = self.as_connection.get(key)

        if self.skip_old_server is False:
            assert bins == {'pi': 3.14}
        else:
            assert bins == {'pi': bytearray(b'F3.1400000000000001\n.')}

        self.as_connection.remove(key)

    # put negative
    @pytest.mark.asyncio
    async def test_neg_put_with_no_parameters(self):
        """
            Invoke put() without any parameters.
        """

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            with pytest.raises(e.ParamError) as paramError:
                await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io())

    @pytest.mark.asyncio
    async def test_neg_put_without_record(self):
        """
            Invoke put() without any record data.
        """
        key = ('test', 'demo', 1)

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            with pytest.raises(e.ParamError) as paramError:
                await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("key, ex_code, ex_msg", test_data.key_neg)
    async def test_neg_put_with_none(self, key, ex_code, ex_msg, record={}):
        """
            Invoke put() with invalid data
        """

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            with pytest.raises(e.ParamError) as paramError:
                await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, record))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("key, ex_code, ex_msg, record", [
        (("test", "demo", None),
         -2, "either key or digest is required", {"name": "John"}),
        ])
    async def test_neg_put_with_invalid_record(self, key, ex_code, ex_msg, record):
        """
            Invoke put() with invalid data
        """

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            with pytest.raises(e.ParamError) as paramError:
                await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, record))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("key, record, exception_code", [
        # Non-existing NS & Set
        (('demo', 'test', 1), {
         'a': ['!@#!#$%#', bytearray('ASD@#$AR#$@#ERQ#', 'utf-8')]}, 20),
        # Non-existing Namespace
        (('test1', 'demo', 1), {'i': 'asdadasd'}, 20),

        ])
    async def test_neg_put_with_wrong_ns_and_set(self, key, record, exception_code):
        """
            Invoke put() with non-existent data
        """

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            with pytest.raises(e.ClientError) as clientError:
                await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, record))

    @pytest.mark.asyncio
    async def test_neg_put_with_policy_gen_EQ_less(self):
        """
            Invoke put() for a record with generation as EQ less
        """
        key = ('test', 'demo', 'policy_gen_EQ_key')

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {}
    
        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)

        assert {"name": "John"} == bins
        rec = {"name": "Smith"}
        policy = {'gen': aerospike.POLICY_GEN_EQ}
        meta = {'gen': 10}
    
        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            with pytest.raises(e.RecordGenerationError):
                await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)
        assert {"name": "John"} == bins

        self.as_connection.remove(key)

    @pytest.mark.asyncio
    async def test_neg_put_with_policy_gen_EQ_more(self):
        """
            Invoke put() for a record with generation as EQ less
        """
        key = ('test', 'demo', 'policy_gen_EQ_more_key')

        rec = {"name": "John"}
        meta = {'gen': 10, 'ttl': 25000}
        policy = {'timeout': 1000}

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)

        assert {"name": "John"} == bins
        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'gen': aerospike.POLICY_GEN_EQ}
        meta = {'gen': 4}

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            with pytest.raises(e.RecordGenerationError):
                await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)
        assert {"name": "John"} == bins

        self.as_connection.remove(key)

    @pytest.mark.asyncio
    async def test_neg_put_with_policy_exists_create(self):
        """
            Invoke put() for a record with all policies.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'total_timeout': 1000,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND,
        }

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)

        assert {"name": "John"} == bins

        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'exists': aerospike.POLICY_EXISTS_CREATE}
        meta = {'gen': 2}

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            with pytest.raises(e.RecordExistsError):
                await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)
        assert {"name": "John"} == bins

        self.as_connection.remove(key)

    @pytest.mark.asyncio
    async def test_neg_put_with_policy_exists_replace_negative(self):
        """
            Invoke put() for a record with replace policy negative.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'timeout': 1000,
            'exists': aerospike.POLICY_EXISTS_REPLACE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            with pytest.raises(e.RecordNotFound):
                assert 0 == await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, rec, meta, policy))

    @pytest.mark.asyncio
    async def test_neg_put_with_policy_replace(self):
        """
            Invoke put() for a record with replace positive.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "Smith"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000, 'exists': aerospike.POLICY_EXISTS_REPLACE}

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            with pytest.raises(e.RecordNotFound):
                await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, rec, meta, policy))

    @pytest.mark.asyncio
    async def test_neg_put_with_policy_exists_update_negative(self):
        """
            Invoke put() for a record with update policy negative.
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {
            'exists': aerospike.POLICY_EXISTS_UPDATE,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'retry': aerospike.POLICY_RETRY_ONCE,
            'key': aerospike.POLICY_KEY_SEND
        }

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            try:
                assert 0 == await io.put(self.as_connection, key, rec, meta, policy, serialize)
            except e.RecordNotFound as exception:
                assert exception.code == 2
                assert exception.msg == 'AEROSPIKE_ERR_RECORD_NOT_FOUND'
        await asyncio.gather(async_io(key, rec, meta, policy))

    @pytest.mark.asyncio
    async def test_neg_put_with_policy_gen_GT_lesser(self):
        """
            Invoke put() for a record with generation as GT lesser
        """
        key = ('test', 'demo', 1)

        rec = {"name": "John"}
        meta = {'gen': 2, 'ttl': 25000}
        policy = {'timeout': 1000}

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            assert 0 == await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)

        assert {"name": "John"} == bins
        gen = meta['gen']
        rec = {"name": "Smith"}
        policy = {'timeout': 1000, 'gen': aerospike.POLICY_GEN_GT}
        meta = {'gen': gen}

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            try:
                assert 0 == await io.put(self.as_connection, key, rec, meta, policy, serialize)
            except e.RecordGenerationError as exception:
                assert exception.code == 3
                assert exception.msg == 'AEROSPIKE_ERR_RECORD_GENERATION'
        await asyncio.gather(async_io(key, rec, meta, policy))

        (key, meta, bins) = self.as_connection.get(key)
        assert {"name": "John"} == bins
        self.as_connection.remove(key)

    @pytest.mark.asyncio
    async def test_neg_put_with_string_record_without_connection(self):
        """
            Invoke put() for a record with string data without connection
        """
        config = {"hosts": [("127.0.0.1", 3000)]}
        client1 = aerospike.client(config)

        key = ('test', 'demo', 1)

        bins = {"name": "John"}

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            try:
                assert 0 == await io.put(client1, key, rec, meta, policy, serialize)
            except e.ClusterError as exception:
                assert exception.code == 11
        await asyncio.gather(async_io(key, bins))

    @pytest.mark.parametrize("key, record, meta, policy, ex_code, ex_msg", [
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': "wrong", 'ttl': 25000}, {'total_timeout': 1000},  # Gen as string
            -2, "Generation should be an int or long"),
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': 3, 'ttl': "25000"}, {'total_timeout': 1000},      # ttl as string
            -2, "TTL should be an int or long"),
        (('test', 'demo', 1), {'name': 'john'},
            {'gen': 3, 'ttl': 25000}, {'total_timeout': "1000"},      # Timeout as string
            -2, "timeout is invalid"),
        (('test', 'demo', 1), {'name': 'john'},  # Policy as string
            {'gen': 3, 'ttl': 25000}, "Policy",
            -2, "policy must be a dict"),
        (('test', 'demo', 1), {'i': 13},  # Meta as string
            "OK", {'total_timeout': 1000},
            -2, "meta must be a dict"),
        (('test', 'demo', 1), {'i': 13},  # Meta as string
            1234, {'total_timeout': 1000},
            -2, "meta must be a dict"),
    ])
    def test_neg_put_with_invalid_metadata(
            self, key, record, meta, policy, ex_code, ex_msg, put_data):
        """
            Invoke put() for a record with generation as string
        """
        with pytest.raises(e.ParamError):
            put_data(self.as_connection, key, record, meta, policy)
        #     # self.as_connection.remove(key)
        # except e.ParamError as exception:
        #     assert exception.code == ex_code
        #     assert exception.msg == ex_msg

    # put edge cases
    @pytest.mark.asyncio
    async def test_edge_put_record_with_bin_name_exceeding_max_limit(self):
        """
            Invoke put() with bin name exceeding the max limit of bin name.
        """
        key = ('test', 'demo', 'put_rec')
        put_record = {
            'a' * 50: "unimportant"
        }

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            with pytest.raises(e.BinNameError):
                await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, put_record))

    @pytest.mark.asyncio
    async def test_edge_put_with_integer_greater_than_maxisze(self):
        """
            Invoke put() for a record with integer greater than max size
        """
        key = ('test', 'demo', 1)

        bins = {"no": 111111111111111111111111111111111111111111111}

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            try:
                assert 0 == await io.put(self.as_connection, key, rec, meta, policy, serialize)
            except e.ParamError as exception:
                assert exception.code == -2
                assert exception.msg == 'integer value exceeds sys.maxsize'
            except SystemError as exception:
                pass
        await asyncio.gather(async_io(key, bins))

    @pytest.mark.asyncio
    async def test_edge_put_with_key_as_an_integer_greater_than_maxsize(self):
        """
            Invoke put() for a record with integer greater than max size
        """
        key = ('test', 'demo', 1111111111111111111111111111111111)

        bins = {"no": 11}

        async def async_io(key=None, rec=None, meta=None, policy=None, serialize=None):
            with pytest.raises(e.ParamError):
                assert 0 == await io.put(self.as_connection, key, rec, meta, policy, serialize)
        await asyncio.gather(async_io(key, bins))

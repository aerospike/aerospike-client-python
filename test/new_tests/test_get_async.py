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
class TestGet():

    @pytest.mark.parametrize("_input, _expected", test_data.pos_data)
    @pytest.mark.asyncio
    async def test_pos_get_put_with_key(self, _input, _expected, put_data):
        """
            Invoke get() with a key and not policy's dict.
        """
        put_data(self.as_connection, _input, _expected)

        @pytest.mark.asyncio
        async def async_io(_input):
            _,_,bin = await io.get(self.as_connection, _input)
            assert bin == _expected
        await asyncio.gather(async_io(_input))

    @pytest.mark.asyncio
    async def test_pos_get_with_data_missing(self):
        """
            Invoke get with a record which does not exist.
        """
        key = ('test', 'demo', 'non-existent-key-that-does-not-exist')
        @pytest.mark.asyncio
        async def async_io(io_input):
            with pytest.raises(e.RecordNotFound):
                await io.get(self.as_connection, io_input)
        await asyncio.gather(async_io(key))

    @pytest.mark.skip(reason="byte key not currently handled")
    @pytest.mark.asyncio
    async def test_get_information_using_bytes_key(self):
        record = {'bytes': 'are_cool'}
        key = ('test', 'demo', b'\x83X\xb5\xde')
        self.as_connection.put(key, record)
        @pytest.mark.asyncio
        async def async_io(io_input):
            _,_,rec_bin = await io.get(self.as_connection, io_input)
            assert rec_bin == record
        await asyncio.gather(async_io(key))
        self.as_connection.remove(key)

    @pytest.mark.asyncio
    async def test_pos_get_initkey_with_digest(self, put_data):
        """
            Invoke get() for a record having bytestring data.
        """
        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        rec = {'name': 'john'}

        policy = {'key': aerospike.POLICY_KEY_DIGEST}

        put_data(self.as_connection, key, rec, policy)

        async def async_io(key_input, policy_input=None):
            key,_,bins = await io.get(self.as_connection, key_input, policy_input)
            assert bins == {'name': 'john'}
            assert key == ('test', 'demo', None,
                        bytearray(b"asd;as[d\'as;djk;uyfl"))
        await asyncio.gather(async_io(key, policy))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("_input, _record, _policy, _expected", [
        (('test', 'demo', 3),
            {'name': 'name%s' % (str(3)), 'age': 3},
            aerospike.POLICY_KEY_DIGEST,
            None),
        (('test', 'demo', 3),
            {'name': 'name%s' % (str(3)), 'age': 3},
            aerospike.POLICY_KEY_SEND,
            3),
    ])
    async def test_pos_get_with_policy_key_digest(
            self, _input, _record, _policy, _expected, put_data):
        """
            Invoke get() for a record with POLICY_KEY_DIGEST
        """
        put_data(self.as_connection, _input, _record)

        async def async_io(key_input, policy_input=None):
            key, _, _ = await io.get(self.as_connection, key_input, policy_input)
            assert key[2] == _expected
        
        policy={'key': _policy}
        await asyncio.gather(async_io(_input, policy))

    @pytest.mark.asyncio
    async def test_pos_get_initkey_with_policy_send(self, put_data):
        """
            Invoke get() for a record having string data.
        """

        key = ('test', 'demo', 1)

        rec = {'name': 'john', 'age': 1}

        policy = {
            'key': aerospike.POLICY_KEY_SEND
        }

        put_data(self.as_connection, key, rec, policy)

        async def async_io(key_input, policy_input=None):
            key, _, bins = await io.get(self.as_connection, key_input, policy_input)
            assert bins == {'name': 'john', 'age': 1}
            assert key == ('test', 'demo', 1, bytearray(
                b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
            )
        await asyncio.gather(async_io(key, policy))

    # Negative get tests
    @pytest.mark.asyncio
    async def test_neg_get_with_no_parameter(self):
        """
            Invoke get() without any mandatory parameters.
        """
        async def async_io(key_input=None, policy_input=None):
            with pytest.raises(e.ParamError) as paramError:
                await io.get(self.as_connection)
        await asyncio.gather(async_io())

    def test_neg_get_with_extra_parameter_in_key(self, put_data):
        """
            Invoke get() with extra parameter
        """
        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"), 1)
        rec = {'name': 'john'}

        policy = {'key': aerospike.POLICY_KEY_DIGEST}
        with pytest.raises(e.ParamError):
            put_data(self.as_connection, key, rec, policy)

    @pytest.mark.asyncio
    async def test_neg_get_with_key_digest(self):
        """
            Invoke get() with a key digest.
        """
        key = ('test', 'demo', 1)
        async def async_io(key_input=None, policy_input=None):
            try:
                key, _ = self.as_connection.exists(key_input)
                key, _, _ = await io.get(self.as_connection, (key[0], key[1], None, key[2]))
            except e.ParamError as exception:
                assert exception.code == -2
                assert exception.msg == 'digest is invalid. expected a bytearray'
            except e.RecordNotFound as exception:
                assert exception.code == 2
        await asyncio.gather(async_io((key[0], key[1], None, key[2])))

    @pytest.mark.parametrize("key, ex_code, ex_msg", test_data.key_neg)
    @pytest.mark.asyncio
    async def test_neg_get_with_none(self, key, ex_code, ex_msg):
        """
            Invoke get() with None namespace/key in key tuple.
        """
        async def async_io(key_input=None, policy_input=None):
            with pytest.raises(e.ParamError):
                await io.get(self.as_connection, key_input)
        await asyncio.gather(async_io(key))

    @pytest.mark.asyncio
    async def test_neg_get_with_invalid_record(self):
        """
            Get a record which does not exist
        """
        key = ('test', 'demo', 'A PRIMARY KEY WHICH DOES NOT EXIST')
        async def async_io(key_input=None, policy_input=None):
            with pytest.raises(e.RecordNotFound):
                await io.get(self.as_connection, key_input)
        await asyncio.gather(async_io(key))

    @pytest.mark.asyncio
    async def test_neg_get_with_non_existent_namespace(self):
        """
            Invoke get() for non-existent namespace.
        """
        key = ('namespace', 'demo', 1)

        async def async_io(key_input=None, policy_input=None):
            with pytest.raises(e.ClientError):
                await io.get(self.as_connection, key_input)
        await asyncio.gather(async_io(key))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("_input, _expected", [
        (('test', 'demo', 1), {'name': 'john', 'age': 1}),
    ])
    async def test_neg_get_remove_key_and_check_get(
            self, _input, _expected, put_data):
        """
            Invoke get() for a record having string data.
        """
        put_data(self.as_connection, _input, _expected)
        self.as_connection.remove(_input)
        async def async_io(key_input=None, policy_input=None):
            try:
                rec = await io.get(self.as_connection, key_input)
                assert rec is None
            except e.RecordNotFound as exception:
                assert exception.code == 2
        await asyncio.gather(async_io(_input))

    @pytest.mark.asyncio
    async def test_neg_get_with_only_key_no_connection(self):
        """
            Invoke get() with a key and not policy's dict no connection
        """
        key = ('test', 'demo', 1)
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        async def async_io(key_input=None, policy_input=None):
            with pytest.raises(e.ClusterError):
                key, _, _ = await io.get(client1, key_input)
        await asyncio.gather(async_io(key))

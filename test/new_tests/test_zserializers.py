# -*- coding: utf-8 -*-

from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus
from aerospike import exception as e

import pytest
import sys
import marshal
import json

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

class SomeClass(object):
    pass


def class_serializer(obj):
    return 'class serialized'


def class_deserializer(obj):
    return (obj, 'class deserialized')


def instance_serializer(obj):
    return 'instance serialized'


def instance_deserializer(obj):
    return (obj, 'instance deserialized')


def serializer_error(obj):
    raise Exception("serialization Failure")


def deserializer_error(obj):
    raise Exception("deserialization Failure")


def serializer_no_arg():
    return ''


def serializer_two_arg(arg1, arg2):
    return ''


class TestPythonSerializer(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        '''
        Remove the test key if it exists each time through.
        Unset any set serializers
        '''
        aerospike.unset_serializers()
        # Try to remove the key if it exists

        def teardown():
            try:
                as_connection.remove(self.test_key)
            except:
                pass
            aerospike.unset_serializers()
        request.addfinalizer(teardown)

    def setup_class(cls):
        """
            Setup class
        """
        aerospike.unset_serializers()
        cls.mixed_record = {'normal': 1234, 'tuple': (1, 2, 3)}
        cls.test_key = ('test', 'demo', 'TestPythonSerializer')

    def test_instance_serializer_and_no_class_serializer(self):
        """
        Invoke put() for record with no class serializer. There is an
        instance serializer
        """
        method_config = {
            'serialization': (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        response = client.put(
            self.test_key, self.mixed_record,
            serializer=aerospike.SERIALIZER_USER)
        assert response == 0

        _, _, bins = client.get(self.test_key)

        assert bins == {'normal': 1234,
                        'tuple': ('instance serialized',
                                  'instance deserialized')}
        client.close()

    def test_put_with_no_serializer_arg_and_instance_serializer_set(self):
        """
        Test that when no serializer arg is passed in,
        and an instance serializer has been set,
        and a class serializer has not been set,
        the instance serializer is used.
        """
        method_config = {
            'serialization': (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        response = client.put(
            self.test_key, self.mixed_record)

        _, _, bins = client.get(self.test_key)

        assert bins == {'normal': 1234,
                        'tuple': (
                            'instance serialized', 'instance deserialized'
                        )
                        }
        client.close()

    def test_put_with_no_serializer_arg_and_class_serializer_set(self):
        '''
        Test that when no serializer arg is passed in,
        and no instance serializer has been set,
        and a class serializer has been set,
        the default language serializer is used.
        '''
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)

        response = self.as_connection.put(
            self.test_key, self.mixed_record)

        _, _, bins = self.as_connection.get(self.test_key)

        # The class serializer would have altered our record, so we
        # We check that the data is the same as what we stored
        assert bins == {'normal': 1234,
                        'tuple': (1, 2, 3)}

    def test_builtin_with_class_serializer(self):
        """
        Invoke put() for mixed data record with builtin serializer
        """
        method_config = {
            'serialization': (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        response = client.put(
            self.test_key, self.mixed_record,
            serializer=aerospike.SERIALIZER_PYTHON)

        _, _, bins = client.get(self.test_key)

        assert bins == {'normal': 1234, 'tuple': (1, 2, 3)}
        client.close()

    def test_builtin_with_class_serializer(self):
        """
        Invoke put() for mixed data record with builtin serializer
        """
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)

        response = self.as_connection.put(
            self.test_key, self.mixed_record,
            serializer=aerospike.SERIALIZER_PYTHON)

        assert response == 0

        _, _, bins = self.as_connection.get(self.test_key)

        # The class serializer would have altered our record, so we
        # We check that the data is the same as what we stored
        assert bins == {'normal': 1234, 'tuple': (1, 2, 3)}

    def test_with_class_serializer(self):
        """
        Invoke put() for mixed data record with class serializer.
        It should get called when serializer is passed
        """
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)

        rec = {'normal': 1234, 'tuple': (1, 2, 3)}
        response = self.as_connection.put(
            self.test_key, self.mixed_record,
            serializer=aerospike.SERIALIZER_USER)

        assert response == 0

        _, _, bins = self.as_connection.get(self.test_key)

        assert bins == {'normal': 1234,
                        'tuple': (
                            'class serialized', 'class deserialized'
                        )
                        }

    def test_builtin_with_class_serializer_and_instance_serializer(self):
        """
        Invoke put() passing SERIALIZER_PYTHON and verify that it
        causes the language serializer to override instance and class
        serializers
        """
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)
        method_config = {
            'serialization': (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        rec = {'normal': 1234, 'tuple': (1, 2, 3)}
        response = client.put(
            self.test_key, self.mixed_record,
            serializer=aerospike.SERIALIZER_PYTHON)

        assert response == 0

        _, _, bins = client.get(self.test_key)

        # The instance and class serializers would have mutated this,
        # So getting back what we put in means the language serializer ran
        assert bins == {'normal': 1234, 'tuple': (1, 2, 3)}
        client.close()

    def test_with_class_serializer_and_instance_serializer(self):
        """
        Invoke put() for mixed data record with class and instance serializer.
        Verify that the instance serializer takes precedence
        """
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)
        method_config = {
            'serialization': (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        response = client.put(
            self.test_key, self.mixed_record,
            serializer=aerospike.SERIALIZER_USER)

        _, _, bins = client.get(self.test_key)

        assert bins == {'normal': 1234,
                        'tuple': (
                            'instance serialized', 'instance deserialized'
                        )
                        }
        client.close()

    def test_with_unset_serializer_python_serializer(self):
        """
        Invoke put() for mixed data record with python serializer and
        calling unset_serializers
        """
        aerospike.set_serializer(json.dumps)
        aerospike.set_deserializer(json.loads)
        method_config = {
        }
        client = TestBaseClass.get_new_connection(method_config)

        aerospike.unset_serializers()
        response = client.put(
            self.test_key, self.mixed_record,
            serializer=aerospike.SERIALIZER_PYTHON)

        _, _, bins = client.get(self.test_key)

        assert bins == {'normal': 1234, 'tuple': (1, 2, 3)}
        client.close()

    def test_unset_instance_serializer(self):
        """
        Verify that calling unset serializers does not remove an instance level
        serializer
        """

        method_config = {
            'serialization': (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        aerospike.unset_serializers()
        response = client.put(
            self.test_key, self.mixed_record,
            serializer=aerospike.SERIALIZER_USER)

        _, _, bins = client.get(self.test_key)

        # tuples JSON-encode to a list, and we use this fact to check which
        # serializer ran:
        assert bins == {'normal': 1234,
                        'tuple': (
                            'instance serialized', 'instance deserialized'
                        )
                        }
        client.close()

    def test_setting_serializer_is_a_per_rec_setting(self):
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)

        self.as_connection.put(
            self.test_key, self.mixed_record,
            serializer=aerospike.SERIALIZER_USER)

        self.as_connection.put(
            ('test', 'demo', 'test_record_2'), self.mixed_record)

        _, _, record = self.as_connection.get(
            ('test', 'demo', 'test_record_2'))

        self.as_connection.remove(('test', 'demo', 'test_record_2'))
        # this should not have been serialized with the class serializer
        assert record == self.mixed_record

    def test_unsetting_serializers_after_a_record_put(self):
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)

        self.as_connection.put(
            self.test_key, self.mixed_record,
            serializer=aerospike.SERIALIZER_USER)

        aerospike.unset_serializers()

        _, _, record = self.as_connection.get(self.test_key)
        # this should not have been deserialized with the class serializer
        # it will have been deserialized by the class deserializer,
        # so this should be a deserialization of 'class serialized'
        assert record['tuple'] != ('class serialized', 'class deserialized')

    def test_changing_deserializer_after_a_record_put(self):
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)

        self.as_connection.put(
            self.test_key, self.mixed_record,
            serializer=aerospike.SERIALIZER_USER)

        aerospike.set_deserializer(instance_deserializer)

        _, _, record = self.as_connection.get(self.test_key)
        # this should not have been deserialized with the class serializer
        # it should now have used the instance deserializer
        assert record['tuple'] == ('class serialized', 'instance deserialized')

    def test_only_setting_a_serializer(self):
        aerospike.set_serializer(class_serializer)

        self.as_connection.put(
            self.test_key, self.mixed_record,
            serializer=aerospike.SERIALIZER_USER)

        _, _, record = self.as_connection.get(self.test_key)
        # this should not have been deserialized with the class serializer
        # it should now have used the language default serializer
        assert record['tuple'] != ('class serialized', 'class deserialized')

    def test_only_setting_a_deserializer(self):
        # This should raise an error
        aerospike.set_deserializer(class_deserializer)

        with pytest.raises(e.ClientError):
            self.as_connection.put(
                self.test_key, self.mixed_record,
                serializer=aerospike.SERIALIZER_USER)

    def test_class_serializer_unset(self):
        """
        Verify that calling unset_serializers actually removes
        the class serializer
        """
        aerospike.set_serializer(json.dumps)
        aerospike.set_deserializer(json.loads)
        method_config = {
        }
        client = TestBaseClass.get_new_connection(method_config)

        aerospike.unset_serializers()
        with pytest.raises(e.ClientError) as err_info:
            client.put(
                self.test_key, self.mixed_record,
                serializer=aerospike.SERIALIZER_USER)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_class_serializer_non_callable(self):
        """
        Verify that calling unset_serializers actually removes
        the class serializer
        """
        with pytest.raises(e.ParamError):
            aerospike.set_serializer(5)

    def test_class_deserializer_non_callable(self):
        """
        Verify that calling unset_serializers actually removes
        the class serializer
        """
        with pytest.raises(e.ParamError):
            aerospike.set_deserializer(5)

    def test_instance_serializer_non_callable(self):
        """
        Verify that calling unset_serializers actually removes
        the class serializer
        """

        method_config = {
            'serialization': (5, instance_deserializer)}

        with pytest.raises(e.ParamError):
            client = aerospike.client(method_config)

    def test_instance_deserializer_non_callable(self):
        """
        Verify that calling unset_serializers actually removes
        the class serializer
        """
        with pytest.raises(e.ParamError):
            aerospike.set_deserializer(5)

        method_config = {
            'serialization': (instance_serializer, 5)}

        with pytest.raises(e.ParamError):
            client = aerospike.client(method_config)

    def test_serializer_with_no_args(self):
        aerospike.set_serializer(serializer_no_arg)
        aerospike.set_deserializer(class_deserializer)

        with pytest.raises(e.ClientError):
                self.as_connection.put(
                    self.test_key, self.mixed_record,
                    serializer=aerospike.SERIALIZER_USER)

    def test_serializer_with_two_args(self):
        aerospike.set_serializer(serializer_two_arg)
        aerospike.set_deserializer(class_deserializer)

        with pytest.raises(e.ClientError):
                self.as_connection.put(
                    self.test_key, self.mixed_record,
                    serializer=aerospike.SERIALIZER_USER)

    def test_put_with_invalid_serializer_constant(self):

        with pytest.raises(e.ClientError):
                self.as_connection.put(
                    self.test_key, self.mixed_record,
                    serializer=-15000)

    def test_put_with_invalid_serializer_arg_type(self):

        self.as_connection.put(
            self.test_key, self.mixed_record,
            serializer="")

    def test_serializer_raises_error(self):

        aerospike.set_serializer(serializer_error)
        aerospike.set_deserializer(class_deserializer)

        with pytest.raises(e.ClientError):
                self.as_connection.put(
                    self.test_key, self.mixed_record,
                    serializer=aerospike.SERIALIZER_USER)

    def test_deserializer_raises_error(self):
        # If the deserializer failed, we should get a bytearray
        # representation of the item
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(deserializer_error)

        self.as_connection.put(
            self.test_key, self.mixed_record,
            serializer=aerospike.SERIALIZER_USER)

        _, _, response = self.as_connection.get(self.test_key)

        assert response['normal'] == 1234
        assert isinstance(response['tuple'], bytearray)

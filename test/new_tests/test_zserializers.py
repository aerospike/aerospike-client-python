# -*- coding: utf-8 -*-

from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus
from aerospike import exception as e
from aerospike_helpers.operations import operations
from aerospike_helpers.expressions import base as expr

import pytest
import json

import aerospike


class SomeClass(object):
    pass


def class_serializer(obj):
    return "class serialized"


def class_deserializer(obj):
    return (obj, "class deserialized")


def instance_serializer(obj):
    return "instance serialized"


def instance_deserializer(obj):
    return (obj, "instance deserialized")


def serializer_error(obj):
    raise Exception("serialization Failure")


def deserializer_error(obj):
    raise Exception("deserialization Failure")


def serializer_no_arg():
    return ""


def serializer_two_arg(arg1, arg2):
    return ""


class TestPythonSerializer(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Remove the test key if it exists each time through.
        Unset any set serializers
        """
        aerospike.unset_serializers()
        # Try to remove the key if it exists

        def teardown():
            try:
                as_connection.remove(self.test_key)
            except Exception:
                pass
            aerospike.unset_serializers()

        request.addfinalizer(teardown)

    def setup_class(cls):
        """
        Setup class
        """
        aerospike.unset_serializers()
        cls.mixed_record = {"normal": 1234, "tuple": (1, 2, 3)}
        cls.test_key = ("test", "demo", "TestPythonSerializer")

    def test_instance_serializer_and_no_class_serializer(self):
        """
        Invoke put() for record with no class serializer. There is an
        instance serializer
        """
        method_config = {"serialization": (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        response = client.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)
        assert response == 0

        _, _, bins = client.get(self.test_key)

        assert bins == {"normal": 1234, "tuple": ("instance serialized", "instance deserialized")}
        client.close()

    def test_put_with_no_serializer_arg_and_instance_serializer_set(self):
        """
        Test that when no serializer arg is passed in,
        and an instance serializer has been set,
        and a class serializer has not been set,
        the instance serializer is used.
        """
        method_config = {"serialization": (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        client.put(self.test_key, self.mixed_record)

        _, _, bins = client.get(self.test_key)

        assert bins == {"normal": 1234, "tuple": ("instance serialized", "instance deserialized")}
        client.close()

    def test_put_with_no_serializer_arg_and_class_serializer_set(self):
        """
        Test that when no serializer arg is passed in,
        and no instance serializer has been set,
        and a class serializer has been set,
        the object fails to be serialized.
        """
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)

        with pytest.raises(e.ClientError):
            self.as_connection.put(self.test_key, self.mixed_record)

    def test_put_with_serializer_arg_none_and_instance_serializer_set(self):
        """
        Invoke put() for mixed data record with serializer set to NONE
        """
        method_config = {"serialization": (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        with pytest.raises(e.ClientError):
            client.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_NONE)

    def test_with_class_serializer(self):
        """
        Invoke put() for mixed data record with class serializer.
        It should get called when serializer is passed
        """
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)

        # TODO: unnecessary variable?
        rec = {"normal": 1234, "tuple": (1, 2, 3)}  # noqa: F841
        response = self.as_connection.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)

        assert response == 0

        _, _, bins = self.as_connection.get(self.test_key)

        assert bins == {"normal": 1234, "tuple": ("class serialized", "class deserialized")}

    def test_builtin_with_class_serializer_and_instance_serializer(self):
        """
        Invoke put() passing SERIALIZER_NONE and verify that it
        causes the instance and class serializers to be ignored
        """
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)
        method_config = {"serialization": (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        with pytest.raises(e.ClientError):
            client.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_NONE)

    def test_with_class_serializer_and_instance_serializer(self):
        """
        Invoke put() for mixed data record with class and instance serializer.
        Verify that the instance serializer takes precedence over the class serializer
        """
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)
        method_config = {"serialization": (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        client.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)

        _, _, bins = client.get(self.test_key)

        assert bins == {"normal": 1234, "tuple": ("instance serialized", "instance deserialized")}
        client.close()

    def test_unset_instance_serializer(self):
        """
        Verify that calling unset serializers does not remove an instance level
        serializer
        """

        method_config = {"serialization": (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        aerospike.unset_serializers()
        client.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)

        _, _, bins = client.get(self.test_key)

        # tuples JSON-encode to a list, and we use this fact to check which
        # serializer ran:
        assert bins == {"normal": 1234, "tuple": ("instance serialized", "instance deserialized")}
        client.close()

    def test_setting_serializer_is_a_per_rec_setting(self):
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)

        self.as_connection.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)

        with pytest.raises(e.ClientError):
            self.as_connection.put(("test", "demo", "test_record_2"), self.mixed_record)

    def test_unsetting_serializers_after_a_record_put(self):
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)

        self.as_connection.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)

        aerospike.unset_serializers()

        _, _, record = self.as_connection.get(self.test_key)
        # this should not have been deserialized with the class serializer
        # it will have been serialized by the class serializer,
        # so this should be a deserialization of b'class serialized' with the bytes type
        # since server blob type is deserialized into Python types
        assert record["tuple"] == b'class serialized'

    def test_changing_deserializer_after_a_record_put(self):
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(class_deserializer)

        self.as_connection.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)

        aerospike.set_deserializer(instance_deserializer)

        _, _, record = self.as_connection.get(self.test_key)
        # this should not have been deserialized with the class serializer
        # it should now have used the instance deserializer
        assert record["tuple"] == ("class serialized", "instance deserialized")

    def test_only_setting_a_serializer(self):
        aerospike.set_serializer(class_serializer)

        self.as_connection.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)

        _, _, record = self.as_connection.get(self.test_key)
        # fetching the record doesn't require it's value to be deserialized
        assert record["tuple"] == b'class serialized'

    def test_only_setting_a_deserializer(self):
        # This should raise an error
        aerospike.set_deserializer(class_deserializer)

        with pytest.raises(e.ClientError):
            self.as_connection.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)

    def test_class_serializer_unset(self):
        """
        Verify that calling unset_serializers actually removes
        the class serializer
        """
        aerospike.set_serializer(json.dumps)
        aerospike.set_deserializer(json.loads)
        method_config = {}
        client = TestBaseClass.get_new_connection(method_config)

        aerospike.unset_serializers()
        with pytest.raises(e.ClientError) as err_info:
            client.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_class_serializer_non_callable(self):
        """
        Class serializer argument must be a callable function
        """
        with pytest.raises(e.ParamError):
            aerospike.set_serializer(5)

    def test_class_deserializer_non_callable(self):
        """
        Class deserializer argument must be a callable function
        """
        with pytest.raises(e.ParamError):
            aerospike.set_deserializer(5)

    def test_instance_serializer_non_callable(self):
        """
        Instance serializer must be a callable function
        """

        method_config = {"serialization": (5, instance_deserializer)}

        with pytest.raises(e.ParamError):
            aerospike.client(method_config)

    def test_instance_deserializer_non_callable(self):
        """
        Instance deserializer must be a callable function
        """
        with pytest.raises(e.ParamError):
            aerospike.set_deserializer(5)

        method_config = {"serialization": (instance_serializer, 5)}

        with pytest.raises(e.ParamError):
            aerospike.client(method_config)

    def test_serializer_with_no_args(self):
        aerospike.set_serializer(serializer_no_arg)
        aerospike.set_deserializer(class_deserializer)

        with pytest.raises(e.ClientError):
            self.as_connection.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)

    def test_serializer_with_two_args(self):
        aerospike.set_serializer(serializer_two_arg)
        aerospike.set_deserializer(class_deserializer)

        with pytest.raises(e.ClientError):
            self.as_connection.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)

    def test_put_with_invalid_serializer_constant(self):

        with pytest.raises(e.ClientError):
            self.as_connection.put(self.test_key, self.mixed_record, serializer=-15000)

    def test_put_with_invalid_serializer_arg_type(self):

        with pytest.raises(e.ClientError):
            self.as_connection.put(self.test_key, self.mixed_record, serializer="")

    def test_serializer_raises_error(self):

        aerospike.set_serializer(serializer_error)
        aerospike.set_deserializer(class_deserializer)

        with pytest.raises(e.ClientError):
            self.as_connection.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)

    def test_deserializer_raises_error(self):
        # If the deserializer failed, we should get a bytes
        # representation of the item
        aerospike.set_serializer(class_serializer)
        aerospike.set_deserializer(deserializer_error)

        self.as_connection.put(self.test_key, self.mixed_record, serializer=aerospike.SERIALIZER_USER)

        _, _, response = self.as_connection.get(self.test_key)

        assert response["normal"] == 1234
        assert isinstance(response["tuple"], bytes)

    # Operations and expressions

    def test_operate_with_no_serializer(self):
        ops = [
            operations.write("tuple", self.mixed_record["tuple"])
        ]
        with pytest.raises(e.ClientError):
            self.as_connection.operate(self.test_key, ops)

    def test_instance_serializer_with_operate(self):
        method_config = {"serialization": (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        ops = [operations.write(bin_name, bin_value) for bin_name, bin_value in self.mixed_record.items()]
        client.operate(self.test_key, ops)

        _, _, bins = client.get(self.test_key)

        # tuples JSON-encode to a list, and we use this fact to check which
        # serializer ran:
        assert bins == {"normal": 1234, "tuple": ("instance serialized", "instance deserialized")}
        client.close()

    def test_instance_deserializer_with_operate(self):
        method_config = {"serialization": (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        client.put(self.test_key, self.mixed_record)

        ops = [
            operations.read("tuple")
        ]
        _, _, bins = client.operate(self.test_key, ops)

        # tuples JSON-encode to a list, and we use this fact to check which
        # serializer ran:
        assert bins == {"tuple": ("instance serialized", "instance deserialized")}
        client.close()

    def test_instance_serializer_with_expressions(self):
        # We have to use a different serializer and deserializer for this test case
        # since the instance serializer and deserializer in this module don't deserialize the server value
        # back to the original Python value.
        method_config = {"serialization": (json.dumps, json.loads)}
        client = TestBaseClass.get_new_connection(method_config)

        client.put(self.test_key, self.mixed_record)

        # The tuple bin should be serialized as a server blob type
        # (1, 2, 3) will be serialized by the json.dumps serializer into a server blob type
        # If it wasn't able to be serialized, client would throw a filtered out error
        exp = expr.Eq(expr.BlobBin("tuple"), (1, 2, 3)).compile()
        client.get(self.test_key, {"expressions": exp})

        client.close()

    def test_module_serializer_with_operate_neg(self):
        # The module-level serializer will not work with operate()
        aerospike.set_serializer(class_serializer)

        ops = [operations.write("tuple", self.mixed_record["tuple"])]
        with pytest.raises(e.ClientError):
            self.as_connection.operate(self.test_key, ops)

    def test_module_deserializer_with_operate(self):
        # However, the module-level de-serializer will work with operate()
        aerospike.set_deserializer(class_deserializer)

        # Serialize the tuple and put it in the server
        method_config = {"serialization": (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)
        client.put(self.test_key, self.mixed_record)

        # Try to fetch and deserialize the tuple with operate()
        ops = [
            operations.read("tuple")
        ]
        _, _, rec = self.as_connection.operate(self.test_key, ops)

        assert rec["tuple"] == ("instance serialized", "class deserialized")

    def test_module_serializer_with_expressions_neg(self):
        # The module-level serializer will not work with expressions
        method_config = {"serialization": (instance_serializer, instance_deserializer)}
        client = TestBaseClass.get_new_connection(method_config)

        client.put(self.test_key, self.mixed_record)

        aerospike.set_serializer(class_serializer)

        exp = expr.Eq(expr.BlobBin("tuple"), (1, 2, 3)).compile()
        with pytest.raises(e.ClientError):
            self.as_connection.get(self.test_key, {"expressions": exp})

        client.close()

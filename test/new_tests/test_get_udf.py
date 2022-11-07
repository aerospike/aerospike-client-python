# -*- coding: utf-8 -*-
import sys
import pytest

from .as_status_codes import AerospikeStatus
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


@pytest.mark.usefixtures("connection_with_udf")
class TestGetRegistered(object):

    def setup_class(cls):
        """
        Set the name of the UDF to load
        store the contents of the file for future comparison
        """
        cls.udf_to_load = u"bin_lua.lua"
        cls.loaded_udf_name = "bin_lua.lua"
        cls.udf_language = aerospike.UDF_TYPE_LUA
        with open(cls.udf_to_load, 'r') as udf_file:
            cls.loaded_udf_content = udf_file.read()

    def test_udf_get_with_correct_paramters_no_policy(self):
        """
        Invoke udf_get() with correct parameters
        """
        udf_contents = self.as_connection.udf_get(
            self.loaded_udf_name, self.udf_language)

        # Check for udf file contents
        assert udf_contents == self.loaded_udf_content

    def test_udf_get_with_unicode_string_module_name(self):
        """
        Invoke udf_get() with correct parameters and a policy
        and a unicode module name
        """
        module = u"bin_lua.lua"
        policy = {'timeout': 5000}

        udf_contents = self.as_connection.udf_get(
            module, self.udf_language, policy)

        # Check for udf file contents
        assert udf_contents == self.loaded_udf_content

    def test_udf_get_with_correct_policy(self):
        """
        Invoke udf_get() with correct policy
        """
        policy = {'timeout': 5000}
        udf_contents = self.as_connection.udf_get(
            self.loaded_udf_name, self.udf_language, policy)

        # Check for udf file contents
        assert udf_contents == self.loaded_udf_content

    def test_udf_get_with_no_parameters(self):
        """
        Invoke udf_get() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.udf_get()

        assert "argument 'module' (pos 1)" in str(
            typeError.value)

    def test_udf_get_with_incorrect_policy(self):
        """
        Invoke udf_get() with incorrect policy
        """
        invalid_policy = {'timeout': 0.5}

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.udf_get(
                self.loaded_udf_name, self.udf_language, invalid_policy)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_udf_get_with_nonexistent_module(self):
        """
        Invoke udf_get() with non-existent module
        """
        non_existant_module = "bin_transform_random_fake_name"
        policy = {'timeout': 1000}

        with pytest.raises(e.UDFError) as err_info:
            self.as_connection.udf_get(
                non_existant_module, self.udf_language, policy)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_UDF

    def test_udf_get_with_invalid_language(self):
        """
        Invoke udf_get() with invalid language code
        """
        invalid_language = 85
        policy = {'timeout': 1000}

        with pytest.raises(e.ClientError) as err_info:
            self.as_connection.udf_get(
                self.loaded_udf_name, invalid_language, policy)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_udf_get_with_extra_parameter(self):
        """
        Invoke udf_get() with extra parameter.
        """
        policy = {'timeout': 1000}

        # Check for status or empty udf contents
        with pytest.raises(TypeError) as typeError:
            self.as_connection.udf_get(
                self.loaded_udf_name, self.udf_language, policy, "")

    @pytest.mark.parametrize(
        'policy',
        ('', (), [], False, 1)
    )
    def test_udf_get_policy_is_wrong_type(self, policy):
        """
        Invoke udf_get() with policy is string
        """
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.udf_get(
                self.loaded_udf_name, self.udf_language, policy)

            assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_udf_get_module_is_none(self):
        """
        Invoke udf_get() with module is none
        """
        with pytest.raises(e.ClientError) as err_info:
            self.as_connection.udf_get(None, self.udf_language)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    @pytest.mark.parametrize(
        'udf_module',
        (
            [],
            {},
            (),
            1,
            None,
            ('module',),
            bytearray('bin_lua.lua', 'utf-8')
        )
    )
    def test_invalid_module_arg_types(self, udf_module):
        with pytest.raises(e.ClientError):
            self.as_connection.udf_get(udf_module, 0)

    @pytest.mark.parametrize(
        'ltype',
        ('0', None, (), [], {})
    )
    def test_invalid_language_arg_types(self, ltype):
        with pytest.raises(TypeError):
            self.as_connection.udf_get(self.loaded_udf_name, ltype)

    def test_udf_get_with_correct_paramters_without_connection(self):
        """
        Invoke udf_get() with correct parameters without connection
        """
        policy = {'timeout': 5000}

        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        with pytest.raises(e.ClusterError) as err_info:
            client1.udf_get(self.loaded_udf_name, self.udf_language, policy)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

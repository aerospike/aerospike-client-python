/*******************************************************************************
 * Copyright 2017-2021 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

#include "tls_config.h"

static void _set_config_str_if_present(as_config *config, PyObject *tls_config,
									   const char *key);

static char *get_string_from_string_like(PyObject *string_like);

/***
 *	Param: tls_conf PyDict.
 *	Fill in the appropriate TLS values of config based on the contents of
 *	tls_config
***/
void setup_tls_config(as_config *config, PyObject *tls_config)
{

	PyObject *config_value = NULL;
	int truth_value = -1;

	// Setup string values in the tls config struct
	_set_config_str_if_present(config, tls_config, "cafile");
	_set_config_str_if_present(config, tls_config, "capath");
	_set_config_str_if_present(config, tls_config, "protocols");
	_set_config_str_if_present(config, tls_config, "cipher_suite");
	_set_config_str_if_present(config, tls_config, "cert_blacklist");
	_set_config_str_if_present(config, tls_config, "keyfile");
	_set_config_str_if_present(config, tls_config, "certfile");
	_set_config_str_if_present(config, tls_config, "keyfile_pw");

	// Setup The boolean values of the struct if they are present
	config_value = PyDict_GetItemString(tls_config, "enable");
	if (config_value) {
		truth_value = PyObject_IsTrue(config_value);
		if (truth_value != -1) {
			config->tls.enable = (bool)truth_value;
			truth_value = -1;
		}
	}

	config_value = PyDict_GetItemString(tls_config, "crl_check");
	if (config_value) {
		truth_value = PyObject_IsTrue(config_value);
		if (truth_value != -1) {
			config->tls.crl_check = (bool)truth_value;
			truth_value = -1;
		}
	}

	config_value = PyDict_GetItemString(tls_config, "crl_check_all");
	if (config_value) {
		truth_value = PyObject_IsTrue(config_value);
		if (truth_value != -1) {
			config->tls.crl_check_all = (bool)truth_value;
			truth_value = -1;
		}
	}

	config_value = PyDict_GetItemString(tls_config, "log_session_info");
	if (config_value) {
		truth_value = PyObject_IsTrue(config_value);
		if (truth_value != -1) {
			config->tls.log_session_info = (bool)truth_value;
			truth_value = -1;
		}
	}

	config_value = PyDict_GetItemString(tls_config, "for_login_only");
	if (config_value) {
		truth_value = PyObject_IsTrue(config_value);
		if (truth_value != -1) {
			config->tls.for_login_only = (bool)truth_value;
			truth_value = -1;
		}
	}
}

/***
 * Param key: name of the tls field to be set
 * Param tls_config: PyObject of a string type Unicode or String
 * Param config: the as_config in which to store information
 * If tls_config is a string type, and key is valid,
 * the appropriate field is set

***/
static void _set_config_str_if_present(as_config *config, PyObject *tls_config,
									   const char *key)
{
	PyObject *config_value = NULL;
	char *config_value_str = NULL;

	config_value = PyDict_GetItemString(tls_config, key);
	if (config_value) {
		// Get the char* value of the python config dictionary's entry for key
		config_value_str = get_string_from_string_like(config_value);
		/* Depending on the key, call the correct setter function */
		if (config_value_str) {
			if (strcmp("cafile", key) == 0) {
				as_config_tls_set_cafile(config,
										 (const char *)config_value_str);
			}
			else if (strcmp("capath", key) == 0) {
				as_config_tls_set_capath(config,
										 (const char *)config_value_str);
			}
			else if (strcmp("protocols", key) == 0) {
				as_config_tls_set_protocols(config,
											(const char *)config_value_str);
			}
			else if (strcmp("cipher_suite", key) == 0) {
				as_config_tls_set_cipher_suite(config,
											   (const char *)config_value_str);
			}
			else if (strcmp("cert_blacklist", key) == 0) {
				as_config_tls_set_cert_blacklist(
					config, (const char *)config_value_str);
			}
			else if (strcmp("keyfile", key) == 0) {
				as_config_tls_set_keyfile(config,
										  (const char *)config_value_str);
			}
			else if (strcmp("certfile", key) == 0) {
				as_config_tls_set_certfile(config,
										   (const char *)config_value_str);
			}
			else if (strcmp("keyfile_pw", key) == 0) {
				as_config_tls_set_keyfile_pw(config,
											 (const char *)config_value_str);
			}
		}
	}
}

/***
 *	Param string_like: PyObject
 *
 *	If string_like is a String or Unicode,
 *	the char* representation of it's string value is returned,
 *	else null is returned
***/
static char *get_string_from_string_like(PyObject *string_like)
{
	char *ret_str = NULL;
	PyObject *ustr = NULL;
	if (PyString_Check(string_like)) {
		ret_str = PyString_AsString(string_like);
	}
	else if (PyUnicode_Check(string_like)) {
		ustr = PyUnicode_AsUTF8String(string_like);
		if (ustr) {
			ret_str = PyBytes_AsString(ustr);
		}
	}
	return ret_str;
}

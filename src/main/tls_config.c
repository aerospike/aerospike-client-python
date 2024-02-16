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

static bool _set_config_str_if_present(as_config *config, PyObject *tls_config,
                                       const char *key);

static char *get_string_from_string_like(PyObject *string_like);

/***
 *	Param: tls_conf PyDict.
 *	Fill in the appropriate TLS values of config based on the contents of
 *	tls_config
    Returns NULL if no error occurred, or the config key and expected value type where the error occurred
***/
as_error_type_info *setup_tls_config(as_config *config, PyObject *tls_config)
{
    // Setup string values in the tls config struct
    char *tls_config_keys[] = {"cafile",       "capath",         "protocols",
                               "cipher_suite", "cert_blacklist", "keyfile",
                               "certfile",     "keyfile_pw"};
    for (unsigned long i = 0;
         i < sizeof(tls_config_keys) / sizeof(tls_config_keys[0]); i++) {
        bool error =
            _set_config_str_if_present(config, tls_config, tls_config_keys[i]);
        if (error) {
            as_error_type_info *info =
                (as_error_type_info *)malloc(sizeof(as_error_type_info));
            info->tls_key = tls_config_keys[i];
            info->expected_type = "str";
            return info;
        }
    }

    // Setup The boolean values of the struct if they are present
    char *tls_config_keys_with_bool_value[] = {
        "enable", "crl_check", "crl_check_all", "log_session_info",
        "for_login_only"};
    bool *config_bool_ptrs[] = {
        &config->tls.enable, &config->tls.crl_check, &config->tls.crl_check_all,
        &config->tls.log_session_info, &config->tls.for_login_only};
    PyObject *config_value = NULL;
    int truth_value = -1;
    unsigned long config_key_count = sizeof(tls_config_keys_with_bool_value) /
                                     sizeof(tls_config_keys_with_bool_value[0]);
    for (unsigned long i = 0; i < config_key_count; i++) {
        config_value = PyDict_GetItemString(tls_config,
                                            tls_config_keys_with_bool_value[i]);
        Py_XINCREF(config_value);
        if (config_value) {
            if (!PyBool_Check(config_value)) {
                as_error_type_info *info =
                    (as_error_type_info *)malloc(sizeof(as_error_type_info));
                info->tls_key = tls_config_keys_with_bool_value[i];
                info->expected_type = "bool";
                return info;
            }
            truth_value = PyObject_IsTrue(config_value);
            if (truth_value != -1) {
                *config_bool_ptrs[i] = (bool)truth_value;
            }
        }
        Py_XDECREF(config_value);
    }

    return NULL;
}

/***
 * Param key: name of the tls field to be set
 * Param tls_config: PyObject of a string type Unicode or String
 * Param config: the as_config in which to store information
 * If tls_config is a string type, and key is valid,
 * the appropriate field is set
 * Returns false if no error, true if an invalid value type was passed to the TLS config at "key"
***/
static bool _set_config_str_if_present(as_config *config, PyObject *tls_config,
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
        else {
            return true;
        }
    }
    return false;
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
    if (PyUnicode_Check(string_like)) {
        ret_str = (char *)PyUnicode_AsUTF8(string_like);
    }
    return ret_str;
}

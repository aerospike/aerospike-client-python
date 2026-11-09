/*******************************************************************************
 * Copyright 2026 Aerospike, Inc.
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

#include <Python.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_error.h>
#include <aerospike/as_host.h>
#include <aerospike/as_vector.h>

#include "conversions.h"
#include "discovery.h"
#include "exceptions.h"
#include "pythoncapi_compat.h"
#include "types.h"

#define HELPERS_MODULE "aerospike_helpers.discovery"
#define PROVIDER_CLASS "SeedCandidateProvider"
#define CONTEXT_CLASS "SeedCandidateContext"
#define REFRESH_METHOD "refresh_seed_candidates"

enum seed_provider_kind { SEED_PROVIDER_STATIC, SEED_PROVIDER_DNS };

struct named_enum_value {
    const char *name;
    int value;
};

// Maps the names a string option accepts to C enum values. values ends with a
// NULL name.
struct named_enum {
    bool ignore_case;
    struct named_enum_value values[4];
};

// Large enough for the quoted, comma-separated names of any named_enum
#define NAMED_ENUM_NAMES_SIZE 128

static const struct named_enum merge_policies = {
    false,
    {{"merge", AS_SEED_MERGE},
     {"replace", AS_SEED_REPLACE},
     {"discovery_only", AS_SEED_DISCOVERY_ONLY},
     {NULL, 0}}};

static const struct named_enum execution_modes = {
    false,
    {{"thread", AS_DISCOVERY_EXECUTION_THREAD},
     {"tend", AS_DISCOVERY_EXECUTION_TEND},
     {NULL, 0}}};

static const struct named_enum seed_providers = {
    false,
    {{"static", SEED_PROVIDER_STATIC}, {"dns", SEED_PROVIDER_DNS}, {NULL, 0}}};

static const struct named_enum dns_record_types = {
    true, {{"a", AS_DNS_RECORD_A}, {"srv", AS_DNS_RECORD_SRV}, {NULL, 0}}};

static const struct named_enum translators = {
    false,
    {{"static-map", AS_TRANSLATOR_STATIC_MAP},
     {"port-map", AS_TRANSLATOR_PORT_MAP},
     {"pass-through", AS_TRANSLATOR_PASS_THROUGH},
     {NULL, 0}}};

static const struct named_enum miss_policies = {
    false,
    {{"pass_through", AS_TRANSLATION_PASS_THROUGH},
     {"strict", AS_TRANSLATION_STRICT},
     {NULL, 0}}};

/*******************************************************************************
 * Option parsing
 ******************************************************************************/

// In these helpers, option_name is the full name of the option, such as
// "discovery.dns.port", and is only used in error messages. The same applies to
// as_addr_map_new_from_pyobject() (see discovery.h).

static as_status check_dict(as_error *err, PyObject *py_value,
                            const char *option_name)
{
    if (PyDict_Check(py_value)) {
        return AEROSPIKE_OK;
    }
    return as_error_update(err, AEROSPIKE_ERR_PARAM,
                           "%s must be a dict, but a %s was received instead",
                           option_name, Py_TYPE(py_value)->tp_name);
}

static as_status check_dict_keys(as_error *err, PyObject *py_dict,
                                 PyObject *py_valid_keys, bool validate_keys)
{
    if (!validate_keys) {
        return AEROSPIKE_OK;
    }

    int retval = does_py_dict_contain_valid_keys(
        err, py_dict, py_valid_keys,
        CLIENT_CONFIG_DICTIONARY_ADJECTIVE_FOR_ERROR_MESSAGE);
    if (retval == -1) {
        return error_from_raised_exception(err, AEROSPIKE_ERR_CLIENT,
                                           "Unable to validate config keys");
    }
    if (retval == 0) {
        return err->code;
    }
    return AEROSPIKE_OK;
}

// Return 1 with a new reference in *py_value if key is set and not None,
// 0 if it is not, or -1 with err set
static int get_pyobject_from_py_dict(as_error *err, PyObject *py_dict,
                                     const char *key, const char *option_name,
                                     PyObject **py_value)
{
    int retval = PyDict_GetItemStringRef(py_dict, key, py_value);
    if (retval == -1) {
        error_from_raised_exception(err, AEROSPIKE_ERR_CLIENT, option_name);
        return -1;
    }
    if (retval == 1 && Py_IsNone(*py_value)) {
        Py_DECREF(*py_value);
        *py_value = NULL;
        return 0;
    }
    return retval;
}

// Write the names of named_enum to names as "\"a\", \"b\""
static void join_named_enum_names(const struct named_enum *named_enum,
                                  char *names)
{
    names[0] = '\0';
    size_t length = 0;
    for (const struct named_enum_value *entry = named_enum->values;
         entry->name && length < NAMED_ENUM_NAMES_SIZE; entry++) {
        const char *separator = ", ";
        if (entry == named_enum->values) {
            separator = "";
        }
        length += snprintf(names + length, NAMED_ENUM_NAMES_SIZE - length,
                           "%s\"%s\"", separator, entry->name);
    }
}

static as_status
pyobject_to_named_enum_value(as_error *err, PyObject *py_value,
                             const char *option_name,
                             const struct named_enum *named_enum, int *value)
{
    if (PyUnicode_Check(py_value)) {
        const char *name = PyUnicode_AsUTF8(py_value);
        if (!name) {
            PyErr_Clear();
        }

        for (const struct named_enum_value *entry = named_enum->values;
             name && entry->name; entry++) {
            int compared;
            if (named_enum->ignore_case) {
                compared = PyOS_stricmp(name, entry->name);
            }
            else {
                compared = strcmp(name, entry->name);
            }
            if (compared == 0) {
                *value = entry->value;
                return AEROSPIKE_OK;
            }
        }
    }

    PyObject *py_repr = PyObject_Repr(py_value);
    const char *repr = NULL;
    if (py_repr) {
        repr = PyUnicode_AsUTF8(py_repr);
    }
    if (!repr) {
        PyErr_Clear();
        repr = Py_TYPE(py_value)->tp_name;
    }
    char names[NAMED_ENUM_NAMES_SIZE];
    join_named_enum_names(named_enum, names);
    as_error_update(err, AEROSPIKE_ERR_PARAM, "%s must be one of %s, not %s",
                    option_name, names, repr);
    Py_XDECREF(py_repr);
    return err->code;
}

static as_status get_uint32_from_py_dict(as_error *err, PyObject *py_dict,
                                         const char *key,
                                         const char *option_name,
                                         uint32_t *value)
{
    PyObject *py_value = NULL;
    int retval =
        get_pyobject_from_py_dict(err, py_dict, key, option_name, &py_value);
    if (retval != 1) {
        return err->code;
    }

    uint32_t number = convert_pyobject_to_uint32_t(py_value);
    Py_DECREF(py_value);
    if (PyErr_Occurred()) {
        return error_from_raised_exception(err, AEROSPIKE_ERR_PARAM,
                                           option_name);
    }
    *value = number;
    return AEROSPIKE_OK;
}

static as_status get_named_enum_value_from_py_dict(
    as_error *err, PyObject *py_dict, const char *key, const char *option_name,
    const struct named_enum *named_enum, int *value)
{
    PyObject *py_value = NULL;
    int retval =
        get_pyobject_from_py_dict(err, py_dict, key, option_name, &py_value);
    if (retval != 1) {
        return err->code;
    }

    as_status status = pyobject_to_named_enum_value(err, py_value, option_name,
                                                    named_enum, value);
    Py_DECREF(py_value);
    return status;
}

// Copy a str option. Return NULL if it is not set or on error (with err set).
// Otherwise the returned string must be freed.
static char *get_str_from_py_dict(as_error *err, PyObject *py_dict,
                                  const char *key, const char *option_name)
{
    PyObject *py_value = NULL;
    int retval =
        get_pyobject_from_py_dict(err, py_dict, key, option_name, &py_value);
    if (retval != 1) {
        return NULL;
    }

    char *value = NULL;
    const char *str = convert_pyobject_to_str(py_value);
    if (!str) {
        error_from_raised_exception(err, AEROSPIKE_ERR_PARAM, option_name);
        goto CLEANUP;
    }
    value = strdup(str);
    if (!value) {
        as_error_update(err, AEROSPIKE_ERR_CLIENT,
                        "Unable to allocate memory for %s", option_name);
    }

CLEANUP:
    Py_DECREF(py_value);
    return value;
}

static void free_str_list(char **items, uint32_t size)
{
    if (!items) {
        return;
    }
    for (uint32_t i = 0; i < size; i++) {
        free(items[i]);
    }
    free(items);
}

// Copy a list of str option. Return NULL if it is not set or on error (with err
// set). Otherwise free the returned array with free_str_list().
static char **get_str_list_from_py_dict(as_error *err, PyObject *py_dict,
                                        const char *key,
                                        const char *option_name, uint32_t *size)
{
    PyObject *py_list = NULL;
    int retval =
        get_pyobject_from_py_dict(err, py_dict, key, option_name, &py_list);
    if (retval != 1) {
        return NULL;
    }

    char **items = NULL;
    if (!PyList_Check(py_list)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "%s must be a list, but a %s was received instead",
                        option_name, Py_TYPE(py_list)->tp_name);
        goto CLEANUP;
    }

    Py_ssize_t list_size = PyList_GET_SIZE(py_list);
    if (list_size > UINT32_MAX) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "%s has too many items",
                        option_name);
        goto CLEANUP;
    }

    // +1 so an empty list never calls calloc(0), which may return NULL
    items = (char **)calloc(list_size + 1, sizeof(char *));
    if (!items) {
        as_error_update(err, AEROSPIKE_ERR_CLIENT,
                        "Unable to allocate memory for %s", option_name);
        goto CLEANUP;
    }

    for (Py_ssize_t i = 0; i < list_size; i++) {
        PyObject *py_item = PyList_GET_ITEM(py_list, i);
        const char *item = convert_pyobject_to_str(py_item);
        if (!item) {
            error_from_raised_exception(err, AEROSPIKE_ERR_PARAM, option_name);
            break;
        }
        items[i] = strdup(item);
        if (!items[i]) {
            as_error_update(err, AEROSPIKE_ERR_CLIENT,
                            "Unable to allocate memory for %s", option_name);
            break;
        }
    }
    if (err->code != AEROSPIKE_OK) {
        free_str_list(items, (uint32_t)list_size);
        items = NULL;
        goto CLEANUP;
    }
    *size = (uint32_t)list_size;

CLEANUP:
    Py_DECREF(py_list);
    return items;
}

/*******************************************************************************
 * Address maps
 ******************************************************************************/

as_status as_addr_map_new_from_pyobject(as_error *err, PyObject *py_map,
                                        const char *option_name,
                                        as_addr_map **entries, uint32_t *size)
{
    if (check_dict(err, py_map, option_name) != AEROSPIKE_OK) {
        return err->code;
    }

    Py_ssize_t map_size = PyDict_Size(py_map);
    if (map_size == 0) {
        *entries = NULL;
        *size = 0;
        return AEROSPIKE_OK;
    }

    as_addr_map *map = (as_addr_map *)malloc(sizeof(as_addr_map) * map_size);
    if (!map) {
        return as_error_update(err, AEROSPIKE_ERR_CLIENT,
                               "Unable to allocate memory for %s", option_name);
    }

    // Entries are counted as they are filled in, so as_addr_map_destroy() can
    // free a partially built map
    uint32_t count = 0;
    as_status status = AEROSPIKE_OK;
    Py_ssize_t pos = 0;
    PyObject *py_orig = NULL;
    PyObject *py_alt = NULL;
    while (PyDict_Next(py_map, &pos, &py_orig, &py_alt)) {
        if (!PyUnicode_Check(py_orig) || !PyUnicode_Check(py_alt)) {
            status =
                as_error_update(err, AEROSPIKE_ERR_PARAM,
                                "%s keys and values must be str", option_name);
            goto CLEANUP;
        }

        const char *orig = PyUnicode_AsUTF8(py_orig);
        if (!orig) {
            goto UTF8_ERROR;
        }
        const char *alt = PyUnicode_AsUTF8(py_alt);
        if (!alt) {
            goto UTF8_ERROR;
        }

        as_addr_map *entry = &map[count];
        entry->orig = strdup(orig);
        entry->alt = strdup(alt);
        // Count the entry before checking so as_addr_map_destroy() frees
        // whichever string was allocated (free(NULL) is a no-op)
        count++;
        if (!entry->orig || !entry->alt) {
            status = as_error_update(err, AEROSPIKE_ERR_CLIENT,
                                     "Unable to allocate memory for %s",
                                     option_name);
            goto CLEANUP;
        }
    }
    *entries = map;
    *size = count;
    return AEROSPIKE_OK;

UTF8_ERROR:
    PyErr_Clear();
    status = as_error_update(err, AEROSPIKE_ERR_PARAM,
                             "%s keys and values must be encodable as UTF-8",
                             option_name);

CLEANUP:
    as_addr_map_destroy(map, count);
    return status;
}

void as_addr_map_destroy(as_addr_map *entries, uint32_t size)
{
    for (uint32_t i = 0; i < size; i++) {
        free(entries[i].orig);
        free(entries[i].alt);
    }
    free(entries);
}

/*******************************************************************************
 * Python seed provider
 ******************************************************************************/

static PyObject *create_py_tuple_from_as_host(const as_host *host)
{
    if (host->tls_name) {
        return Py_BuildValue("(sHs)", host->name, host->port, host->tls_name);
    }
    return Py_BuildValue("(sH)", host->name, host->port);
}

static PyObject *
create_py_seed_candidate_context(as_error *err,
                                 const as_seed_candidate_context *context)
{
    const as_vector *seeds = context->configured_seeds;
    uint32_t seeds_size = 0;
    if (seeds) {
        seeds_size = seeds->size;
    }

    PyObject *py_seeds = PyTuple_New(seeds_size);
    if (!py_seeds) {
        goto ERROR;
    }
    for (uint32_t i = 0; i < seeds_size; i++) {
        const as_host *host =
            (const as_host *)as_vector_get((as_vector *)seeds, i);
        PyObject *py_host = create_py_tuple_from_as_host(host);
        if (!py_host) {
            goto ERROR;
        }
        PyTuple_SET_ITEM(py_seeds, i, py_host);
    }

    PyObject *py_module = PyImport_ImportModule(HELPERS_MODULE);
    if (!py_module) {
        goto ERROR;
    }
    PyObject *py_context = PyObject_CallMethod(
        py_module, CONTEXT_CLASS, "zOK", context->cluster_name, py_seeds,
        (unsigned long long)context->tend_iteration);
    Py_DECREF(py_module);
    if (!py_context) {
        goto ERROR;
    }
    Py_DECREF(py_seeds);
    return py_context;

ERROR:
    Py_XDECREF(py_seeds);
    error_from_raised_exception(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
                                "Unable to create the " CONTEXT_CLASS);
    return NULL;
}

static as_status host_error(as_error *err, Py_ssize_t index)
{
    return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
                           REFRESH_METHOD
                           "() returned an invalid host at index %zd. Each "
                           "host must be an (address, port) or (address, "
                           "port, tls_name) tuple with a str address, an int "
                           "port from 1 to 65535 and a str or None tls_name",
                           index);
}

static as_status
as_seed_candidates_init_from_pyobject(as_error *err, PyObject *py_hosts,
                                      as_seed_candidates *candidates)
{
    if (!PyList_Check(py_hosts)) {
        return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
                               REFRESH_METHOD
                               "() must return a list, but a %s was returned",
                               Py_TYPE(py_hosts)->tp_name);
    }

    for (Py_ssize_t i = 0; i < PyList_GET_SIZE(py_hosts); i++) {
        PyObject *py_host = PyList_GET_ITEM(py_hosts, i);
        if (!PyTuple_Check(py_host)) {
            return host_error(err, i);
        }
        Py_ssize_t tuple_size = PyTuple_GET_SIZE(py_host);
        if (tuple_size != 2 && tuple_size != 3) {
            return host_error(err, i);
        }

        PyObject *py_address = PyTuple_GET_ITEM(py_host, 0);
        PyObject *py_port = PyTuple_GET_ITEM(py_host, 1);
        if (!PyUnicode_Check(py_address) || !PyLong_Check(py_port)) {
            return host_error(err, i);
        }
        const char *address = PyUnicode_AsUTF8(py_address);
        long port = PyLong_AsLong(py_port);
        if (!address || PyErr_Occurred() || port < 1 || port > UINT16_MAX) {
            PyErr_Clear();
            return host_error(err, i);
        }

        const char *tls_name = NULL;
        if (tuple_size == 3) {
            PyObject *py_tls_name = PyTuple_GET_ITEM(py_host, 2);
            if (!Py_IsNone(py_tls_name) && !PyUnicode_Check(py_tls_name)) {
                return host_error(err, i);
            }
            if (!Py_IsNone(py_tls_name)) {
                tls_name = PyUnicode_AsUTF8(py_tls_name);
            }
            if (!Py_IsNone(py_tls_name) && !tls_name) {
                PyErr_Clear();
                return host_error(err, i);
            }
        }

        if (!as_seed_candidates_append(candidates, address, tls_name,
                                       (uint16_t)port)) {
            return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
                                   "Unable to add seed candidate %s:%ld",
                                   address, port);
        }
    }
    return AEROSPIKE_OK;
}

static as_status
call_python_seed_provider(as_error *err, PyObject *py_provider,
                          const as_seed_candidate_context *context,
                          as_seed_candidates *candidates)
{
    PyObject *py_context = create_py_seed_candidate_context(err, context);
    if (!py_context) {
        return err->code;
    }

    PyObject *py_hosts =
        PyObject_CallMethod(py_provider, REFRESH_METHOD, "O", py_context);
    Py_DECREF(py_context);
    if (!py_hosts) {
        return error_from_raised_exception(err,
                                           AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
                                           REFRESH_METHOD "() raised");
    }

    as_status status =
        as_seed_candidates_init_from_pyobject(err, py_hosts, candidates);
    Py_DECREF(py_hosts);
    return status;
}

// Runs on the C client's discovery thread, which holds no Python thread state
static as_status
refresh_python_seed_provider(as_error *err,
                             const as_seed_candidate_context *context,
                             as_seed_candidates *candidates, void *udata)
{
    // PyGILState_Ensure() never returns once Python is finalizing
    if (Py_IsFinalizing()) {
        return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
                               "Python is finalizing");
    }

    PyGILState_STATE gil_state = PyGILState_Ensure();
    as_status status =
        call_python_seed_provider(err, (PyObject *)udata, context, candidates);
    PyGILState_Release(gil_state);
    return status;
}

static void close_python_seed_provider(PyObject *py_provider)
{
    // The client may release the provider while an exception is pending
#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 12
    PyObject *py_pending = PyErr_GetRaisedException();
#else
    PyObject *py_pending_type = NULL;
    PyObject *py_pending = NULL;
    PyObject *py_pending_traceback = NULL;
    PyErr_Fetch(&py_pending_type, &py_pending, &py_pending_traceback);
#endif

    PyObject *py_result = PyObject_CallMethod(py_provider, "close", NULL);
    if (py_result) {
        Py_DECREF(py_result);
    }
    else {
        PyErr_WriteUnraisable(py_provider);
    }
    Py_DECREF(py_provider);

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 12
    PyErr_SetRaisedException(py_pending);
#else
    PyErr_Restore(py_pending_type, py_pending, py_pending_traceback);
#endif
}

static int close_python_seed_provider_pending(void *udata)
{
    close_python_seed_provider((PyObject *)udata);
    return 0;
}

static void release_python_seed_provider(void *udata)
{
    if (PyGILState_Check()) {
        close_python_seed_provider((PyObject *)udata);
        return;
    }

    // The C client releases a provider whose refresh was still running on its
    // discovery thread, where this must not wait for the GIL. Python closes
    // the provider later instead, or never if it is finalizing or its pending
    // call queue is full.
    if (!Py_IsFinalizing()) {
        (void)Py_AddPendingCall(close_python_seed_provider_pending, udata);
    }
}

static as_status register_python_seed_provider(as_error *err, as_config *config,
                                               PyObject *py_provider)
{
    PyObject *py_module = PyImport_ImportModule(HELPERS_MODULE);
    if (!py_module) {
        return error_from_raised_exception(err, AEROSPIKE_ERR_CLIENT,
                                           "Unable to import " HELPERS_MODULE);
    }
    PyObject *py_provider_class =
        PyObject_GetAttrString(py_module, PROVIDER_CLASS);
    Py_DECREF(py_module);
    if (!py_provider_class) {
        return error_from_raised_exception(err, AEROSPIKE_ERR_CLIENT,
                                           "Unable to get " HELPERS_MODULE
                                           "." PROVIDER_CLASS);
    }
    int is_provider = PyObject_IsInstance(py_provider, py_provider_class);
    Py_DECREF(py_provider_class);
    if (is_provider == -1) {
        return error_from_raised_exception(err, AEROSPIKE_ERR_CLIENT,
                                           "discovery.provider");
    }
    if (is_provider == 0) {
        char names[NAMED_ENUM_NAMES_SIZE];
        join_named_enum_names(&seed_providers, names);
        return as_error_update(err, AEROSPIKE_ERR_PARAM,
                               "discovery.provider must be one of %s or "
                               "an " HELPERS_MODULE "." PROVIDER_CLASS
                               " instance, but a %s was received instead",
                               names, Py_TYPE(py_provider)->tp_name);
    }

    PyObject *py_periodic =
        PyObject_CallMethod(py_provider, "needs_periodic_refresh", NULL);
    if (!py_periodic) {
        return error_from_raised_exception(
            err, AEROSPIKE_ERR_PARAM,
            "discovery.provider.needs_periodic_refresh() raised");
    }
    bool is_bool = PyBool_Check(py_periodic);
    bool periodic = Py_IsTrue(py_periodic);
    Py_DECREF(py_periodic);
    if (!is_bool) {
        return as_error_update(
            err, AEROSPIKE_ERR_PARAM,
            "discovery.provider.needs_periodic_refresh() must return a bool");
    }

    // The registration owns this reference once it succeeds
    Py_INCREF(py_provider);
    as_status status = as_config_set_binding_seed_provider(
        config, Py_TYPE(py_provider)->tp_name, refresh_python_seed_provider,
        periodic, release_python_seed_provider, py_provider, err);
    if (status != AEROSPIKE_OK) {
        Py_DECREF(py_provider);
    }
    return status;
}

/*******************************************************************************
 * DNS seed provider
 ******************************************************************************/

static as_status register_dns_seed_provider(as_error *err, as_config *config,
                                            PyObject *py_dns,
                                            bool validate_keys)
{
    as_dns_seed_provider_config dns;
    as_dns_seed_provider_config_init(&dns);
    if (!py_dns) {
        return as_config_set_dns_seed_provider(config, &dns, err);
    }

    if (check_dict(err, py_dns, "discovery.dns") != AEROSPIKE_OK) {
        return err->code;
    }
    if (check_dict_keys(err, py_dns, py_client_config_discovery_dns_valid_keys,
                        validate_keys) != AEROSPIKE_OK) {
        return err->code;
    }

    char *tls_name = NULL;
    char **servers = NULL;

    char **names = get_str_list_from_py_dict(
        err, py_dns, "names", "discovery.dns.names", &dns.names_size);
    if (err->code != AEROSPIKE_OK) {
        goto CLEANUP;
    }
    dns.names = (const char **)names;

    int record_type = dns.record_type;
    if (get_named_enum_value_from_py_dict(
            err, py_dns, "record_type", "discovery.dns.record_type",
            &dns_record_types, &record_type) != AEROSPIKE_OK) {
        goto CLEANUP;
    }
    dns.record_type = (as_dns_record_type)record_type;

    PyObject *py_port = NULL;
    if (get_pyobject_from_py_dict(err, py_dns, "port", "discovery.dns.port",
                                  &py_port) == -1) {
        goto CLEANUP;
    }
    if (py_port) {
        uint16_t port = convert_pyobject_to_uint16_t(py_port);
        Py_DECREF(py_port);
        if (PyErr_Occurred()) {
            error_from_raised_exception(err, AEROSPIKE_ERR_PARAM,
                                        "discovery.dns.port");
            goto CLEANUP;
        }
        dns.port = port;
    }

    tls_name =
        get_str_from_py_dict(err, py_dns, "tls_name", "discovery.dns.tls_name");
    if (err->code != AEROSPIKE_OK) {
        goto CLEANUP;
    }
    dns.tls_name = tls_name;

    if (get_uint32_from_py_dict(err, py_dns, "timeout_ms",
                                "discovery.dns.timeout_ms",
                                &dns.timeout_ms) != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    servers = get_str_list_from_py_dict(
        err, py_dns, "servers", "discovery.dns.servers", &dns.servers_size);
    if (err->code != AEROSPIKE_OK) {
        goto CLEANUP;
    }
    dns.servers = (const char **)servers;

    as_config_set_dns_seed_provider(config, &dns, err);

CLEANUP:
    free_str_list(names, dns.names_size);
    free(tls_name);
    free_str_list(servers, dns.servers_size);
    return err->code;
}

/*******************************************************************************
 * Public functions
 ******************************************************************************/

static as_status register_seed_provider(as_error *err, as_config *config,
                                        PyObject *py_provider, PyObject *py_dns,
                                        bool validate_keys)
{
    if (py_provider && !PyUnicode_Check(py_provider)) {
        if (py_dns) {
            return as_error_update(
                err, AEROSPIKE_ERR_PARAM,
                "discovery.dns needs discovery.provider \"dns\"");
        }
        return register_python_seed_provider(err, config, py_provider);
    }

    int provider = SEED_PROVIDER_STATIC;
    if (py_provider && pyobject_to_named_enum_value(
                           err, py_provider, "discovery.provider",
                           &seed_providers, &provider) != AEROSPIKE_OK) {
        return err->code;
    }

    if (provider == SEED_PROVIDER_DNS) {
        return register_dns_seed_provider(err, config, py_dns, validate_keys);
    }
    if (py_dns) {
        return as_error_update(
            err, AEROSPIKE_ERR_PARAM,
            "discovery.dns needs discovery.provider \"dns\"");
    }
    return AEROSPIKE_OK;
}

as_status as_discovery_config_init_from_pyobject(as_error *err,
                                                 as_config *config,
                                                 PyObject *py_discovery,
                                                 bool validate_keys)
{
    if (check_dict(err, py_discovery, "discovery") != AEROSPIKE_OK) {
        return err->code;
    }
    if (check_dict_keys(err, py_discovery,
                        py_client_config_discovery_valid_keys,
                        validate_keys) != AEROSPIKE_OK) {
        return err->code;
    }

    as_discovery_config *discovery = &config->discovery;

    int merge_policy = discovery->merge_policy;
    if (get_named_enum_value_from_py_dict(
            err, py_discovery, "merge_policy", "discovery.merge_policy",
            &merge_policies, &merge_policy) != AEROSPIKE_OK) {
        return err->code;
    }
    discovery->merge_policy = (as_seed_merge_policy)merge_policy;

    int execution_mode = discovery->execution_mode;
    if (get_named_enum_value_from_py_dict(
            err, py_discovery, "execution_mode", "discovery.execution_mode",
            &execution_modes, &execution_mode) != AEROSPIKE_OK) {
        return err->code;
    }
    discovery->execution_mode = (as_discovery_execution_mode)execution_mode;

    if (get_uint32_from_py_dict(err, py_discovery, "refresh_interval_ms",
                                "discovery.refresh_interval_ms",
                                &discovery->refresh_interval_ms) !=
        AEROSPIKE_OK) {
        return err->code;
    }
    if (get_uint32_from_py_dict(err, py_discovery, "refresh_timeout_ms",
                                "discovery.refresh_timeout_ms",
                                &discovery->refresh_timeout_ms) !=
        AEROSPIKE_OK) {
        return err->code;
    }
    if (get_uint32_from_py_dict(err, py_discovery, "tend_refresh_deadline_ms",
                                "discovery.tend_refresh_deadline_ms",
                                &discovery->tend_refresh_deadline_ms) !=
        AEROSPIKE_OK) {
        return err->code;
    }

    PyObject *py_provider = NULL;
    if (get_pyobject_from_py_dict(err, py_discovery, "provider",
                                  "discovery.provider", &py_provider) == -1) {
        return err->code;
    }
    PyObject *py_dns = NULL;
    if (get_pyobject_from_py_dict(err, py_discovery, "dns", "discovery.dns",
                                  &py_dns) == -1) {
        Py_XDECREF(py_provider);
        return err->code;
    }

    as_status status =
        register_seed_provider(err, config, py_provider, py_dns, validate_keys);
    Py_XDECREF(py_provider);
    Py_XDECREF(py_dns);
    return status;
}

static as_status register_translator(as_error *err, as_config *config,
                                     as_translator_kind translator,
                                     PyObject *py_static_map,
                                     PyObject *py_port_map)
{
    if (py_static_map && translator != AS_TRANSLATOR_STATIC_MAP) {
        return as_error_update(
            err, AEROSPIKE_ERR_PARAM,
            "translation.static_map needs translation.provider \"static-map\"");
    }
    if (py_port_map && translator != AS_TRANSLATOR_PORT_MAP) {
        return as_error_update(
            err, AEROSPIKE_ERR_PARAM,
            "translation.port_map needs translation.provider \"port-map\"");
    }
    // ip_map is the static-map translator's map
    if (config->ip_map &&
        (translator != AS_TRANSLATOR_STATIC_MAP || py_static_map)) {
        return as_error_update(
            err, AEROSPIKE_ERR_PARAM,
            "ip_map is the map of translation.provider \"static-map\". Set "
            "either ip_map or translation.static_map, and no other "
            "translation.provider");
    }

    if (translator == AS_TRANSLATOR_PASS_THROUGH) {
        return as_config_set_pass_through_translator(config, err);
    }
    if (translator == AS_TRANSLATOR_STATIC_MAP) {
        if (!py_static_map) {
            return AEROSPIKE_OK;
        }
        return as_addr_map_new_from_pyobject(
            err, py_static_map, "translation.static_map", &config->ip_map,
            &config->ip_map_size);
    }

    as_addr_map *entries = NULL;
    uint32_t size = 0;
    if (py_port_map &&
        as_addr_map_new_from_pyobject(err, py_port_map, "translation.port_map",
                                      &entries, &size) != AEROSPIKE_OK) {
        return err->code;
    }
    as_status status =
        as_config_set_port_map_translator(config, entries, size, err);
    as_addr_map_destroy(entries, size);
    return status;
}

as_status as_translation_config_init_from_pyobject(as_error *err,
                                                   as_config *config,
                                                   PyObject *py_translation,
                                                   bool validate_keys)
{
    if (check_dict(err, py_translation, "translation") != AEROSPIKE_OK) {
        return err->code;
    }
    if (check_dict_keys(err, py_translation,
                        py_client_config_translation_valid_keys,
                        validate_keys) != AEROSPIKE_OK) {
        return err->code;
    }

    int on_miss = config->translation.on_miss;
    if (get_named_enum_value_from_py_dict(err, py_translation, "on_miss",
                                          "translation.on_miss", &miss_policies,
                                          &on_miss) != AEROSPIKE_OK) {
        return err->code;
    }
    config->translation.on_miss = (as_translation_miss_policy)on_miss;

    int translator = AS_TRANSLATOR_STATIC_MAP;
    if (get_named_enum_value_from_py_dict(err, py_translation, "provider",
                                          "translation.provider", &translators,
                                          &translator) != AEROSPIKE_OK) {
        return err->code;
    }

    PyObject *py_static_map = NULL;
    if (get_pyobject_from_py_dict(err, py_translation, "static_map",
                                  "translation.static_map",
                                  &py_static_map) == -1) {
        return err->code;
    }
    PyObject *py_port_map = NULL;
    if (get_pyobject_from_py_dict(err, py_translation, "port_map",
                                  "translation.port_map", &py_port_map) == -1) {
        Py_XDECREF(py_static_map);
        return err->code;
    }

    as_status status =
        register_translator(err, config, (as_translator_kind)translator,
                            py_static_map, py_port_map);
    Py_XDECREF(py_static_map);
    Py_XDECREF(py_port_map);
    return status;
}

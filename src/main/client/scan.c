/*******************************************************************************
 * Copyright 2013-2014 Aerospike, Inc.
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

#include "client.h"
#include "scan.h"
#include "policy.h"
#include "conversions.h"
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_scan.h>

#define PROGRESS_PCT "progress_pct"
#define RECORDS_SCANNED "records_scanned"
#define STATUS "status"

AerospikeScan * AerospikeClient_Scan(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
    return AerospikeScan_New(self, args, kwds);
}

PyObject * AerospikeClient_ScanApply_Invoke(
	AerospikeClient * self, 
	char* namespace_p, char* set_p, char* module_p, char* function_p,
    PyObject * py_args, PyObject * py_policy, PyObject * py_options)
{
    as_list* arglist = NULL;
    as_policy_scan scan_policy;
    as_policy_scan* scan_policy_p = NULL;
    as_error err;
    as_scan scan;
    long scan_id = 0;

	// Initialize error
	as_error_init(&err);

    if (!(namespace_p) || !(set_p) || !(module_p) || !(function_p)) {
        goto CLEANUP;
    }

    if (!PyList_Check(py_args)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Arguments should be a list");
        goto CLEANUP;
    }

    pyobject_to_list(&err, py_args, &arglist);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    as_scan_init(&scan, namespace_p, set_p);
    if (py_policy) {
        set_policy_scan(&err, py_policy, &scan_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }
    pyobject_to_policy_scan(&err, py_policy, &scan_policy, &scan_policy_p);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    if (py_options) {
        set_scan_options(&err, &scan, py_options);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    as_scan_apply_each(&scan, module_p, function_p, arglist);
    aerospike_scan_background(self->as, &err, scan_policy_p, &scan, &scan_id);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

CLEANUP:
    if (arglist) {
        as_list_destroy(arglist);
    }

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	
	return PyLong_FromLong(scan_id);
}

PyObject * AerospikeClient_ScanApply(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	// Python Function Arguments
	PyObject * py_args = NULL;
	PyObject * py_policy = NULL;
	PyObject * py_options = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"ns", "set", "module", "function", "args", "policy", "options", NULL};
    char *namespace = NULL, *set = NULL, *module = NULL, *function = NULL;
	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "ssssO|OO:scan_apply", kwlist, &namespace, &set,
                &module, &function, &py_args, &py_policy, &py_options) == false ) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_ScanApply_Invoke(self, namespace, set, module,
            function, py_args, py_policy, py_options);
}

PyObject * AerospikeClient_ScanInfo(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
    // Initialize error
    as_error err;
    as_error_init(&err);

    // Python Function Arguments
    PyObject * py_policy = NULL;
    PyObject * retObj = PyDict_New();

    long lscanId = 0;

    as_policy_info policy_struct;
    as_policy_info *policy = NULL;
    as_scan_info scan_info;

    // Python Function Keyword Arguments
    static char * kwlist[] = {"scanid", "policy", NULL};

    // Python Function Argument Parsing
    if ( PyArg_ParseTupleAndKeywords(args, kwds, "l|O:scan_info", kwlist, &lscanId, &py_policy) == false ) {
        return NULL;
    }

    // Convert python object to policy_info 
    pyobject_to_policy_info( &err, py_policy, &policy_struct, &policy );
    if ( err.code != AEROSPIKE_OK ) {
        goto CLEANUP;
    }

    if (AEROSPIKE_OK != (aerospike_scan_info(self->as, &err,
                    policy, lscanId, &scan_info))) {
        goto CLEANUP;
    }

    if(retObj)
    {
        PyDict_SetItem(retObj, Py_BuildValue("s",PROGRESS_PCT), PyLong_FromLong(scan_info.progress_pct) );
        PyDict_SetItem(retObj, Py_BuildValue("s",RECORDS_SCANNED), PyLong_FromLong(scan_info.records_scanned) );	
        PyDict_SetItem(retObj, Py_BuildValue("s",STATUS), PyLong_FromLong(scan_info.status + AS_SCAN_STATUS));
    }

CLEANUP:

    if ( err.code != AEROSPIKE_OK ) {
        PyObject * py_err = NULL;
        error_to_pyobject(&err, &py_err);
        PyErr_SetObject(PyExc_Exception, py_err);
        return NULL;
    }

    return retObj;

}

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

/*PyObject * AerospikeClient_ScanApply(AerospikeClient * self, PyObject * args, PyObject * kwds)
  {
// Initialize error
as_error err;
as_error_init(&err);

// Python Function Arguments
PyObject * py_policy = NULL;
PyObject * py_List = NULL;
char* strNamespace = NULL;
char* strSet = NULL;
char* strModule = NULL;
char* strFuncName = NULL;
long lscanId = 0;
as_arraylist argsList;
as_arraylist * argsList_p = NULL;
as_scan scan;

// Python Function Keyword Arguments
static char * kwlist[] = {"namespace","set","module","function","list","scanid","policy",NULL};

// Python Function Argument Parsing
if ( PyArg_ParseTupleAndKeywords(args, kwds, "sssslO|O:scanApply", kwlist, 
&strNamespace,&strSet,&strModule,&strFuncName,&lscanId,&py_List ,&py_policy) == false ) {
return NULL;
}

if(!PyList_Check(py_List))
{
goto CLEANUP;
}


as_arraylist_inita(&argsList,PyList_Size(py_List));

argsList_p = &argsList;

pyobject_to_list(&err,py_List,&argsList_p);
if ( err.code != AEROSPIKE_OK ) {
goto CLEANUP;
}



as_scan_init(&scan,strNamespace,strSet);

// Convert python object to policy_info 
as_policy_scan *policy, policy_struct;
pyobject_to_policy_scan(&err, py_policy, &policy_struct, &policy );
if ( err.code != AEROSPIKE_OK ) {
goto CLEANUP;
}

if(strModule && strFuncName && (!as_scan_apply_each(&scan,strModule,strFuncName,(as_list*)argsList_p)))
{
goto CLEANUP;
}

if(AEROSPIKE_OK != aerospike_scan_background(self->as,&err, policy, &scan, &lscanId))
{
goto CLEANUP;
}


CLEANUP:

if(argsList_p)
{
as_arraylist_destroy(argsList_p);
}

as_scan_destroy(&scan);

if ( err.code != AEROSPIKE_OK ) {
    PyObject * py_err = NULL;
    error_to_pyobject(&err, &py_err);
    PyErr_SetObject(PyExc_Exception, py_err);
    return NULL;
}

return PyLong_FromLong(0);

}*/

PyObject * AerospikeClient_ScanApply_Invoke(
        AerospikeClient * self, 
        char* namespace_p, char* set_p, char* module_p, char* function_p,
        PyObject * py_args, PyObject * py_policy)
{
    as_list* arglist = NULL;
    as_policy_scan scan_policy;
    as_policy_scan* scan_policy_p = NULL;
    as_error err;
    as_scan scan;
    long scan_id;

    if (!(namespace_p) || !(set_p) || !(module_p) || !(function_p)) {
        goto CLEANUP;
    }

    if(PyList_Check(py_args)) {
        pyobject_to_list(&err, py_args, &arglist);
        if (err.code != AEROSPIKE_OK) {
            goto CLEANUP;
        }
    } else {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Arguments should be a list");
        goto CLEANUP;
    }

    as_scan_init(&scan, namespace_p, set_p);
    if (py_policy) {
        set_policy_scan(&err, py_policy, &scan_policy, &scan);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }
    pyobject_to_policy_scan(&err, py_policy, &scan_policy, &scan_policy_p);

    bool retval = as_scan_apply_each(&scan, module_p, function_p, arglist);
    printf("as_scan_apply_each is %d\n", retval);
    aerospike_scan_background(self->as, &err, scan_policy_p, &scan, &scan_id);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

CLEANUP:

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

    // Python Function Keyword Arguments
    static char * kwlist[] = {"ns", "set", "module", "function", "args", "policy", NULL};
    char *namespace = NULL, *set = NULL, *module = NULL, *function = NULL;
    // Python Function Argument Parsing
    if ( PyArg_ParseTupleAndKeywords(args, kwds, "ssssO|O:scan_apply", kwlist, &namespace, &set,
                &module, &function, &py_args, &py_policy) == false ) {
        return NULL;
    }

    // Invoke Operation
    return AerospikeClient_ScanApply_Invoke(self, namespace, set, module,
            function, py_args, py_policy);
}

PyObject * AerospikeClient_ScanInfo(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
    // Initialize error
    as_error err;
    as_error_init(&err);

    // Python Function Arguments
    PyObject * py_policy = NULL;
    long lscanId = 0;
    as_policy_info policy_struct;
    as_policy_info *policy = NULL;
    as_scan_info scan_info;
    PyObject * retObj = PyDict_New();

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
        PyDict_SetItem(retObj, Py_BuildValue("s",STATUS), PyLong_FromLong(scan_info.status + AS_SCAN_STATUS)); // Need to define macro AS_SCAN_STATUS

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

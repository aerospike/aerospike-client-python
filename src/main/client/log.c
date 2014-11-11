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


PyObject * AerospikeClient_Set_Log_Level(AerospikeClient * self, PyObject *args, PyObject * kwds)
{
	as_status status = AEROSPIKE_OK;
	// Python Function Arguments
	long lLogLevel = 0;
 	

	// Python Function Keyword Arguments
	static char * kwlist[] = {"loglevel",NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "l:setLogLevel", kwlist,&lLogLevel) == false ) {
		return NULL;
	}
	
	if(AEROSPIKE_OK != as_log_set_level(self->as->log,lLogLevel))
	{
		status = AEROSPIKE_ERR_PARAM;
		goto CLEANUP;
	}
	
	
CLEANUP:

	return PyLong_FromLong(status);

}

PyObject * AerospikeClient_Set_Log_Handler(AerospikeClient * self, PyObject *args, PyObject * kwds)
{
}

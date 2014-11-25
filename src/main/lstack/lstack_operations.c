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

#include <aerospike/aerospike_lstack.h>
#include <aerospike/as_error.h>
#include <aerospike/as_ldt.h>

#include "client.h"
#include "conversions.h"
#include "lstack.h"
#include "policy.h"

PyObject * AerospikeLStack_Push(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	return PyLong_FromLong(0);
}

PyObject * AerospikeLStack_PushMany(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	return PyLong_FromLong(0);
}

PyObject * AerospikeLStack_Peek(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	return PyLong_FromLong(0);
}

PyObject * AerospikeLStack_Filter(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	return PyLong_FromLong(0);
}

PyObject * AerospikeLStack_Destroy(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	return PyLong_FromLong(0);
}

PyObject * AerospikeLStack_Get_Capacity(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	return PyLong_FromLong(0);
}

PyObject * AerospikeLStack_Set_Capacity(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	return PyLong_FromLong(0);
}

PyObject * AerospikeLStack_Size(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	return PyLong_FromLong(0);
}

PyObject * AerospikeLStack_Config(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	return PyLong_FromLong(0);
	Py_INCREF(Py_None);
	return Py_None;
}

/*******************************************************************************
 * Copyright 2013-2021 Aerospike, Inc.
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

#pragma once

#include <Python.h>
#include <aerospike/as_status.h>

/*
 * Structure to hold user's log_callback object
 */
typedef struct Aerospike_log_callback {
    PyObject *callback;
} AerospikeLogCallback;

/**
 * Set log level for C-SDK
 *          aerospike.set_log_level( aerospike.LOG_LEVEL_WARN )
 */
PyObject *Aerospike_Set_Log_Level(PyObject *parent, PyObject *args,
                                  PyObject *kwds);

/**
 * Set log handler
 */
PyObject *Aerospike_Set_Log_Handler(PyObject *parent, PyObject *args,
                                    PyObject *kwds);

void Aerospike_Enable_Default_Logging();

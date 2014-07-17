#pragma once

#include <Python.h>
#include <stdbool.h>

#include "types.h"

#define TRACE() printf("%s:%d\n",__FILE__,__LINE__)


PyObject * AerospikeClient_create_user(AerospikeClient * self, PyObject *args);

PyObject * AerospikeClient_drop_user(AerospikeClient *self, PyObject *args);

PyObject * AerospikeClient_set_password(AerospikeClient *self, PyObject *args);

PyObject * AerospikeClient_change_password(AerospikeClient *self, PyObject *args);

PyObject * AerospikeClient_grant_roles(AerospikeClient *self, PyObject *args);

PyObject * AerospikeClient_revoke_roles(AerospikeClient *self, PyObject *args);

PyObject * AerospikeClient_replace_roles(AerospikeClient *self, PyObject *args);

PyObject * AerospikeClient_query_user(AerospikeClient *self, PyObject *args);

PyObject * AerospikeClient_query_users(AerospikeClient *self, PyObject *args);


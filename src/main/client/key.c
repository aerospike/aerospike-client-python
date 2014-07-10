#include <Python.h>
#include <stdbool.h>

#include "client.h"
#include "key.h"

AerospikeKey * AerospikeClient_Key(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	return AerospikeKey_New(self, args, kwds);
}
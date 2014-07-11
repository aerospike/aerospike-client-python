#include <Python.h>
#include <stdbool.h>

#include "client.h"
#include "query.h"

AerospikeQuery * AerospikeClient_Query(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	return AerospikeQuery_New(self, args, kwds);
}
#include <Python.h>
#include <stdbool.h>

#include "client.h"
#include "scan.h"

AerospikeScan * AerospikeClient_Scan(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	return AerospikeScan_New(self, args, kwds);
}
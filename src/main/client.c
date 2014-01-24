#include <Python.h>
#include <structmember.h>
#include <stdbool.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

#include "client.h"

#undef TRACE
#define TRACE()


/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeClientType_Methods[] = {
    {"connect",	(PyCFunction) AerospikeClient_Connect,	METH_VARARGS | METH_KEYWORDS, "Connect to the cluster."},
    {"close",	(PyCFunction) AerospikeClient_Close,	METH_VARARGS | METH_KEYWORDS, "Close the connection(s) to the cluster."},
    {"key",		(PyCFunction) AerospikeClient_Key,		METH_VARARGS | METH_KEYWORDS, "Initialize a key object for performing key operations."},
    {"query",	(PyCFunction) AerospikeClient_Query,	METH_VARARGS | METH_KEYWORDS, "Initialize a query object for peforming queries."},
    {"scan",	(PyCFunction) AerospikeClient_Scan,		METH_VARARGS | METH_KEYWORDS, "Initialize a scan object for performing scans."},
    {"info",	(PyCFunction) AerospikeClient_Info,		METH_VARARGS | METH_KEYWORDS, "Send an info request to the cluster."},
	{NULL}
};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static PyObject * AerospikeClientType_New(PyTypeObject * type, PyObject * args, PyObject * kwds)
{
	AerospikeClient * self = NULL;

    self = (AerospikeClient *) type->tp_alloc(type, 0);

    if ( self == NULL ) {
    	return NULL;
    }

	return (PyObject *) self;
}

static int AerospikeClientType_Init(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_config = NULL;

	static char * kwlist[] = {"config", NULL};
	
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O:client", kwlist, &py_config) == false ) {
		return 0;
	}

	if ( ! PyDict_Check(py_config) ) {
		return 0;
	}

    as_config config;
    as_config_init(&config);

    PyObject * py_hosts = PyDict_GetItemString(py_config, "hosts");
    if ( py_hosts && PyList_Check(py_hosts) ) {
    	int size = (int) PyList_Size(py_hosts);
    	for ( int i = 0; i < size && i < AS_CONFIG_HOSTS_SIZE; i++ ) {
    		PyObject * py_host = PyList_GetItem(py_hosts, i);
    		if ( PyTuple_Check(py_host) && PyTuple_Size(py_host) == 2 ) {
    			PyObject * py_addr = PyTuple_GetItem(py_host,0);
    			PyObject * py_port = PyTuple_GetItem(py_host,1);
    			if ( PyString_Check(py_addr) ) {
    				char * addr = PyString_AsString(py_addr);
    				config.hosts[i].addr = addr;
    			}
    			if ( PyInt_Check(py_port) ) {
    				config.hosts[i].port = (uint16_t) PyInt_AsLong(py_port);
    			}
    			else if ( PyLong_Check(py_port) ) {
    				config.hosts[i].port = (uint16_t) PyLong_AsLong(py_port);
    			}
    		}
    		else if ( PyString_Check(py_host) ) {
				char * addr = PyString_AsString(py_host);
				config.hosts[i].addr = addr;
				config.hosts[i].port = 3000;
    		}
    	}
    }

    //  = {
    //     .non_blocking = false,
    //     .hosts = { 
    //     	{ .addr = "localhost" , .port = 3000 },
    //     	{ 0 }
    //     },
    //     .lua = {
    //     	.cache_enabled = false,
    //     	.system_path = "../aerospike-mod-lua/src/lua",
    //     	.user_path = "src/test/lua"
    //     }
    // };
	TRACE();
    
    as_policies_init(&config.policies);

	// as_error err;
	// as_error_reset(&err);
	// TRACE();

	self->as = aerospike_new(&config);

	// TRACE();
	
	// if ( aerospike_connect(as, &err) == AEROSPIKE_OK ) {
	// 	self->as = as;
	// }


	TRACE();

    return 0;
}

static void AerospikeClientType_Dealloc(PyObject * self)
{
    self->ob_type->tp_free((PyObject *) self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeClientType = {
	PyObject_HEAD_INIT(NULL)

    .ob_size			= 0,
    .tp_name			= "aerospike.client",
    .tp_basicsize		= sizeof(AerospikeClient),
    .tp_itemsize		= 0,
    .tp_dealloc			= (destructor) AerospikeClientType_Dealloc,
    .tp_print			= 0,
    .tp_getattr			= 0,
    .tp_setattr			= 0,
    .tp_compare			= 0,
    .tp_repr			= 0,
    .tp_as_number		= 0,
    .tp_as_sequence		= 0,
    .tp_as_mapping		= 0,
    .tp_hash			= 0,
    .tp_call			= 0,
    .tp_str				= 0,
    .tp_getattro		= 0,
    .tp_setattro		= 0,
    .tp_as_buffer		= 0,
    .tp_flags			= Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc				= "aerospike.client doc",
    .tp_traverse		= 0,
    .tp_clear			= 0,
    .tp_richcompare		= 0,
    .tp_weaklistoffset	= 0,
    .tp_iter			= 0,
    .tp_iternext		= 0,
    .tp_methods			= AerospikeClientType_Methods,
    .tp_members			= 0,
    .tp_getset			= 0,
    .tp_base			= 0,
    .tp_dict			= 0,
    .tp_descr_get		= 0,
    .tp_descr_set		= 0,
    .tp_dictoffset		= 0,
    .tp_init			= (initproc) AerospikeClientType_Init,
    .tp_alloc			= 0,
    .tp_new				= AerospikeClientType_New
};

/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

bool AerospikeClient_Ready()
{
	return PyType_Ready(&AerospikeClientType) < 0;
}

PyObject * AerospikeClient_Create(PyObject * self, PyObject * args, PyObject * kwds)
{
    PyObject * client = AerospikeClientType.tp_new(&AerospikeClientType, args, kwds);
    AerospikeClientType.tp_init(client, args, kwds);
	return client;
}
.. _Data_Mapping:

*************************************************
Python Data Mappings
*************************************************

.. rubric:: How Python types map to server types

Default Behavior
----------------

By default, the :py:class:`~aerospike.Client` maps the supported Python types to Aerospike server \
`types <https://docs.aerospike.com/server/guide/data-types/overview>`_. \
When an unsupported type is encountered by the module:

1. When sending data to the server, it does not serialize the type and will throw an error.
2. When reading `AS_BYTES_PYTHON` types from the server, it returns the raw bytes as a :class:`bytearray`.
   To deserialize this data, the application must use cPickle instead of relying on the client to do it automatically.

Serializers
-----------

However, the functions :func:`~aerospike.set_serializer` and :func:`~aerospike.set_deserializer` \
allow for user-defined functions to handle serialization.
The serialized data is stored in the server with generic encoding \
(`AS_BYTES_BLOB <https://docs.aerospike.com/apidocs/c/d0/dd4/as__bytes_8h.html#a0cf2a6a1f39668f606b19711b3a98bf3>`_).
This type allows the storage of binary data readable by Aerospike Clients in other languages. \
The *serialization* config parameter of :func:`aerospike.client` registers an \
instance-level pair of functions that handle serialization.

.. warning::

    *Aerospike is introducing a new boolean data type in server version 5.6.*

    In order to support cross client compatibility and rolling upgrades, Python client version ``6.x`` comes with a new client config, ``send_bool_as``.
    It configures how the client writes Python booleans and allows for opting into using the new boolean type.
    It is important to consider how other clients connected to the Aerospike database write booleans in order to maintain cross client compatibility.
    For example, if there is a client that reads and writes booleans as integers, then another Python client working with the same data should do the same thing.

    ``send_bool_as`` can be set so the client writes Python booleans as integers or the Aerospike native boolean type.

    All versions before ``6.x`` wrote Python booleans as ``AS_BYTES_PYTHON``.

Data Mappings
-------------

The following table shows which Python types map directly to Aerospike server types.

+---------------------------------+------------------------+
|   Python Type                   | Server type            |
+=================================+========================+
|:class:`int`                     |`integer`_              |
+---------------------------------+------------------------+
|:class:`bool`                    |depends on send_bool_as |
+---------------------------------+------------------------+
|:class:`str`                     |`string`_               |
+---------------------------------+------------------------+
|:class:`unicode`                 |`string`_               |
+---------------------------------+------------------------+
|:class:`float`                   |`double`_               |
+---------------------------------+------------------------+
|:class:`dict`                    |`map`_                  |
+---------------------------------+------------------------+
|:class:`aerospike.KeyOrderedDict`|`key ordered map`_      |
+---------------------------------+------------------------+
|:class:`list`                    |`list`_                 |
+---------------------------------+------------------------+
|:class:`bytes`                   |`blob`_                 |
+---------------------------------+------------------------+
|:class:`aerospike.GeoJSON`       |`GeoJSON`_              |
+---------------------------------+------------------------+

.. note::

    :ref:`KeyOrderedDict <aerospike.KeyOrderedDict>` is a special case. Like :class:`dict`, :class:`~aerospike.KeyOrderedDict` maps to the Aerospike map data type. \
    However, the map will be sorted in key order before being sent to the server (see :ref:`aerospike_map_order`).

It is possible to nest these datatypes. For example a list may contain a dictionary, or a dictionary may contain a list
as a value.

.. _integer: https://docs.aerospike.com/server/guide/data-types/scalar-data-types#integer
.. _string: https://docs.aerospike.com/server/guide/data-types/scalar-data-types#string
.. _double: https://docs.aerospike.com/server/guide/data-types/scalar-data-types#double
.. _map: https://docs.aerospike.com/server/guide/data-types/cdt-map
.. _key ordered map: https://docs.aerospike.com/server/guide/data-types/cdt-map
.. _list: https://docs.aerospike.com/server/guide/data-types/cdt-list
.. _blob: https://docs.aerospike.com/server/guide/data-types/blob
.. _GeoJSON: https://docs.aerospike.com/server/guide/data-types/geospatial

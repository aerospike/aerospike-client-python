.. _aerospike.KeyOrderedDict:

.. currentmodule:: aerospike

==========================================================
:class:`aerospike.KeyOrderedDict` --- KeyOrderedDict Class
==========================================================

.. class:: KeyOrderedDict

The KeyOrderedDict class is a dictionary that directly maps to a key ordered map on the Aerospike server.
This assists in matching key ordered maps through various read operations. See the example snippet below.

.. include:: examples/keyordereddict.py
    :code: python

KeyOrderedDict inherits from :class:`dict` and has no extra functionality.
The only difference is its mapping to a key ordered map.

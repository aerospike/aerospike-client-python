.. _aerospike.ConfigProvider:

.. currentmodule:: aerospike

===================================================================
:class:`aerospike.ConfigProvider` --- Dynamic config provider class
===================================================================

Methods
=======

.. class:: ConfigProvider

    Dynamic configuration provider. Determines how to retrieve cluster policies.

    For the ``interval`` parameter, an unsigned 32-bit integer must be passed.

    :param path: Dynamic configuration file path. Cluster policies will be read from the yaml file at cluster initialization and whenever the file changes. The policies fields in the file override all command policies.
    :type path: str
    :param interval: Check dynamic configuration file for changes after this number of cluster tend iterations. Defaults to ``60``.
    :type interval: int, optional

    .. py:attribute:: path

        Dynamic configuration file path.

        This attribute is read-only.

        :type: str

    .. py:attribute:: interval

        Check dynamic configuration file for changes after this number of cluster tend iterations.

        This attribute is read-only.

        :type: int

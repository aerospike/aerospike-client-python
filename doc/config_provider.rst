.. _aerospike.ConfigProvider:

.. currentmodule:: aerospike

===================================================================
:class:`aerospike.ConfigProvider` --- Dynamic config provider class
===================================================================

Methods
=======

.. class:: ConfigProvider

    Dynamic configuration provider. Determines how to retrieve cluster policies.

    An instance of this class is immutable.

    For the ``interval`` parameter, an unsigned 32-bit integer must be passed.

    :param path: Dynamic configuration file path. Cluster policies will be read from the yaml file at cluster initialization and whenever the file changes. The policies fields in the file override all command policies.
    :type path: str

    :param interval: Interval in milliseconds between dynamic configuration check for file modifications.
        The value must be greater than or equal to the tend interval. Defaults to ``60000``.
    :type interval: int, optional

    .. py:attribute:: path

        This attribute is read-only.

        :type: str

    .. py:attribute:: interval

        This attribute is read-only.

        :type: int

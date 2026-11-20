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

    .. note::

        In the dynamic configuration file, ``metrics.extended.operational.enabled`` defaults to false, and
        ``metrics.enabled: true`` alone does not turn operational metrics on. The legacy ``metrics.enable: true``
        key turns them on when ``metrics.extended.operational.enabled`` is absent.
        :meth:`~aerospike.Client.enable_metrics` turns operational metrics on by default instead, so applications
        that already call it keep their latency and namespace metrics. When
        ``metrics.extended.operational.enabled`` is set in the file, it overrides
        :attr:`~aerospike_helpers.metrics.MetricsPolicy.operational_enabled`. See that attribute for the full rules.

    :param path: Dynamic configuration file path. Cluster policies will be read from the yaml file at cluster initialization and whenever the file changes. The policies fields in the file override all command policies.
    :type path: str

    :param interval: Interval in milliseconds between dynamic configuration check for file modifications.
        The value must be greater than or equal to the tend interval. Defaults to ``5000``.
    :type interval: int | None

    .. py:attribute:: path

        This attribute is read-only.

        :type: str

    .. py:attribute:: interval

        This attribute is read-only.

        :type: int

##########################################################################
# Copyright 2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##########################################################################

"""Classes for implementing a custom seed provider.

A seed provider returns the seed hosts the client connects to. Pass an instance of a
:class:`SeedCandidateProvider` subclass as the ``provider`` of the ``discovery`` client config.
See :ref:`aerospike_discovery` for the other discovery and address translation options.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

SeedHost = Union[Tuple[str, int], Tuple[str, int, Optional[str]]]
"""An ``(address, port)`` or ``(address, port, tls_name)`` tuple that identifies a seed host."""


@dataclass(frozen=True)
class SeedCandidateContext:
    """Passed to :meth:`SeedCandidateProvider.refresh_seed_candidates`.

    This class is not meant to be created by the user.
    """

    cluster_name: Optional[str]
    """The ``cluster_name`` of the client config, or :py:obj:`None` if it is not set."""

    configured_seeds: Tuple[SeedHost, ...]
    """The ``hosts`` of the client config."""

    tend_iteration: int
    """The number of cluster tends since the client was created."""


class SeedCandidateProvider(ABC):
    """Base class of a seed provider implemented in Python.

    The client calls the provider on its own discovery thread, so a slow refresh does not delay
    cluster tending. Connecting waits at most the ``refresh_timeout_ms`` of the ``discovery`` client
    config for the first refresh, and the client keeps using the previous seeds until a refresh
    returns. :meth:`refresh_seed_candidates` holds the GIL while it runs Python code, so it should
    not run long.

    A provider can only be used with an ``execution_mode`` of ``"thread"`` (the default).
    """

    @abstractmethod
    def refresh_seed_candidates(self, context: SeedCandidateContext) -> List[SeedHost]:
        """Return the seed hosts to connect to, with the preferred host first.

        Raise an exception if the seed hosts cannot be determined. The client then keeps the
        previous seeds, and raises :exc:`~aerospike.exception.SeedDiscoveryFailed` when connecting
        if it has no seed host left.

        :param context: The client state when the seeds are refreshed.
        :return: A list of ``(address, port)`` or ``(address, port, tls_name)`` tuples.
        """

    @abstractmethod
    def needs_periodic_refresh(self) -> bool:
        """Return :py:obj:`True` if the seed hosts can change after the client connects.

        If so, the client calls :meth:`refresh_seed_candidates` every ``refresh_interval_ms`` of
        the ``discovery`` client config. Otherwise, it is only called when the client connects.
        This method is called once when the client is created.
        """

    def close(self) -> None:
        """Release resources held by the provider.

        Called once when the client object is destroyed, but not by :meth:`aerospike.Client.close`
        because the client can connect again. Exceptions raised by this method are ignored. The
        default implementation does nothing.
        """

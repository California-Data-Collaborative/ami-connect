from abc import ABC, abstractmethod
from typing import Mapping, Optional

from amiadapters.configuration.models import (
    MetricsConfigurationBase,
    NoopMetricsConfiguration,
)


class MetricsBackend(ABC):
    """
    Abstract base class for metrics backends
    """

    @abstractmethod
    def incr(
        self,
        name: str,
        value: int = 1,
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        pass

    @abstractmethod
    def gauge(
        self,
        name: str,
        value: float,
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        pass

    @abstractmethod
    def timing(
        self,
        name: str,
        value_seconds: float,
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        pass


class NoopMetricsBackend(MetricsBackend):
    def incr(self, *args, **kwargs):
        pass

    def gauge(self, *args, **kwargs):
        pass

    def timing(self, *args, **kwargs):
        pass


class Metrics:
    """
    Metrics client that delegates to the backend
    configured by the pipeline.
    """

    @classmethod
    def from_configuration(
        cls,
        config: MetricsConfigurationBase,
    ) -> "Metrics":
        if isinstance(config, NoopMetricsConfiguration):
            return cls(backend=NoopMetricsBackend())
        raise ValueError(f"Unrecognized metrics configuration type {type(config)}")

    def __init__(self, backend: MetricsBackend):
        self._backend = backend

    def incr(self, name, value=1, tags=None):
        self._backend.incr(name, value, tags)

    def gauge(self, name, value, tags=None):
        self._backend.gauge(name, value, tags)

    def timing(self, name, value_seconds, tags=None):
        self._backend.timing(name, value_seconds, tags)

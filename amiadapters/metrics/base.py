from abc import ABC, abstractmethod
from contextlib import contextmanager
import time
from typing import Mapping, Optional
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ProfileNotFound


from amiadapters.configuration.env import get_global_aws_profile, get_global_aws_region
from amiadapters.configuration.models import (
    CloudwatchMetricsConfiguration,
    MetricsConfigurationBase,
    NoopMetricsConfiguration,
)


class MetricsBackend(ABC):
    """
    Abstract base class for metrics backends, which report telemetry data about
    our system. Implementations can connect to different metrics systems, e.g. CloudWatch.
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
    """
    A no-op metrics backend that does nothing. This is the default backend.
    """

    def incr(self, *args, **kwargs):
        pass

    def gauge(self, *args, **kwargs):
        pass

    def timing(self, *args, **kwargs):
        pass


class Metrics:
    """
    Metrics client that delegates to the backend
    configured by the pipeline. The application code uses this class
    so that it is decoupled from the specific metrics backend implementation.
    """

    @classmethod
    def from_configuration(
        cls,
        config: MetricsConfigurationBase,
    ) -> "Metrics":
        if isinstance(config, NoopMetricsConfiguration):
            return cls(backend=NoopMetricsBackend())
        elif isinstance(config, CloudwatchMetricsConfiguration):
            return cls(backend=CloudWatchMetricsBackend(config))
        raise ValueError(f"Unrecognized metrics configuration type {type(config)}")

    def __init__(self, backend: MetricsBackend):
        self._backend = backend

    def incr(self, name, value=1, tags=None):
        self._backend.incr(name, value, tags)

    def gauge(self, name, value, tags=None):
        self._backend.gauge(name, value, tags)

    def timing(self, name, value_seconds, tags=None):
        self._backend.timing(name, value_seconds, tags)

    @contextmanager
    def timed_task(self, name, tags):
        start = time.monotonic()
        try:
            yield
            success = True
        except Exception:
            success = False
            raise
        finally:
            duration = time.monotonic() - start
            self.timing(
                name,
                duration,
                tags={**tags, "success": str(success).lower()},
            )


class CloudWatchMetricsBackend(MetricsBackend):
    """
    A metrics backend that reports metrics to AWS CloudWatch.
    """

    def __init__(self, config: CloudwatchMetricsConfiguration):
        self.namespace = config.namespace
        if config.cloudwatch_client is None:
            aws_profile_name = get_global_aws_profile()
            aws_region = get_global_aws_region()
            if aws_profile_name:
                try:
                    session = boto3.Session(profile_name=aws_profile_name)
                    self.client = session.client("cloudwatch", region_name=aws_region)
                except ProfileNotFound as e:
                    self.client = boto3.client("cloudwatch", region_name=aws_region)
            else:
                # If we could not find a profile name, we create the client and rely on
                # IAM roles for authorization, e.g. on the Airflow server
                self.client = boto3.client("cloudwatch", region_name=aws_region)
        else:
            self.client = config.cloudwatch_client

    def incr(self, name, value=1, tags=None):
        self._put(name, value, "Count", tags)

    def gauge(self, name, value, tags=None):
        self._put(name, value, "None", tags)

    def timing(self, name, value_seconds, tags=None):
        self._put(name, value_seconds, "Seconds", tags)

    def _put(self, name, value, unit, tags):
        self.client.put_metric_data(
            Namespace=self.namespace,
            MetricData=[
                {
                    "MetricName": name,
                    "Timestamp": datetime.now(timezone.utc),
                    "Value": value,
                    "Unit": unit,
                    "Dimensions": [
                        {"Name": k, "Value": v} for k, v in (tags or {}).items()
                    ],
                }
            ],
        )

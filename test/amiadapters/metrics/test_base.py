from unittest.mock import MagicMock

from amiadapters.metrics.base import (
    Metrics,
    MetricsBackend,
    NoopMetricsBackend,
    NoopMetricsConfiguration,
)
from test.base_test_case import BaseTestCase


class TestMetricsGauge(BaseTestCase):
    def setUp(self):
        # Create a mock backend to verify calls
        self.mock_backend = MagicMock(spec=MetricsBackend)
        self.metrics = Metrics(backend=self.mock_backend)

    def test_incr_calls_backend_with_correct_args(self):
        self.metrics.incr("test.metric", 42.5, tags={"env": "prod"})
        self.mock_backend.incr.assert_called_once_with(
            "test.metric", 42.5, {"env": "prod"}
        )

    def test_timing_calls_backend_with_correct_args(self):
        self.metrics.timing("test.metric", 42.5, tags={"env": "prod"})
        self.mock_backend.timing.assert_called_once_with(
            "test.metric", 42.5, {"env": "prod"}
        )

    def test_gauge_calls_backend_with_correct_args(self):
        self.metrics.gauge("test.metric", 42.5, tags={"env": "prod"})
        self.mock_backend.gauge.assert_called_once_with(
            "test.metric", 42.5, {"env": "prod"}
        )

    def test_gauge_calls_backend_with_default_tags(self):
        self.metrics.gauge("test.metric", 10.0)
        self.mock_backend.gauge.assert_called_once_with("test.metric", 10.0, None)

    def test_gauge_from_configuration_with_noop_metrics_configuration(self):
        config = NoopMetricsConfiguration()
        metrics = Metrics.from_configuration(config)
        # Should not raise and should use NoopMetricsBackend
        self.assertIsInstance(metrics._backend, NoopMetricsBackend)

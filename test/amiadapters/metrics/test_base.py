from unittest.mock import MagicMock

from amiadapters.metrics.base import (
    CloudWatchMetricsBackend,
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


class TestCloudWatchMetricsBackend(BaseTestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.namespace = "TestNamespace"
        self.cw_backend = CloudWatchMetricsBackend(
            namespace=self.namespace, cloudwatch_client=self.mock_client
        )

    def test_incr_calls_put_metric_data_with_count_unit(self):
        self.cw_backend.incr("my.metric", 3, tags={"foo": "bar"})
        self.mock_client.put_metric_data.assert_called_once()
        args, kwargs = self.mock_client.put_metric_data.call_args
        self.assertEqual(kwargs["Namespace"], self.namespace)
        metric = kwargs["MetricData"][0]
        self.assertEqual(metric["MetricName"], "my.metric")
        self.assertEqual(metric["Value"], 3)
        self.assertEqual(metric["Unit"], "Count")
        self.assertEqual(metric["Dimensions"], [{"Name": "foo", "Value": "bar"}])
        self.assertIn("Timestamp", metric)

    def test_gauge_calls_put_metric_data_with_none_unit(self):
        self.cw_backend.gauge("my.gauge", 7.5, tags={"env": "dev"})
        self.mock_client.put_metric_data.assert_called_once()
        metric = self.mock_client.put_metric_data.call_args[1]["MetricData"][0]
        self.assertEqual(metric["MetricName"], "my.gauge")
        self.assertEqual(metric["Value"], 7.5)
        self.assertEqual(metric["Unit"], "None")
        self.assertEqual(metric["Dimensions"], [{"Name": "env", "Value": "dev"}])

    def test_timing_calls_put_metric_data_with_seconds_unit(self):
        self.cw_backend.timing("my.timer", 1.23, tags={"service": "api"})
        self.mock_client.put_metric_data.assert_called_once()
        metric = self.mock_client.put_metric_data.call_args[1]["MetricData"][0]
        self.assertEqual(metric["MetricName"], "my.timer")
        self.assertEqual(metric["Value"], 1.23)
        self.assertEqual(metric["Unit"], "Seconds")
        self.assertEqual(metric["Dimensions"], [{"Name": "service", "Value": "api"}])

    def test_put_metric_data_with_no_tags(self):
        self.cw_backend.incr("no.tags.metric", 5)
        self.mock_client.put_metric_data.assert_called_once()
        metric = self.mock_client.put_metric_data.call_args[1]["MetricData"][0]
        self.assertEqual(metric["Dimensions"], [])

    def test_cloudwatch_backend_is_metrics_backend(self):
        self.assertIsInstance(self.cw_backend, MetricsBackend)

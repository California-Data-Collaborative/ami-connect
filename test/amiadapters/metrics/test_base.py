from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from amiadapters.configuration.models import (
    CloudwatchMetricsConfiguration,
    NoopMetricsConfiguration,
)
from amiadapters.metrics.base import (
    CloudWatchMetricsBackend,
    Metrics,
    MetricsBackend,
    NoopMetricsBackend,
    seconds_since,
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
            "test.metric", 42.5, "None", {"env": "prod"}
        )

    def test_gauge_calls_backend_with_default_tags(self):
        self.metrics.gauge("test.metric", 10.0)
        self.mock_backend.gauge.assert_called_once_with(
            "test.metric", 10.0, "None", None
        )

    def test_gauge_from_configuration_with_noop_metrics_configuration(self):
        config = NoopMetricsConfiguration()
        metrics = Metrics.from_configuration(config)
        # Should not raise and should use NoopMetricsBackend
        self.assertIsInstance(metrics._backend, NoopMetricsBackend)


class TestMetricsClient(BaseTestCase):

    def test_from_configuration_with_noop_metrics_configuration(self):
        config = NoopMetricsConfiguration()
        metrics = Metrics.from_configuration(config)
        self.assertIsInstance(metrics._backend, NoopMetricsBackend)

    def test_from_configuration_with_cloudwatch_metrics_configuration(self):

        config = CloudwatchMetricsConfiguration(cloudwatch_client=MagicMock())
        metrics = Metrics.from_configuration(config)
        self.assertIsInstance(metrics._backend, CloudWatchMetricsBackend)
        self.assertEqual(metrics._backend.namespace, "ami-connect")


class TestNoopMetricsBackend(BaseTestCase):
    def setUp(self):
        self.noop_backend = NoopMetricsBackend()

    def test_incr_noops(self):
        self.noop_backend.incr("test.metric", 42.5, tags={"env": "prod"})

    def test_timing_noops(self):
        self.noop_backend.timing("test.metric", 42.5, tags={"env": "prod"})

    def test_gauge_noops(self):
        self.noop_backend.gauge("test.metric", 42.5, tags={"env": "prod"})


class TestCloudWatchMetricsBackend(BaseTestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.namespace = "TestNamespace"
        self.cw_backend = CloudWatchMetricsBackend(
            CloudwatchMetricsConfiguration(
                namespace=self.namespace, cloudwatch_client=self.mock_client
            )
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


class TestSecondsSince(BaseTestCase):
    def test_seconds_since_with_datetime_with_timezone(self):
        now = datetime.now(timezone.utc)
        earlier = now - timedelta(seconds=42)
        result = seconds_since(earlier)
        self.assertTrue(41.9 < result < 42.1)  # allow for small timing differences

    def test_seconds_since_with_naive_datetime(self):
        now = datetime.now()
        earlier = now - timedelta(seconds=10)
        # seconds_since should treat naive datetime as UTC
        result = seconds_since(earlier)
        self.assertTrue(9.9 < result < 10.1)

    def test_seconds_since_with_isoformat_string(self):
        now = datetime.now(timezone.utc)
        earlier = now - timedelta(seconds=5)
        earlier_str = earlier.isoformat()
        result = seconds_since(earlier_str)
        self.assertTrue(4.9 < result < 5.1)

    def test_seconds_since_with_naive_isoformat_string(self):
        now = datetime.now()
        earlier = now - timedelta(seconds=3)
        earlier_str = earlier.isoformat()
        result = seconds_since(earlier_str)
        self.assertTrue(2.9 < result < 3.1)

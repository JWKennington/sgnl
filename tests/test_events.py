"""Tests for sgnl.events"""

import argparse
import importlib
import signal
import sys
from unittest import mock

import pytest

# Mock confluent_kafka before importing events
mock_kafka = mock.MagicMock()
mock_kafka.KafkaError._PARTITION_EOF = 1
sys.modules["confluent_kafka"] = mock_kafka
sys.modules["confluent_kafka.admin"] = mock.MagicMock()

from sgnl import events  # noqa: E402


class TestImportError:
    """Test ImportError handling when confluent_kafka is not available."""

    def test_import_error_when_kafka_missing(self):
        """Test that ImportError is raised when confluent_kafka is missing."""
        # Save original modules
        original_kafka = sys.modules.get("confluent_kafka")
        original_kafka_admin = sys.modules.get("confluent_kafka.admin")
        original_events = sys.modules.get("sgnl.events")

        try:
            # Remove confluent_kafka from sys.modules
            if "confluent_kafka" in sys.modules:
                del sys.modules["confluent_kafka"]
            if "confluent_kafka.admin" in sys.modules:
                del sys.modules["confluent_kafka.admin"]
            if "sgnl.events" in sys.modules:
                del sys.modules["sgnl.events"]

            # Make import raise ImportError
            with mock.patch.dict(
                sys.modules,
                {"confluent_kafka": None},
            ):
                with pytest.raises(ImportError) as exc_info:
                    importlib.import_module("sgnl.events")

                assert "confluent_kafka is required" in str(exc_info.value)
        finally:
            # Restore original modules
            if original_kafka is not None:
                sys.modules["confluent_kafka"] = original_kafka
            if original_kafka_admin is not None:
                sys.modules["confluent_kafka.admin"] = original_kafka_admin
            if original_events is not None:
                sys.modules["sgnl.events"] = original_events


class TestEventProcessor:
    """Tests for EventProcessor class."""

    @pytest.fixture
    def mock_producer(self):
        """Create a mock producer."""
        return mock.MagicMock()

    @pytest.fixture
    def mock_consumer(self):
        """Create a mock consumer."""
        return mock.MagicMock()

    @pytest.fixture
    def mock_admin_client(self):
        """Create a mock admin client."""
        admin = mock.MagicMock()
        topic_metadata = mock.MagicMock()
        topic_metadata.topics = {}
        admin.list_topics.return_value = topic_metadata
        return admin

    def test_init_basic(self, mock_producer, mock_admin_client):
        """Test basic initialization."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                input_topic="input",
                output_topic="output",
            )

            assert processor.kafka_settings["bootstrap.servers"] == "localhost:9092"
            assert processor.is_running is False

    def test_init_no_kafka_server_raises(self):
        """Test that missing kafka_server raises assertion."""
        with pytest.raises(AssertionError):
            events.EventProcessor()

    def test_init_as_source(self, mock_producer, mock_admin_client):
        """Test initialization as source (no input_topic)."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
            )

            assert processor.is_source is True
            assert processor.is_sink is False

    def test_init_as_sink(self, mock_producer, mock_admin_client, mock_consumer):
        """Test initialization as sink (no output_topic)."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "Consumer", return_value=mock_consumer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                input_topic="input",
            )

            assert processor.is_source is False
            assert processor.is_sink is True

    def test_init_sink_with_partitions_raises(
        self, mock_producer, mock_admin_client, mock_consumer
    ):
        """Test that sink with topic_partitions raises assertion."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "Consumer", return_value=mock_consumer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            with pytest.raises(AssertionError):
                events.EventProcessor(
                    kafka_server="localhost:9092",
                    input_topic="input",
                    topic_partitions=2,
                )

    def test_init_with_string_input_topic(
        self, mock_producer, mock_admin_client, mock_consumer
    ):
        """Test initialization with string input_topic."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "Consumer", return_value=mock_consumer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                input_topic="input",
                output_topic="output",
            )

            assert processor.is_source is False

    def test_init_with_string_output_topic(
        self, mock_producer, mock_admin_client, mock_consumer
    ):
        """Test initialization with string output_topic."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "Consumer", return_value=mock_consumer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                input_topic=["input"],
                output_topic="output",
            )

            assert processor.is_sink is False

    def test_init_with_int_topic_partitions(
        self, mock_producer, mock_admin_client, mock_consumer
    ):
        """Test initialization with int topic_partitions."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "Consumer", return_value=mock_consumer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                input_topic="input",
                output_topic="output",
                topic_partitions=2,
            )

            assert processor is not None

    def test_init_mismatched_partitions_raises(
        self, mock_producer, mock_admin_client, mock_consumer
    ):
        """Test that mismatched output_topic and topic_partitions raises."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "Consumer", return_value=mock_consumer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            with pytest.raises(AssertionError):
                events.EventProcessor(
                    kafka_server="localhost:9092",
                    input_topic="input",
                    output_topic=["output1", "output2"],
                    topic_partitions=[1],  # Mismatched length
                )

    def test_init_topic_exists(self, mock_producer, mock_admin_client, mock_consumer):
        """Test initialization when topic already exists."""
        mock_admin_client.list_topics.return_value.topics = {"output": True}

        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "Consumer", return_value=mock_consumer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            events.EventProcessor(
                kafka_server="localhost:9092",
                input_topic="input",
                output_topic="output",
            )

            # Should not create topic since it exists
            assert not mock_admin_client.create_topics.called

    def test_init_with_heartbeats(self, mock_producer, mock_admin_client):
        """Test initialization with heartbeats enabled."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
            mock.patch.object(events, "gps_now", return_value=1000000000),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
                send_heartbeats=True,
                heartbeat_cadence=10.0,
                heartbeat_topic="heartbeats",
            )

            assert processor.send_heartbeats is True
            assert processor.heartbeat_topic == "heartbeats"
            assert processor.heartbeat_cadence == 10.0

    def test_init_with_sgn_control(self, mock_producer, mock_admin_client):
        """Test initialization with sgn_control."""
        mock_control = mock.MagicMock()

        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
                sgn_control=mock_control,
            )

            assert processor.sgn_control is mock_control

    def test_fetch_with_valid_messages(
        self, mock_producer, mock_admin_client, mock_consumer
    ):
        """Test fetch with valid messages."""
        mock_message = mock.MagicMock()
        mock_message.error.return_value = None
        mock_consumer.consume.return_value = [mock_message]

        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "Consumer", return_value=mock_consumer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                input_topic="input",
                output_topic="output",
            )
            processor.ingest = mock.MagicMock()

            processor.fetch()

            processor.ingest.assert_called_once_with(mock_message)

    def test_fetch_with_message_error(
        self, mock_producer, mock_admin_client, mock_consumer
    ):
        """Test fetch with message error."""
        mock_error = mock.MagicMock()
        mock_error.code.return_value = 999  # Not _PARTITION_EOF

        mock_message = mock.MagicMock()
        mock_message.error.return_value = mock_error
        mock_consumer.consume.return_value = [mock_message]

        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "Consumer", return_value=mock_consumer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
            mock.patch("logging.warning") as mock_warning,
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                input_topic="input",
                output_topic="output",
            )

            processor.fetch()

            mock_warning.assert_called()

    def test_fetch_with_partition_eof(
        self, mock_producer, mock_admin_client, mock_consumer
    ):
        """Test fetch with partition EOF error (ignored)."""
        mock_error = mock.MagicMock()
        mock_error.code.return_value = events.KafkaError._PARTITION_EOF

        mock_message = mock.MagicMock()
        mock_message.error.return_value = mock_error
        mock_consumer.consume.return_value = [mock_message]

        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "Consumer", return_value=mock_consumer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
            mock.patch("logging.warning") as mock_warning,
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                input_topic="input",
                output_topic="output",
            )

            processor.fetch()

            # Should not log warning for partition EOF
            mock_warning.assert_not_called()

    def test_fetch_with_none_message(
        self, mock_producer, mock_admin_client, mock_consumer
    ):
        """Test fetch with None message."""
        mock_consumer.consume.return_value = [None]

        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "Consumer", return_value=mock_consumer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                input_topic="input",
                output_topic="output",
            )
            processor.ingest = mock.MagicMock()

            processor.fetch()

            processor.ingest.assert_not_called()

    def test_process_loop(self, mock_producer, mock_admin_client, mock_consumer):
        """Test process loop."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "Consumer", return_value=mock_consumer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
            mock.patch("time.sleep"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                input_topic="input",
                output_topic="output",
            )

            # Set up to stop after one iteration
            call_count = [0]

            def stop_after_one():
                call_count[0] += 1
                if call_count[0] > 1:
                    processor.is_running = False

            processor.handle = stop_after_one
            processor.is_running = True

            processor.process()

            assert processor.is_running is False

    def test_process_as_source(self, mock_producer, mock_admin_client):
        """Test process loop as source (no fetch)."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
            mock.patch("time.sleep"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
            )

            call_count = [0]

            def stop_after_one():
                call_count[0] += 1
                if call_count[0] > 1:
                    processor.is_running = False

            processor.handle = stop_after_one
            processor.is_running = True

            processor.process()

            assert processor.is_source is True

    def test_process_with_heartbeats(self, mock_producer, mock_admin_client):
        """Test process loop with heartbeats."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
            mock.patch("time.sleep"),
            mock.patch.object(events, "gps_now", return_value=1000000000),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
                send_heartbeats=True,
                heartbeat_cadence=10.0,
                heartbeat_topic="heartbeats",
            )

            call_count = [0]

            def stop_after_one():
                call_count[0] += 1
                if call_count[0] > 1:
                    processor.is_running = False

            processor.handle = stop_after_one
            processor.heartbeat = mock.MagicMock()
            processor.is_running = True

            processor.process()

            processor.heartbeat.assert_called()

    def test_process_with_sgn_control_eos(self, mock_producer, mock_admin_client):
        """Test process loop with sgn_control signaling EOS."""
        mock_control = mock.MagicMock()
        mock_control.signaled_eos.return_value = True

        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
            mock.patch("time.sleep"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
                sgn_control=mock_control,
            )

            processor.handle = mock.MagicMock()
            processor.is_running = True

            processor.process()

            assert processor.is_running is False

    def test_start(self, mock_producer, mock_admin_client):
        """Test start method."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
            mock.patch("logging.info"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
            )

            # Mock process to just set is_running to False
            def mock_process():
                processor.is_running = False

            processor.process = mock_process

            processor.start()

            # start() sets is_running to True before calling process()
            # but process() then sets it to False

    def test_stop(self, mock_producer, mock_admin_client):
        """Test stop method."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
            mock.patch("logging.info"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
            )
            processor.is_running = True

            processor.stop()

            assert processor.is_running is False
            mock_producer.flush.assert_called_once()

    def test_catch_signal(self, mock_producer, mock_admin_client):
        """Test catch signal handler."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
            mock.patch("logging.info"),
            mock.patch("sys.exit") as mock_exit,
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
            )
            processor.is_running = True

            processor.catch(signal.SIGINT, None)

            mock_exit.assert_called_once_with(0)
            assert processor.is_running is False

    def test_ingest_returns_not_implemented(self, mock_producer, mock_admin_client):
        """Test ingest returns NotImplementedError."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
            )

            result = processor.ingest(None)

            assert result == NotImplementedError

    def test_handle_returns_not_implemented(self, mock_producer, mock_admin_client):
        """Test handle returns NotImplementedError."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
            )

            result = processor.handle()

            assert result == NotImplementedError

    def test_finish(self, mock_producer, mock_admin_client):
        """Test finish method (default does nothing)."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
            )

            # Should not raise
            processor.finish()

    def test_heartbeat_sends_when_cadence_exceeded(
        self, mock_producer, mock_admin_client
    ):
        """Test heartbeat sends message when cadence exceeded."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
            mock.patch.object(events, "gps_now", side_effect=[1000000000, 1000000015]),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
                send_heartbeats=True,
                heartbeat_cadence=10.0,
                heartbeat_topic="heartbeats",
            )

            processor.heartbeat()

            mock_producer.produce.assert_called_once()
            mock_producer.poll.assert_called_once_with(0)

    def test_heartbeat_skips_when_cadence_not_exceeded(
        self, mock_producer, mock_admin_client
    ):
        """Test heartbeat skips when cadence not exceeded."""
        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
            mock.patch.object(events, "gps_now", side_effect=[1000000000, 1000000005]),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
                send_heartbeats=True,
                heartbeat_cadence=10.0,
                heartbeat_topic="heartbeats",
            )

            processor.heartbeat()

            mock_producer.produce.assert_not_called()

    def test_topic_exists_true(self, mock_producer, mock_admin_client):
        """Test topic_exists returns True when topic exists."""
        mock_admin_client.list_topics.return_value.topics = {"existing_topic": True}

        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
            )

            result = processor.topic_exists("existing_topic")

            assert result is True

    def test_topic_exists_false(self, mock_producer, mock_admin_client):
        """Test topic_exists returns False when topic doesn't exist."""
        mock_admin_client.list_topics.return_value.topics = {}

        with (
            mock.patch.object(events, "Producer", return_value=mock_producer),
            mock.patch.object(events, "AdminClient", return_value=mock_admin_client),
            mock.patch("signal.signal"),
        ):
            processor = events.EventProcessor(
                kafka_server="localhost:9092",
                output_topic="output",
            )

            result = processor.topic_exists("nonexistent_topic")

            assert result is False


class TestAppendArgs:
    """Tests for append_args function."""

    def test_append_args(self):
        """Test appending arguments to parser."""
        parser = argparse.ArgumentParser()

        events.append_args(parser)

        # Parse with default values
        args = parser.parse_args([])

        assert args.tag == "default"
        assert args.processing_cadence == 0.1
        assert args.request_timeout == 0.2
        assert args.kafka_server is None
        assert args.input_topic is None

    def test_append_args_with_values(self):
        """Test appending arguments with custom values."""
        parser = argparse.ArgumentParser()

        events.append_args(parser)

        args = parser.parse_args(
            [
                "--tag",
                "custom",
                "--processing-cadence",
                "0.5",
                "--request-timeout",
                "1.0",
                "--kafka-server",
                "localhost:9092",
                "--input-topic",
                "topic1",
                "--input-topic",
                "topic2",
            ]
        )

        assert args.tag == "custom"
        assert args.processing_cadence == 0.5
        assert args.request_timeout == 1.0
        assert args.kafka_server == "localhost:9092"
        assert args.input_topic == ["topic1", "topic2"]

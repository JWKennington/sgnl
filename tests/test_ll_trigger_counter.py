"""Tests for sgnl.bin.ll_trigger_counter"""

import json
import sys
from collections import deque
from unittest import mock

import pytest

# Mock ezdag before importing the module
sys.modules["ezdag"] = mock.MagicMock()

from sgnl.bin import ll_trigger_counter  # noqa: E402


class TestXmlString:
    """Tests for xml_string function."""

    def test_xml_string_returns_utf8(self):
        """Test xml_string returns UTF-8 string."""
        mock_rstat = mock.MagicMock()
        mock_rstat.save_fileobj = mock.MagicMock(
            side_effect=lambda f: f.write(b"<xml>test</xml>")
        )

        result = ll_trigger_counter.xml_string(mock_rstat)

        assert result == "<xml>test</xml>"
        mock_rstat.save_fileobj.assert_called_once()


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_default_values(self):
        """Test parsing with default values."""
        with mock.patch("sys.argv", ["ll_trigger_counter"]):
            args = ll_trigger_counter.parse_command_line()
            assert args.topic == "coinc"
            assert args.output_period == 3600.0
            assert args.output == "zerolag_rankingstatpdf.xml.gz"
            assert args.tag == "test"
            assert args.verbose is False

    def test_topic_option(self):
        """Test parsing with --topic option."""
        with mock.patch("sys.argv", ["ll_trigger_counter", "--topic", "custom_topic"]):
            args = ll_trigger_counter.parse_command_line()
            assert args.topic == "custom_topic"

    def test_output_period_option(self):
        """Test parsing with --output-period option."""
        with mock.patch("sys.argv", ["ll_trigger_counter", "--output-period", "1800"]):
            args = ll_trigger_counter.parse_command_line()
            assert args.output_period == 1800.0

    def test_kafka_server_option(self):
        """Test parsing with --kafka-server option."""
        with mock.patch(
            "sys.argv", ["ll_trigger_counter", "--kafka-server", "localhost:9092"]
        ):
            args = ll_trigger_counter.parse_command_line()
            assert args.kafka_server == "localhost:9092"

    def test_output_option(self):
        """Test parsing with --output option."""
        with mock.patch(
            "sys.argv", ["ll_trigger_counter", "--output", "custom_output.xml.gz"]
        ):
            args = ll_trigger_counter.parse_command_line()
            assert args.output == "custom_output.xml.gz"

    def test_tag_option(self):
        """Test parsing with --tag option."""
        with mock.patch("sys.argv", ["ll_trigger_counter", "--tag", "prod"]):
            args = ll_trigger_counter.parse_command_line()
            assert args.tag == "prod"

    def test_verbose_flag(self):
        """Test parsing with -v flag."""
        with mock.patch("sys.argv", ["ll_trigger_counter", "-v"]):
            args = ll_trigger_counter.parse_command_line()
            assert args.verbose is True

    def test_verbose_long_flag(self):
        """Test parsing with --verbose flag."""
        with mock.patch("sys.argv", ["ll_trigger_counter", "--verbose"]):
            args = ll_trigger_counter.parse_command_line()
            assert args.verbose is True


class TestZeroLagCounts:
    """Tests for ZeroLagCounts class."""

    @pytest.fixture
    def mock_dependencies(self):
        """Set up common mocks for ZeroLagCounts tests."""
        with (
            mock.patch("sgnl.bin.ll_trigger_counter.events.EventProcessor.__init__"),
            mock.patch("sgnl.bin.ll_trigger_counter.os.access", return_value=True),
            mock.patch(
                "sgnl.bin.ll_trigger_counter.ligolw_utils.local_path_from_url",
                return_value="/path/to/file",
            ),
            mock.patch("sgnl.bin.ll_trigger_counter.far.RankingStatPDF") as mock_far,
            mock.patch(
                "sgnl.bin.ll_trigger_counter.HTTPControl.exchange_state"
            ) as mock_exchange,
            mock.patch("sgnl.bin.ll_trigger_counter.xml_string", return_value="<xml/>"),
        ):
            mock_rstat = mock.MagicMock()
            mock_far.load.return_value = mock_rstat

            yield {
                "mock_far": mock_far,
                "mock_rstat": mock_rstat,
                "mock_exchange": mock_exchange,
            }

    def test_init_with_defaults(self, mock_dependencies):
        """Test ZeroLagCounts initialization with default parameters."""
        mock_logger = mock.MagicMock()
        mock_sgn_control = mock.MagicMock()

        zlc = ll_trigger_counter.ZeroLagCounts(
            kafka_server="localhost:9092",
            logger=mock_logger,
            sgn_control=mock_sgn_control,
        )

        assert zlc.process_params == {}
        assert zlc.fname == "zerolag_rankingstatpdf.xml.gz"
        assert zlc.verbose is False
        assert zlc.output_period == 3600
        assert isinstance(zlc.counts_dict, dict)
        assert isinstance(zlc.msgs, deque)

    def test_init_with_process_params(self, mock_dependencies):
        """Test ZeroLagCounts initialization with process params."""
        mock_logger = mock.MagicMock()
        mock_sgn_control = mock.MagicMock()
        process_params = {"key": "value"}

        zlc = ll_trigger_counter.ZeroLagCounts(
            kafka_server="localhost:9092",
            logger=mock_logger,
            sgn_control=mock_sgn_control,
            process_params=process_params,
        )

        assert zlc.process_params == {"key": "value"}

    def test_init_file_not_accessible(self):
        """Test ZeroLagCounts raises error when file not accessible."""
        with (
            mock.patch("sgnl.bin.ll_trigger_counter.events.EventProcessor.__init__"),
            mock.patch("sgnl.bin.ll_trigger_counter.os.access", return_value=False),
            mock.patch(
                "sgnl.bin.ll_trigger_counter.ligolw_utils.local_path_from_url",
                return_value="/path/to/file",
            ),
        ):
            mock_logger = mock.MagicMock()
            mock_sgn_control = mock.MagicMock()

            with pytest.raises(ValueError) as exc_info:
                ll_trigger_counter.ZeroLagCounts(
                    kafka_server="localhost:9092",
                    logger=mock_logger,
                    sgn_control=mock_sgn_control,
                )

            assert "zerolag ranking stat PDF does not exist" in str(exc_info.value)

    def test_ingest_message_with_key(self, mock_dependencies):
        """Test ingest method with message that has a key."""
        mock_logger = mock.MagicMock()
        mock_sgn_control = mock.MagicMock()

        zlc = ll_trigger_counter.ZeroLagCounts(
            kafka_server="localhost:9092",
            logger=mock_logger,
            sgn_control=mock_sgn_control,
        )

        mock_message = mock.MagicMock()
        mock_message.key.return_value = b"key1.key2"
        mock_message.topic.return_value = "test_topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 100
        mock_message.value.return_value = json.dumps({"data": "value"}).encode("utf-8")
        mock_message.timestamp.return_value = (1234567890, 0)

        zlc.ingest(mock_message)

        assert len(zlc.msgs) == 1
        msg = zlc.msgs[0]
        assert msg.key == ("key1", "key2")
        assert msg.value == {"data": "value"}

    def test_ingest_message_without_key(self, mock_dependencies):
        """Test ingest method with message that has no key."""
        mock_logger = mock.MagicMock()
        mock_sgn_control = mock.MagicMock()

        zlc = ll_trigger_counter.ZeroLagCounts(
            kafka_server="localhost:9092",
            logger=mock_logger,
            sgn_control=mock_sgn_control,
        )

        mock_message = mock.MagicMock()
        mock_message.key.return_value = None
        mock_message.topic.return_value = "test_topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 100
        mock_message.value.return_value = json.dumps({"data": "value"}).encode("utf-8")
        mock_message.timestamp.return_value = (1234567890, 0)

        zlc.ingest(mock_message)

        assert len(zlc.msgs) == 1
        msg = zlc.msgs[0]
        assert msg.key == ()

    def test_handle_with_triggers(self, mock_dependencies):
        """Test handle method with triggers."""
        mock_logger = mock.MagicMock()
        mock_sgn_control = mock.MagicMock()

        zlc = ll_trigger_counter.ZeroLagCounts(
            kafka_server="localhost:9092",
            logger=mock_logger,
            sgn_control=mock_sgn_control,
        )

        # Add some messages
        zlc.msgs.append(mock.MagicMock())

        with mock.patch(
            "sgnl.bin.ll_trigger_counter.aggregator.parse_triggers"
        ) as mock_parse:
            mock_parse.return_value = [{"end": 1000000000.0, "likelihood": 10.5}]

            zlc.handle()

            mock_parse.assert_called_once()
            assert len(zlc.msgs) == 0  # msgs should be cleared

    def test_handle_without_triggers(self, mock_dependencies):
        """Test handle method without triggers."""
        mock_logger = mock.MagicMock()
        mock_sgn_control = mock.MagicMock()

        zlc = ll_trigger_counter.ZeroLagCounts(
            kafka_server="localhost:9092",
            logger=mock_logger,
            sgn_control=mock_sgn_control,
        )

        with mock.patch(
            "sgnl.bin.ll_trigger_counter.aggregator.parse_triggers"
        ) as mock_parse:
            mock_parse.return_value = []

            zlc.handle()

            mock_logger.debug.assert_called_with("no triggers to process")

    def test_handle_triggers_snapshot(self, mock_dependencies):
        """Test handle method triggers snapshot after output_period."""
        mock_logger = mock.MagicMock()
        mock_sgn_control = mock.MagicMock()

        zlc = ll_trigger_counter.ZeroLagCounts(
            kafka_server="localhost:9092",
            logger=mock_logger,
            sgn_control=mock_sgn_control,
            output_period=0,  # Set to 0 to trigger immediate snapshot
        )

        # Set time_since_last to a time in the past
        zlc.time_since_last = 0

        with (
            mock.patch(
                "sgnl.bin.ll_trigger_counter.aggregator.parse_triggers"
            ) as mock_parse,
            mock.patch.object(zlc, "snapshot_output_url") as mock_snapshot,
        ):
            mock_parse.return_value = []

            zlc.handle()

            mock_snapshot.assert_called_once()

    def test_add_coincs_with_likelihood(self, mock_dependencies):
        """Test add_coincs method with coincs that have likelihood."""
        mock_logger = mock.MagicMock()
        mock_sgn_control = mock.MagicMock()

        zlc = ll_trigger_counter.ZeroLagCounts(
            kafka_server="localhost:9092",
            logger=mock_logger,
            sgn_control=mock_sgn_control,
        )

        coincs = [
            {"end": 1000000005.0, "likelihood": 10.5},
            {"end": 1000000007.0, "likelihood": 15.5},  # Same 10s window, higher LR
            {"end": 1000000015.0, "likelihood": 5.0},  # Different 10s window
        ]

        count = zlc.add_coincs(coincs)

        assert count == 3
        # First two should be in same key (1000000010), max LR should be 15.5
        assert zlc.counts_dict[1000000010.0] == 15.5
        # Third should be in different key
        assert zlc.counts_dict[1000000020.0] == 5.0

    def test_add_coincs_without_likelihood(self, mock_dependencies):
        """Test add_coincs method with coincs that have no likelihood."""
        mock_logger = mock.MagicMock()
        mock_sgn_control = mock.MagicMock()

        zlc = ll_trigger_counter.ZeroLagCounts(
            kafka_server="localhost:9092",
            logger=mock_logger,
            sgn_control=mock_sgn_control,
        )

        coincs = [
            {"end": 1000000005.0},  # No likelihood
            {"end": 1000000015.0, "likelihood": 5.0},
        ]

        count = zlc.add_coincs(coincs)

        assert count == 1
        assert len(zlc.counts_dict) == 1

    def test_add_counts_to_zerolag_pdf(self, mock_dependencies):
        """Test __add_counts_to_zerolag_pdf method."""
        mock_logger = mock.MagicMock()
        mock_sgn_control = mock.MagicMock()

        zlc = ll_trigger_counter.ZeroLagCounts(
            kafka_server="localhost:9092",
            logger=mock_logger,
            sgn_control=mock_sgn_control,
        )

        # Add some counts
        zlc.counts_dict = {1000000010.0: 10.5, 1000000020.0: 15.5}

        mock_zlpdf = mock.MagicMock()

        # Call the private method
        zlc._ZeroLagCounts__add_counts_to_zerolag_pdf(mock_zlpdf)

        # Verify count was accessed for each LR value
        assert mock_zlpdf.zero_lag_lr_lnpdf.count.__getitem__.call_count == 2

    def test_snapshot_output_url(self, mock_dependencies):
        """Test snapshot_output_url method."""
        mock_logger = mock.MagicMock()
        mock_sgn_control = mock.MagicMock()
        mock_rstat = mock_dependencies["mock_rstat"]

        zlc = ll_trigger_counter.ZeroLagCounts(
            kafka_server="localhost:9092",
            logger=mock_logger,
            sgn_control=mock_sgn_control,
        )

        mock_copy = mock.MagicMock()
        mock_rstat.copy.return_value = mock_copy

        with (
            mock.patch("sgnl.bin.ll_trigger_counter.HTTPControl.exchange_state"),
            mock.patch("sgnl.bin.ll_trigger_counter.time_now", return_value=1000000000),
            mock.patch(
                "sgnl.bin.ll_trigger_counter.CacheEntry.from_T050017"
            ) as mock_cache,
            mock.patch("sgnl.bin.ll_trigger_counter.os.path.exists", return_value=True),
            mock.patch(
                "sgnl.bin.ll_trigger_counter.T050017_filename",
                return_value="backup.xml.gz",
            ),
            mock.patch("sgnl.bin.ll_trigger_counter.xml_string", return_value="<xml/>"),
        ):
            mock_entry = mock.MagicMock()
            mock_entry.observatory = "H1"
            mock_entry.description = "test"
            mock_cache.return_value = mock_entry

            zlc.snapshot_output_url()

            mock_rstat.copy.assert_called_once()
            assert mock_copy.save.call_count == 2  # Once for main, once for backup

    def test_snapshot_output_url_creates_backup_dir(self, mock_dependencies):
        """Test snapshot_output_url creates backup directory if not exists."""
        mock_logger = mock.MagicMock()
        mock_sgn_control = mock.MagicMock()
        mock_rstat = mock_dependencies["mock_rstat"]

        zlc = ll_trigger_counter.ZeroLagCounts(
            kafka_server="localhost:9092",
            logger=mock_logger,
            sgn_control=mock_sgn_control,
        )

        mock_copy = mock.MagicMock()
        mock_rstat.copy.return_value = mock_copy

        with (
            mock.patch("sgnl.bin.ll_trigger_counter.HTTPControl.exchange_state"),
            mock.patch("sgnl.bin.ll_trigger_counter.time_now", return_value=1000000000),
            mock.patch(
                "sgnl.bin.ll_trigger_counter.CacheEntry.from_T050017"
            ) as mock_cache,
            mock.patch(
                "sgnl.bin.ll_trigger_counter.os.path.exists", return_value=False
            ),
            mock.patch("sgnl.bin.ll_trigger_counter.os.makedirs") as mock_makedirs,
            mock.patch(
                "sgnl.bin.ll_trigger_counter.T050017_filename",
                return_value="backup.xml.gz",
            ),
            mock.patch("sgnl.bin.ll_trigger_counter.xml_string", return_value="<xml/>"),
        ):
            mock_entry = mock.MagicMock()
            mock_entry.observatory = "H1"
            mock_entry.description = "test"
            mock_cache.return_value = mock_entry

            zlc.snapshot_output_url()

            mock_makedirs.assert_called_once()


class TestMain:
    """Tests for main function."""

    def test_main_setup(self):
        """Test main function setup."""
        with (
            mock.patch(
                "sys.argv",
                [
                    "ll_trigger_counter",
                    "--kafka-server",
                    "localhost:9092",
                    "--output",
                    "test_output.xml.gz",
                ],
            ),
            mock.patch("sgnl.bin.ll_trigger_counter.HTTPControl") as mock_http_control,
            mock.patch("sgnl.bin.ll_trigger_counter.ZeroLagCounts") as mock_zlc_class,
            mock.patch("sgnl.bin.ll_trigger_counter.Queue"),
        ):
            mock_zlc = mock.MagicMock()
            mock_zlc_class.return_value = mock_zlc

            mock_context = mock.MagicMock()
            mock_http_control.return_value.__enter__ = mock.MagicMock(
                return_value=mock_context
            )
            mock_http_control.return_value.__exit__ = mock.MagicMock(return_value=False)

            ll_trigger_counter.main()

            mock_zlc_class.assert_called_once()
            mock_zlc.start.assert_called_once()

    def test_main_verbose(self):
        """Test main function with verbose flag."""
        with (
            mock.patch(
                "sys.argv",
                [
                    "ll_trigger_counter",
                    "--kafka-server",
                    "localhost:9092",
                    "-v",
                ],
            ),
            mock.patch("sgnl.bin.ll_trigger_counter.HTTPControl") as mock_http_control,
            mock.patch("sgnl.bin.ll_trigger_counter.ZeroLagCounts") as mock_zlc_class,
            mock.patch("sgnl.bin.ll_trigger_counter.Queue"),
        ):
            mock_zlc = mock.MagicMock()
            mock_zlc_class.return_value = mock_zlc

            mock_context = mock.MagicMock()
            mock_http_control.return_value.__enter__ = mock.MagicMock(
                return_value=mock_context
            )
            mock_http_control.return_value.__exit__ = mock.MagicMock(return_value=False)

            ll_trigger_counter.main()

            # Verify verbose was passed to ZeroLagCounts
            call_kwargs = mock_zlc_class.call_args[1]
            assert call_kwargs["verbose"] is True

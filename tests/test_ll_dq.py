"""Tests for sgnl.bin.ll_dq"""

import sys
from unittest import mock

import pytest


@pytest.fixture(autouse=True)
def mock_dependencies():
    """Mock external dependencies and clean up after tests."""
    original_modules = {}

    modules_to_mock = [
        "sgn",
        "sgn.apps",
        "sgn.base",
        "sgnligo",
        "sgnligo.sinks",
        "sgnligo.sources",
        "sgnligo.transforms",
        "sgnts",
        "sgnts.sinks",
        "strike",
        "strike.config",
        "sgnl.psd",
        "sgnl.transforms",
    ]

    for mod in modules_to_mock:
        original_modules[mod] = sys.modules.get(mod)
        sys.modules[mod] = mock.MagicMock()

    yield

    # Restore originals
    for mod in modules_to_mock:
        if original_modules[mod] is None:
            sys.modules.pop(mod, None)
        else:
            sys.modules[mod] = original_modules[mod]

    # Clear cached imports
    sys.modules.pop("sgnl.bin.ll_dq", None)


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_required_args(self):
        """Test parsing with minimal required arguments."""
        from sgnl.bin import ll_dq

        with mock.patch("sys.argv", ["ll_dq"]):
            options = ll_dq.parse_command_line()
            # Check defaults
            assert options.analysis_tag == "test"
            assert options.horizon_approximant == "IMRPhenomD"
            assert options.horizon_f_min == 15.0
            assert options.horizon_f_max == 900.0
            assert options.injections is False
            assert options.search is None
            assert options.verbose is False

    def test_output_kafka_server_option(self):
        """Test --output-kafka-server option."""
        from sgnl.bin import ll_dq

        with mock.patch("sys.argv", ["ll_dq", "--output-kafka-server", "kafka:9092"]):
            options = ll_dq.parse_command_line()
            assert options.output_kafka_server == "kafka:9092"

    def test_analysis_tag_option(self):
        """Test --analysis-tag option."""
        from sgnl.bin import ll_dq

        with mock.patch("sys.argv", ["ll_dq", "--analysis-tag", "production"]):
            options = ll_dq.parse_command_line()
            assert options.analysis_tag == "production"

    def test_horizon_approximant_option(self):
        """Test --horizon-approximant option."""
        from sgnl.bin import ll_dq

        with mock.patch("sys.argv", ["ll_dq", "--horizon-approximant", "TaylorF2"]):
            options = ll_dq.parse_command_line()
            assert options.horizon_approximant == "TaylorF2"

    def test_horizon_f_min_option(self):
        """Test --horizon-f-min option."""
        from sgnl.bin import ll_dq

        with mock.patch("sys.argv", ["ll_dq", "--horizon-f-min", "20.0"]):
            options = ll_dq.parse_command_line()
            assert options.horizon_f_min == 20.0

    def test_horizon_f_max_option(self):
        """Test --horizon-f-max option."""
        from sgnl.bin import ll_dq

        with mock.patch("sys.argv", ["ll_dq", "--horizon-f-max", "1024.0"]):
            options = ll_dq.parse_command_line()
            assert options.horizon_f_max == 1024.0

    def test_injections_flag(self):
        """Test --injections flag."""
        from sgnl.bin import ll_dq

        with mock.patch("sys.argv", ["ll_dq", "--injections"]):
            options = ll_dq.parse_command_line()
            assert options.injections is True

    def test_search_ew_option(self):
        """Test --search ew option."""
        from sgnl.bin import ll_dq

        with mock.patch("sys.argv", ["ll_dq", "--search", "ew"]):
            options = ll_dq.parse_command_line()
            assert options.search == "ew"

    def test_verbose_flag(self):
        """Test --verbose flag."""
        from sgnl.bin import ll_dq

        with mock.patch("sys.argv", ["ll_dq", "--verbose"]):
            options = ll_dq.parse_command_line()
            assert options.verbose is True

    def test_verbose_short_flag(self):
        """Test -v flag."""
        from sgnl.bin import ll_dq

        with mock.patch("sys.argv", ["ll_dq", "-v"]):
            options = ll_dq.parse_command_line()
            assert options.verbose is True


class TestLlDq:
    """Tests for ll_dq function."""

    def test_ll_dq_basic(self):
        """Test ll_dq function with basic inputs."""
        from sgnl.bin import ll_dq

        # Create mock data_source_info
        mock_data_source_info = mock.MagicMock()
        mock_data_source_info.ifos = ["H1"]
        mock_data_source_info.data_source = "devshm"
        mock_data_source_info.input_sample_rate = 16384

        # Create mock condition_info
        mock_condition_info = mock.MagicMock()

        # Mock the pipeline and other components
        mock_pipeline = mock.MagicMock()
        ll_dq.Pipeline.return_value = mock_pipeline

        # Mock datasource and condition returns
        ll_dq.datasource.return_value = ({"H1": "source_link"}, None)
        ll_dq.condition.return_value = (
            {"H1": "condition_link"},
            {"H1": "spectrum_link"},
            None,
        )

        ll_dq.ll_dq(
            data_source_info=mock_data_source_info,
            condition_info=mock_condition_info,
            output_kafka_server="kafka:9092",
            analysis_tag="test",
            horizon_approximant="IMRPhenomD",
            horizon_f_min=15.0,
            horizon_f_max=900.0,
            injections=False,
            highpass_filter=10.0,
            verbose=False,
        )

        # Verify Pipeline was created and run
        ll_dq.Pipeline.assert_called_once()
        mock_pipeline.run.assert_called_once()

        # Verify datasource was called
        ll_dq.datasource.assert_called_once()

        # Verify condition was called
        ll_dq.condition.assert_called_once()

        # Verify insert was called (at least twice - once for nodes, once for links)
        assert mock_pipeline.insert.call_count >= 2

    def test_ll_dq_with_injections(self):
        """Test ll_dq function with injections enabled."""
        from sgnl.bin import ll_dq

        mock_data_source_info = mock.MagicMock()
        mock_data_source_info.ifos = ["L1"]
        mock_data_source_info.data_source = "devshm"
        mock_data_source_info.input_sample_rate = 16384

        mock_condition_info = mock.MagicMock()

        mock_pipeline = mock.MagicMock()
        ll_dq.Pipeline.return_value = mock_pipeline

        ll_dq.datasource.return_value = ({"L1": "source_link"}, None)
        ll_dq.condition.return_value = (
            {"L1": "condition_link"},
            {"L1": "spectrum_link"},
            None,
        )

        # Reset the mock to clear any previous calls
        ll_dq.KafkaSink.reset_mock()

        ll_dq.ll_dq(
            data_source_info=mock_data_source_info,
            condition_info=mock_condition_info,
            output_kafka_server="kafka:9092",
            analysis_tag="prod",
            horizon_approximant="IMRPhenomD",
            horizon_f_min=15.0,
            horizon_f_max=900.0,
            injections=True,
            highpass_filter=10.0,
            verbose=True,
        )

        # Verify KafkaSink was created with injection prefix
        kafka_sink_call = ll_dq.KafkaSink.call_args
        assert kafka_sink_call is not None
        # The prefix should contain "inj_" when injections=True
        assert "inj_" in kafka_sink_call[1]["prefix"]

    def test_ll_dq_multiple_ifos_raises(self):
        """Test ll_dq raises ValueError with multiple ifos."""
        from sgnl.bin import ll_dq

        mock_data_source_info = mock.MagicMock()
        mock_data_source_info.ifos = ["H1", "L1"]  # Multiple ifos

        mock_condition_info = mock.MagicMock()

        with pytest.raises(ValueError, match="Only supports one ifo"):
            ll_dq.ll_dq(
                data_source_info=mock_data_source_info,
                condition_info=mock_condition_info,
                output_kafka_server="kafka:9092",
                analysis_tag="test",
                horizon_approximant="IMRPhenomD",
                horizon_f_min=15.0,
                horizon_f_max=900.0,
                injections=False,
                highpass_filter=10.0,
                verbose=False,
            )


class TestMain:
    """Tests for main function."""

    def test_main_default_config(self):
        """Test main function with default config (no search)."""
        from sgnl.bin import ll_dq

        mock_data_source_info = mock.MagicMock()
        mock_data_source_info.ifos = ["H1"]
        mock_data_source_info.data_source = "devshm"
        mock_data_source_info.input_sample_rate = 16384

        mock_condition_info = mock.MagicMock()

        ll_dq.DataSourceInfo.from_options.return_value = mock_data_source_info
        ll_dq.ConditionInfo.from_options.return_value = mock_condition_info

        # Setup config mock
        mock_config = {
            "default": {"highpass_filter": 10.0},
            "ew": {"highpass_filter": 5.0},
        }
        ll_dq.get_analysis_config.return_value = mock_config

        mock_pipeline = mock.MagicMock()
        ll_dq.Pipeline.return_value = mock_pipeline

        ll_dq.datasource.return_value = ({"H1": "source_link"}, None)
        ll_dq.condition.return_value = (
            {"H1": "condition_link"},
            {"H1": "spectrum_link"},
            None,
        )

        with mock.patch("sys.argv", ["ll_dq"]):
            ll_dq.main()

        # Verify get_analysis_config was called
        ll_dq.get_analysis_config.assert_called_once()

        # Verify pipeline was run
        mock_pipeline.run.assert_called_once()

    def test_main_with_search_ew(self):
        """Test main function with ew search option."""
        from sgnl.bin import ll_dq

        mock_data_source_info = mock.MagicMock()
        mock_data_source_info.ifos = ["H1"]
        mock_data_source_info.data_source = "devshm"
        mock_data_source_info.input_sample_rate = 16384

        mock_condition_info = mock.MagicMock()

        ll_dq.DataSourceInfo.from_options.return_value = mock_data_source_info
        ll_dq.ConditionInfo.from_options.return_value = mock_condition_info

        # Setup config mock with ew-specific settings
        mock_config = {
            "default": {"highpass_filter": 10.0},
            "ew": {"highpass_filter": 5.0},
        }
        ll_dq.get_analysis_config.return_value = mock_config

        mock_pipeline = mock.MagicMock()
        ll_dq.Pipeline.return_value = mock_pipeline

        ll_dq.datasource.return_value = ({"H1": "source_link"}, None)
        ll_dq.condition.return_value = (
            {"H1": "condition_link"},
            {"H1": "spectrum_link"},
            None,
        )

        with mock.patch("sys.argv", ["ll_dq", "--search", "ew"]):
            ll_dq.main()

        # Verify pipeline was run
        mock_pipeline.run.assert_called_once()

    def test_main_with_all_options(self):
        """Test main function with all options specified."""
        from sgnl.bin import ll_dq

        mock_data_source_info = mock.MagicMock()
        mock_data_source_info.ifos = ["L1"]
        mock_data_source_info.data_source = "devshm"
        mock_data_source_info.input_sample_rate = 16384

        mock_condition_info = mock.MagicMock()

        ll_dq.DataSourceInfo.from_options.return_value = mock_data_source_info
        ll_dq.ConditionInfo.from_options.return_value = mock_condition_info

        mock_config = {
            "default": {"highpass_filter": 10.0},
            "ew": {"highpass_filter": 5.0},
        }
        ll_dq.get_analysis_config.return_value = mock_config

        mock_pipeline = mock.MagicMock()
        ll_dq.Pipeline.return_value = mock_pipeline

        ll_dq.datasource.return_value = ({"L1": "source_link"}, None)
        ll_dq.condition.return_value = (
            {"L1": "condition_link"},
            {"L1": "spectrum_link"},
            None,
        )

        with mock.patch(
            "sys.argv",
            [
                "ll_dq",
                "--output-kafka-server",
                "kafka:9092",
                "--analysis-tag",
                "production",
                "--horizon-approximant",
                "TaylorF2",
                "--horizon-f-min",
                "20.0",
                "--horizon-f-max",
                "1024.0",
                "--injections",
                "-v",
            ],
        ):
            ll_dq.main()

        # Verify pipeline was run
        mock_pipeline.run.assert_called_once()

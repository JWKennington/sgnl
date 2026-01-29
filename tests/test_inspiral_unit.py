"""Unit tests for sgnl.bin.inspiral with mocked dependencies."""

import sys
from unittest import mock

import pytest


@pytest.fixture(autouse=True)
def mock_dependencies():
    """Mock external dependencies and clean up after tests."""
    # Clear any cached inspiral import first
    sys.modules.pop("sgnl.bin.inspiral", None)

    original_modules = {}

    # Create structured mocks for complex dependencies
    torch_mock = mock.MagicMock()
    torch_mock.float64 = "float64"
    torch_mock.float32 = "float32"
    torch_mock.float16 = "float16"

    igwn_ligolw_mock = mock.MagicMock()
    igwn_ligolw_utils_mock = mock.MagicMock()

    sgn_apps_mock = mock.MagicMock()
    sgn_control_mock = mock.MagicMock()
    sgn_sinks_mock = mock.MagicMock()

    sgnligo_sources_mock = mock.MagicMock()
    sgnligo_transforms_mock = mock.MagicMock()
    sgnligo_sinks_mock = mock.MagicMock()

    sgnts_sinks_mock = mock.MagicMock()

    strike_config_mock = mock.MagicMock()
    strike_config_mock.get_analysis_config.return_value = {
        "default": {
            "chi2_over_snr2_min": 0.0,
            "chi2_over_snr2_max": 1.0,
            "chi_bin_min": 0,
            "chi_bin_max": 10,
            "chi_bin_num": 10,
            "highpass_filter": 15.0,
        },
        "ew": {
            "chi2_over_snr2_min": 0.0,
            "chi2_over_snr2_max": 1.0,
            "chi_bin_min": 0,
            "chi_bin_max": 10,
            "chi_bin_num": 10,
            "highpass_filter": 15.0,
        },
    }

    modules_to_mock = {
        "torch": torch_mock,
        "igwn_ligolw": igwn_ligolw_mock,
        "igwn_ligolw.ligolw": mock.MagicMock(),
        "igwn_ligolw.lsctables": mock.MagicMock(),
        "igwn_ligolw.utils": igwn_ligolw_utils_mock,
        "igwn_ligolw.array": mock.MagicMock(),
        "igwn_ligolw.param": mock.MagicMock(),
        "sgn": mock.MagicMock(),
        "sgn.apps": sgn_apps_mock,
        "sgn.control": sgn_control_mock,
        "sgn.sinks": sgn_sinks_mock,
        "sgnligo": mock.MagicMock(),
        "sgnligo.sources": sgnligo_sources_mock,
        "sgnligo.transforms": sgnligo_transforms_mock,
        "sgnligo.sinks": sgnligo_sinks_mock,
        "sgnts": mock.MagicMock(),
        "sgnts.sinks": sgnts_sinks_mock,
        "strike": mock.MagicMock(),
        "strike.config": strike_config_mock,
        "sgnl.simulation": mock.MagicMock(),
        "sgnl.control": mock.MagicMock(),
        "sgnl.sinks": mock.MagicMock(),
        "sgnl.sort_bank": mock.MagicMock(),
        "sgnl.strike_object": mock.MagicMock(),
        "sgnl.transforms": mock.MagicMock(),
    }

    # Store original sgnl module reference
    sgnl_mod = sys.modules.get("sgnl")

    for mod, mock_obj in modules_to_mock.items():
        original_modules[mod] = sys.modules.get(mod)
        sys.modules[mod] = mock_obj

    # Patch sgnl module attributes if it exists
    if sgnl_mod is not None:
        for submod in [
            "simulation",
            "control",
            "sinks",
            "sort_bank",
            "strike_object",
            "transforms",
        ]:
            if f"sgnl.{submod}" in modules_to_mock:
                setattr(sgnl_mod, submod, modules_to_mock[f"sgnl.{submod}"])

    yield

    # Restore originals
    for mod in modules_to_mock:
        if original_modules[mod] is None:
            sys.modules.pop(mod, None)
        else:
            sys.modules[mod] = original_modules[mod]

    # Clear the cached import
    sys.modules.pop("sgnl.bin.inspiral", None)


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_required_svd_bank_option(self):
        """Test that --svd-bank is required."""
        from sgnl.bin import inspiral

        # Mock DataSourceInfo and ConditionInfo append_options
        inspiral.DataSourceInfo.append_options = mock.MagicMock()
        inspiral.ConditionInfo.append_options = mock.MagicMock()

        with mock.patch("sys.argv", ["inspiral"]):
            with pytest.raises(SystemExit):
                inspiral.parse_command_line()

    def test_basic_options(self):
        """Test parsing with basic options."""
        from sgnl.bin import inspiral

        inspiral.DataSourceInfo.append_options = mock.MagicMock()
        inspiral.ConditionInfo.append_options = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "inspiral",
                "--svd-bank",
                "bank.xml",
            ],
        ):
            options = inspiral.parse_command_line()
            assert options.svd_bank == ["bank.xml"]
            assert options.trigger_finding_duration == 1
            assert options.snr_min == 4
            assert options.coincidence_threshold == 0.005

    def test_multiple_svd_banks(self):
        """Test parsing with multiple SVD banks."""
        from sgnl.bin import inspiral

        inspiral.DataSourceInfo.append_options = mock.MagicMock()
        inspiral.ConditionInfo.append_options = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "inspiral",
                "--svd-bank",
                "bank1.xml",
                "--svd-bank",
                "bank2.xml",
            ],
        ):
            options = inspiral.parse_command_line()
            assert options.svd_bank == ["bank1.xml", "bank2.xml"]

    def test_trigger_generator_options(self):
        """Test trigger generator options."""
        from sgnl.bin import inspiral

        inspiral.DataSourceInfo.append_options = mock.MagicMock()
        inspiral.ConditionInfo.append_options = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "inspiral",
                "--svd-bank",
                "bank.xml",
                "--trigger-finding-duration",
                "2.0",
                "--snr-min",
                "5.0",
                "--coincidence-threshold",
                "0.01",
                "--event-config",
                "config.yaml",
                "--trigger-output",
                "output.sqlite",
                "--snr-timeseries-output",
                "snr.h5",
            ],
        ):
            options = inspiral.parse_command_line()
            assert options.trigger_finding_duration == 2.0
            assert options.snr_min == 5.0
            assert options.coincidence_threshold == 0.01
            assert options.event_config == "config.yaml"
            assert options.trigger_output == ["output.sqlite"]
            assert options.snr_timeseries_output == "snr.h5"

    def test_impulse_options(self):
        """Test impulse test options."""
        from sgnl.bin import inspiral

        inspiral.DataSourceInfo.append_options = mock.MagicMock()
        inspiral.ConditionInfo.append_options = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "inspiral",
                "--svd-bank",
                "bank.xml",
                "--impulse-bank",
                "templates.xml",
                "--impulse-bankno",
                "0",
                "--impulse-ifo",
                "H1",
            ],
        ):
            options = inspiral.parse_command_line()
            assert options.impulse_bank == "templates.xml"
            assert options.impulse_bankno == 0
            assert options.impulse_ifo == "H1"

    def test_ranking_statistic_options(self):
        """Test ranking statistic options."""
        from sgnl.bin import inspiral

        inspiral.DataSourceInfo.append_options = mock.MagicMock()
        inspiral.ConditionInfo.append_options = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "inspiral",
                "--svd-bank",
                "bank.xml",
                "--search",
                "ew",
                "--snapshot-interval",
                "7200",
                "--snapshot-multiprocess",
                "--far-trials-factor",
                "2.0",
                "--cap-singles",
                "--compress-likelihood-ratio",
                "--compress-likelihood-ratio-threshold",
                "0.05",
                "--all-triggers-to-background",
                "--min-instruments-candidates",
                "2",
            ],
        ):
            options = inspiral.parse_command_line()
            assert options.search == "ew"
            assert options.snapshot_interval == 7200
            assert options.snapshot_multiprocess is True
            assert options.far_trials_factor == 2.0
            assert options.cap_singles is True
            assert options.compress_likelihood_ratio is True
            assert options.compress_likelihood_ratio_threshold == 0.05
            assert options.all_triggers_to_background is True
            assert options.min_instruments_candidates == 2

    def test_likelihood_file_options(self):
        """Test likelihood file options."""
        from sgnl.bin import inspiral

        inspiral.DataSourceInfo.append_options = mock.MagicMock()
        inspiral.ConditionInfo.append_options = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "inspiral",
                "--svd-bank",
                "bank.xml",
                "--output-likelihood-file",
                "output_lr.h5",
                "--input-likelihood-file",
                "input_lr.h5",
                "--rank-stat-pdf-file",
                "pdf.h5",
                "--zerolag-rank-stat-pdf-file",
                "zerolag.h5",
            ],
        ):
            options = inspiral.parse_command_line()
            assert options.output_likelihood_file == ["output_lr.h5"]
            assert options.input_likelihood_file == ["input_lr.h5"]
            assert options.rank_stat_pdf_file == "pdf.h5"
            assert options.zerolag_rank_stat_pdf_file == ["zerolag.h5"]

    def test_gracedb_options(self):
        """Test GraceDB options."""
        from sgnl.bin import inspiral

        inspiral.DataSourceInfo.append_options = mock.MagicMock()
        inspiral.ConditionInfo.append_options = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "inspiral",
                "--svd-bank",
                "bank.xml",
                "--gracedb-far-threshold",
                "1e-6",
                "--gracedb-aggregator-far-threshold",
                "1e-7",
                "--gracedb-aggregator-far-trials-factor",
                "2",
                "--gracedb-group",
                "CBC",
                "--gracedb-pipeline",
                "gstlal",
                "--gracedb-search",
                "AllSky",
                "--gracedb-label",
                "INJ",
                "--gracedb-service-url",
                "https://gracedb.ligo.org/api/",
            ],
        ):
            options = inspiral.parse_command_line()
            assert options.gracedb_far_threshold == 1e-6
            assert options.gracedb_aggregator_far_threshold == 1e-7
            assert options.gracedb_aggregator_far_trials_factor == 2
            assert options.gracedb_group == "CBC"
            assert options.gracedb_pipeline == "gstlal"
            assert options.gracedb_search == "AllSky"
            assert options.gracedb_label == ["INJ"]
            assert options.gracedb_service_url == "https://gracedb.ligo.org/api/"

    def test_program_behaviour_options(self):
        """Test program behaviour options."""
        from sgnl.bin import inspiral

        inspiral.DataSourceInfo.append_options = mock.MagicMock()
        inspiral.ConditionInfo.append_options = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "inspiral",
                "--svd-bank",
                "bank.xml",
                "--torch-device",
                "cuda:0",
                "--torch-dtype",
                "float64",
                "--injections",
                "--injection-file",
                "injections.xml",
                "--reconstruct-inj-segments",
                "--verbose",
                "--analysis-tag",
                "test_analysis",
                "--job-tag",
                "0001",
                "--graph-name",
                "pipeline.png",
                "--fake-sink",
            ],
        ):
            options = inspiral.parse_command_line()
            assert options.torch_device == "cuda:0"
            assert options.torch_dtype == "float64"
            assert options.injections is True
            assert options.injection_file == "injections.xml"
            assert options.reconstruct_inj_segments is True
            assert options.verbose is True
            assert options.analysis_tag == "test_analysis"
            assert options.job_tag == "0001"
            assert options.graph_name == "pipeline.png"
            assert options.fake_sink is True

    def test_nsubbank_pretend_and_nslice(self):
        """Test nsubbank-pretend and nslice options."""
        from sgnl.bin import inspiral

        inspiral.DataSourceInfo.append_options = mock.MagicMock()
        inspiral.ConditionInfo.append_options = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "inspiral",
                "--svd-bank",
                "bank.xml",
                "--nsubbank-pretend",
                "5",
                "--nslice",
                "10",
            ],
        ):
            options = inspiral.parse_command_line()
            assert options.nsubbank_pretend == 5
            assert options.nslice == 10

    def test_default_values(self):
        """Test default option values."""
        from sgnl.bin import inspiral

        inspiral.DataSourceInfo.append_options = mock.MagicMock()
        inspiral.ConditionInfo.append_options = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "inspiral",
                "--svd-bank",
                "bank.xml",
            ],
        ):
            options = inspiral.parse_command_line()
            assert options.trigger_finding_duration == 1
            assert options.snr_min == 4
            assert options.coincidence_threshold == 0.005
            assert options.search is None
            assert options.snapshot_interval == 14400
            assert options.snapshot_multiprocess is False
            assert options.far_trials_factor == 1.0
            assert options.compress_likelihood_ratio is False
            assert options.compress_likelihood_ratio_threshold == 0.03
            assert options.all_triggers_to_background is False
            assert options.min_instruments_candidates == 1
            assert options.gracedb_far_threshold == -1
            assert options.gracedb_aggregator_far_threshold == 3.84e-07
            assert options.gracedb_aggregator_far_trials_factor == 1
            assert options.gracedb_group == "Test"
            assert options.gracedb_pipeline == "SGNL"
            assert options.gracedb_search == "MOCK"
            assert options.torch_device == "cpu"
            assert options.torch_dtype == "float32"
            assert options.injections is False
            assert options.verbose is False
            assert options.analysis_tag == "test"
            assert options.job_tag == ""
            assert options.fake_sink is False
            assert options.nsubbank_pretend == 0
            assert options.nslice == -1


class TestInspiralValidation:
    """Tests for inspiral function validation."""

    def _create_mock_data_source_info(self, data_source="frames", ifos=None):
        """Create a mock DataSourceInfo object."""
        if ifos is None:
            ifos = ["H1", "L1"]
        mock_info = mock.MagicMock()
        mock_info.data_source = data_source
        mock_info.ifos = ifos
        mock_info.all_analysis_ifos = ifos
        mock_info.seg = mock.MagicMock()
        mock_info.input_sample_rate = 16384
        mock_info.channel_dict = {ifo: f"{ifo}:GDS-CALIB_STRAIN" for ifo in ifos}
        return mock_info

    def _create_mock_condition_info(self):
        """Create a mock ConditionInfo object."""
        return mock.MagicMock()

    def test_snapshot_multiprocess_offline_error(self):
        """Test that snapshot_multiprocess raises error in offline mode."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        with pytest.raises(ValueError, match="snapshot_multiprocess is only allowed"):
            inspiral.inspiral(
                data_source_info=data_source_info,
                condition_info=condition_info,
                svd_bank=["bank.xml"],
                snapshot_multiprocess=True,
            )

    def test_trigger_output_exists_error(self):
        """Test that existing trigger output raises error."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        with mock.patch("os.path.exists", return_value=True):
            with pytest.raises(ValueError, match="output db exists"):
                inspiral.inspiral(
                    data_source_info=data_source_info,
                    condition_info=condition_info,
                    svd_bank=["bank.xml"],
                    trigger_output=["existing.sqlite"],
                    event_config="config.yaml",
                )

    def test_output_likelihood_exists_error(self):
        """Test that existing output likelihood file raises error."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        with mock.patch("os.path.exists", return_value=True):
            with pytest.raises(ValueError, match="ranking stat output exists"):
                inspiral.inspiral(
                    data_source_info=data_source_info,
                    condition_info=condition_info,
                    svd_bank=["bank.xml"],
                    output_likelihood_file=["existing_lr.h5"],
                )

    def test_impulse_missing_bank_error(self):
        """Test that impulse mode requires impulse_bank."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="impulse")
        condition_info = self._create_mock_condition_info()

        with pytest.raises(ValueError, match="Must specify impulse_bank"):
            inspiral.inspiral(
                data_source_info=data_source_info,
                condition_info=condition_info,
                svd_bank=["bank.xml"],
            )

    def test_impulse_missing_bankno_error(self):
        """Test that impulse mode requires impulse_bankno."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="impulse")
        condition_info = self._create_mock_condition_info()

        with pytest.raises(ValueError, match="Must specify impulse_bankno"):
            inspiral.inspiral(
                data_source_info=data_source_info,
                condition_info=condition_info,
                svd_bank=["bank.xml"],
                impulse_bank="templates.xml",
            )

    def test_impulse_missing_ifo_error(self):
        """Test that impulse mode requires impulse_ifo."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="impulse")
        condition_info = self._create_mock_condition_info()

        with pytest.raises(ValueError, match="Must specify impulse_ifo"):
            inspiral.inspiral(
                data_source_info=data_source_info,
                condition_info=condition_info,
                svd_bank=["bank.xml"],
                impulse_bank="templates.xml",
                impulse_bankno=0,
            )

    def test_min_instruments_greater_than_two_error(self):
        """Test that min_instruments_candidates > 2 raises error."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        with pytest.raises(ValueError, match="min_instruments_candidates > 2"):
            inspiral.inspiral(
                data_source_info=data_source_info,
                condition_info=condition_info,
                svd_bank=["bank.xml"],
                min_instruments_candidates=3,
            )

    def test_unknown_dtype_error(self):
        """Test that unknown torch_dtype raises error."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        with pytest.raises(ValueError, match="Unknown data type"):
            inspiral.inspiral(
                data_source_info=data_source_info,
                condition_info=condition_info,
                svd_bank=["bank.xml"],
                torch_dtype="float128",
            )

    def test_missing_trigger_output_error(self):
        """Test that missing trigger_output raises error for offline mode."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        with pytest.raises(ValueError, match="Must supply trigger_output"):
            inspiral.inspiral(
                data_source_info=data_source_info,
                condition_info=condition_info,
                svd_bank=["bank.xml"],
                fake_sink=False,
            )

    def test_missing_event_config_error(self):
        """Test that missing event_config raises error when trigger_output set."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        with mock.patch("os.path.exists", return_value=False):
            with pytest.raises(ValueError, match="Must supply event_config"):
                inspiral.inspiral(
                    data_source_info=data_source_info,
                    condition_info=condition_info,
                    svd_bank=["bank.xml"],
                    trigger_output=["output.sqlite"],
                )

    def test_injection_file_without_injections_error(self):
        """Test that injection_file without --injections raises error."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        with mock.patch("os.path.exists", return_value=False):
            with pytest.raises(
                ValueError, match="Must supply --injections when --injection-file"
            ):
                inspiral.inspiral(
                    data_source_info=data_source_info,
                    condition_info=condition_info,
                    svd_bank=["bank.xml"],
                    trigger_output=["output.sqlite"],
                    event_config="config.yaml",
                    injection_file="injections.xml",
                )

    def test_injections_without_injection_file_error(self):
        """Test that --injections without injection_file raises error for frames."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        with mock.patch("os.path.exists", return_value=False):
            with pytest.raises(ValueError, match="Must supply --injection-file"):
                inspiral.inspiral(
                    data_source_info=data_source_info,
                    condition_info=condition_info,
                    svd_bank=["bank.xml"],
                    trigger_output=["output.sqlite"],
                    event_config="config.yaml",
                    injections=True,
                )

    def test_output_likelihood_with_injections_error(self):
        """Test that output_likelihood_file with injections raises error."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        with mock.patch("os.path.exists", return_value=False):
            with pytest.raises(
                ValueError,
                match="Must not set --output-likelihood-file when --injections",
            ):
                inspiral.inspiral(
                    data_source_info=data_source_info,
                    condition_info=condition_info,
                    svd_bank=["bank.xml"],
                    trigger_output=["output.sqlite"],
                    event_config="config.yaml",
                    injections=True,
                    injection_file="injections.xml",
                    output_likelihood_file=["lr.h5"],
                )

    def test_kafka_server_offline_error(self):
        """Test that output_kafka_server in offline mode raises error."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        with mock.patch("os.path.exists", return_value=False):
            with pytest.raises(
                ValueError, match="output_kafka_server can only be set if in online"
            ):
                inspiral.inspiral(
                    data_source_info=data_source_info,
                    condition_info=condition_info,
                    svd_bank=["bank.xml"],
                    trigger_output=["output.sqlite"],
                    event_config="config.yaml",
                    output_kafka_server="localhost:9092",
                )

    def test_dtype_float64(self):
        """Test that float64 dtype is properly handled."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        # This should pass dtype validation but fail later
        with mock.patch("os.path.exists", return_value=False):
            with pytest.raises(ValueError, match="Must supply trigger_output"):
                inspiral.inspiral(
                    data_source_info=data_source_info,
                    condition_info=condition_info,
                    svd_bank=["bank.xml"],
                    torch_dtype="float64",
                )

    def test_dtype_float16(self):
        """Test that float16 dtype is properly handled."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        # This should pass dtype validation but fail later
        with mock.patch("os.path.exists", return_value=False):
            with pytest.raises(ValueError, match="Must supply trigger_output"):
                inspiral.inspiral(
                    data_source_info=data_source_info,
                    condition_info=condition_info,
                    svd_bank=["bank.xml"],
                    torch_dtype="float16",
                )

    def test_trigger_output_warning_online(self, capsys):
        """Test that trigger_output warning is logged in online mode."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="arrakis")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        trigger_output=["output.sqlite"],
                        event_config="config.yaml",
                        fake_sink=True,
                    )

        # The warning is logged, not printed to stderr
        # Just verify the function ran without error

    def _setup_pipeline_mocks(self, inspiral_module, ifos=None):
        """Setup mocks for pipeline components using mock.patch.

        Returns a context manager that patches all pipeline components.
        Use with 'with' statement to apply the patches.
        """
        from contextlib import ExitStack

        import numpy as np

        if ifos is None:
            ifos = ["H1", "L1"]

        # Create mock objects
        mock_bank = mock.MagicMock()
        mock_bank.sngl_inspiral_table = []
        mock_bank.template_bank_filename = "/mock/test_bank.xml"

        mock_sorted_bank = mock.MagicMock()
        mock_sorted_bank.bank_metadata = {"maxrate": 4096}
        mock_sorted_bank.template_ids = mock.MagicMock()
        mock_sorted_bank.template_ids.numpy.return_value = np.array([0, 1, 2])
        mock_sorted_bank.bankids_map = {"0": [0, 1, 2]}
        mock_sorted_bank.autocorrelation_banks = mock.MagicMock()
        mock_sorted_bank.autocorrelation_length_mask = mock.MagicMock()
        mock_sorted_bank.autocorrelation_lengths = mock.MagicMock()
        mock_sorted_bank.end_time_delta = mock.MagicMock()
        mock_sorted_bank.template_durations = mock.MagicMock()
        mock_sorted_bank.horizon_distance_funcs = {}
        mock_sorted_bank.sngls = {}
        mock_sorted_bank.subbankids = {}

        mock_pipeline = mock.MagicMock()
        mock_strike_object = mock.MagicMock()

        # Create ExitStack to manage multiple patches
        stack = ExitStack()

        # Patch all pipeline components on the inspiral module
        stack.enter_context(
            mock.patch.object(
                inspiral_module,
                "datasource",
                return_value=(
                    {ifo: f"{ifo}_link" for ifo in ifos},
                    {ifo: f"{ifo}_latency" for ifo in ifos},
                ),
            )
        )
        stack.enter_context(
            mock.patch.object(
                inspiral_module,
                "group_and_read_banks",
                return_value={ifo: [mock_bank] for ifo in ifos},
            )
        )
        stack.enter_context(
            mock.patch.object(
                inspiral_module, "SortedBank", return_value=mock_sorted_bank
            )
        )
        stack.enter_context(
            mock.patch.object(
                inspiral_module,
                "condition",
                return_value=(
                    {ifo: f"{ifo}_cond" for ifo in ifos},
                    {ifo: f"{ifo}_spec" for ifo in ifos},
                    {ifo: f"{ifo}_whiten_lat" for ifo in ifos},
                ),
            )
        )
        stack.enter_context(
            mock.patch.object(
                inspiral_module,
                "lloid",
                return_value={ifo: f"{ifo}_lloid" for ifo in ifos},
            )
        )
        stack.enter_context(
            mock.patch.object(
                inspiral_module, "StrikeObject", return_value=mock_strike_object
            )
        )
        stack.enter_context(
            mock.patch.object(inspiral_module, "Pipeline", return_value=mock_pipeline)
        )
        stack.enter_context(mock.patch.object(inspiral_module, "StrikeTransform"))
        stack.enter_context(mock.patch.object(inspiral_module, "StillSuitSink"))
        stack.enter_context(mock.patch.object(inspiral_module, "StrikeSink"))
        stack.enter_context(mock.patch.object(inspiral_module, "ImpulseSink"))
        stack.enter_context(mock.patch.object(inspiral_module, "GraceDBSink"))
        stack.enter_context(mock.patch.object(inspiral_module, "EyeCandy"))
        stack.enter_context(mock.patch.object(inspiral_module, "KafkaSink"))
        stack.enter_context(mock.patch.object(inspiral_module, "DumpSeriesSink"))
        stack.enter_context(
            mock.patch.object(inspiral_module, "HorizonDistanceTracker")
        )
        stack.enter_context(mock.patch.object(inspiral_module, "Latency"))
        stack.enter_context(mock.patch.object(inspiral_module, "NullSink"))
        stack.enter_context(mock.patch.object(inspiral_module, "Itacacac"))
        stack.enter_context(
            mock.patch.object(
                inspiral_module,
                "get_analysis_config",
                return_value={
                    "default": {
                        "chi2_over_snr2_min": 0.0,
                        "chi2_over_snr2_max": 1.0,
                        "chi_bin_min": 0,
                        "chi_bin_max": 10,
                        "chi_bin_num": 10,
                        "highpass_filter": 15.0,
                    },
                    "ew": {
                        "chi2_over_snr2_min": 0.0,
                        "chi2_over_snr2_max": 1.0,
                        "chi_bin_min": 0,
                        "chi_bin_max": 10,
                        "chi_bin_num": 10,
                        "highpass_filter": 15.0,
                    },
                },
            )
        )
        stack.enter_context(mock.patch.object(inspiral_module, "SnapShotControl"))
        stack.enter_context(mock.patch.object(inspiral_module, "HTTPControl"))
        stack.enter_context(mock.patch.object(inspiral_module, "simulation"))
        stack.enter_context(mock.patch.object(inspiral_module, "ligolw_utils"))
        stack.enter_context(mock.patch.object(inspiral_module, "lsctables"))

        return stack, mock_sorted_bank, mock_pipeline, mock_bank


class TestInspiralPipeline:
    """Tests for inspiral function pipeline setup."""

    def _create_mock_data_source_info(self, data_source="frames", ifos=None):
        """Create a mock DataSourceInfo object."""
        if ifos is None:
            ifos = ["H1", "L1"]
        mock_info = mock.MagicMock()
        mock_info.data_source = data_source
        mock_info.ifos = ifos
        mock_info.all_analysis_ifos = ifos
        mock_info.seg = mock.MagicMock()
        mock_info.input_sample_rate = 16384
        mock_info.channel_dict = {ifo: f"{ifo}:GDS-CALIB_STRAIN" for ifo in ifos}
        return mock_info

    def _create_mock_condition_info(self):
        """Create a mock ConditionInfo object."""
        return mock.MagicMock()

    def _setup_pipeline_mocks(self, inspiral_module, ifos=None):
        """Setup mocks for pipeline components using mock.patch.

        Returns a context manager that patches all pipeline components.
        Use with 'with' statement to apply the patches.
        """
        from contextlib import ExitStack

        import numpy as np

        if ifos is None:
            ifos = ["H1", "L1"]

        # Create mock objects
        mock_bank = mock.MagicMock()
        mock_bank.sngl_inspiral_table = []
        mock_bank.template_bank_filename = "/mock/test_bank.xml"

        mock_sorted_bank = mock.MagicMock()
        mock_sorted_bank.bank_metadata = {"maxrate": 4096}
        mock_sorted_bank.template_ids = mock.MagicMock()
        mock_sorted_bank.template_ids.numpy.return_value = np.array([0, 1, 2])
        mock_sorted_bank.bankids_map = {"0": [0, 1, 2]}
        mock_sorted_bank.autocorrelation_banks = mock.MagicMock()
        mock_sorted_bank.autocorrelation_length_mask = mock.MagicMock()
        mock_sorted_bank.autocorrelation_lengths = mock.MagicMock()
        mock_sorted_bank.end_time_delta = mock.MagicMock()
        mock_sorted_bank.template_durations = mock.MagicMock()
        mock_sorted_bank.horizon_distance_funcs = {}
        mock_sorted_bank.sngls = {}
        mock_sorted_bank.subbankids = {}

        mock_pipeline = mock.MagicMock()
        mock_strike_object = mock.MagicMock()

        # Create ExitStack to manage multiple patches
        stack = ExitStack()

        # Patch all pipeline components on the inspiral module
        stack.enter_context(
            mock.patch.object(
                inspiral_module,
                "datasource",
                return_value=(
                    {ifo: f"{ifo}_link" for ifo in ifos},
                    {ifo: f"{ifo}_latency" for ifo in ifos},
                ),
            )
        )
        stack.enter_context(
            mock.patch.object(
                inspiral_module,
                "group_and_read_banks",
                return_value={ifo: [mock_bank] for ifo in ifos},
            )
        )
        stack.enter_context(
            mock.patch.object(
                inspiral_module, "SortedBank", return_value=mock_sorted_bank
            )
        )
        stack.enter_context(
            mock.patch.object(
                inspiral_module,
                "condition",
                return_value=(
                    {ifo: f"{ifo}_cond" for ifo in ifos},
                    {ifo: f"{ifo}_spec" for ifo in ifos},
                    {ifo: f"{ifo}_whiten_lat" for ifo in ifos},
                ),
            )
        )
        stack.enter_context(
            mock.patch.object(
                inspiral_module,
                "lloid",
                return_value={ifo: f"{ifo}_lloid" for ifo in ifos},
            )
        )
        stack.enter_context(
            mock.patch.object(
                inspiral_module, "StrikeObject", return_value=mock_strike_object
            )
        )
        stack.enter_context(
            mock.patch.object(inspiral_module, "Pipeline", return_value=mock_pipeline)
        )
        stack.enter_context(mock.patch.object(inspiral_module, "StrikeTransform"))
        stack.enter_context(mock.patch.object(inspiral_module, "StillSuitSink"))
        stack.enter_context(mock.patch.object(inspiral_module, "StrikeSink"))
        stack.enter_context(mock.patch.object(inspiral_module, "ImpulseSink"))
        stack.enter_context(mock.patch.object(inspiral_module, "GraceDBSink"))
        stack.enter_context(mock.patch.object(inspiral_module, "EyeCandy"))
        stack.enter_context(mock.patch.object(inspiral_module, "KafkaSink"))
        stack.enter_context(mock.patch.object(inspiral_module, "DumpSeriesSink"))
        stack.enter_context(
            mock.patch.object(inspiral_module, "HorizonDistanceTracker")
        )
        stack.enter_context(mock.patch.object(inspiral_module, "Latency"))
        stack.enter_context(mock.patch.object(inspiral_module, "NullSink"))
        stack.enter_context(mock.patch.object(inspiral_module, "Itacacac"))
        stack.enter_context(
            mock.patch.object(
                inspiral_module,
                "get_analysis_config",
                return_value={
                    "default": {
                        "chi2_over_snr2_min": 0.0,
                        "chi2_over_snr2_max": 1.0,
                        "chi_bin_min": 0,
                        "chi_bin_max": 10,
                        "chi_bin_num": 10,
                        "highpass_filter": 15.0,
                    },
                    "ew": {
                        "chi2_over_snr2_min": 0.0,
                        "chi2_over_snr2_max": 1.0,
                        "chi_bin_min": 0,
                        "chi_bin_max": 10,
                        "chi_bin_num": 10,
                        "highpass_filter": 15.0,
                    },
                },
            )
        )
        stack.enter_context(mock.patch.object(inspiral_module, "SnapShotControl"))
        stack.enter_context(mock.patch.object(inspiral_module, "HTTPControl"))
        stack.enter_context(mock.patch.object(inspiral_module, "simulation"))
        stack.enter_context(mock.patch.object(inspiral_module, "ligolw_utils"))
        stack.enter_context(mock.patch.object(inspiral_module, "lsctables"))

        return stack, mock_sorted_bank, mock_pipeline, mock_bank

    def test_inspiral_offline_with_fake_sink(self):
        """Test inspiral function with fake sink in offline mode."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        fake_sink=True,
                    )

            mock_pipeline.run.assert_called_once()

    def test_inspiral_offline_with_trigger_output(self):
        """Test inspiral function with trigger output in offline mode."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        trigger_output=["output.sqlite"],
                        event_config="config.yaml",
                    )

            mock_pipeline.run.assert_called_once()
            inspiral.StillSuitSink.assert_called_once()

    def test_inspiral_offline_with_output_likelihood(self):
        """Test inspiral function with output likelihood file."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        trigger_output=["output.sqlite"],
                        event_config="config.yaml",
                        output_likelihood_file=["lr.h5"],
                    )

            inspiral.StrikeSink.assert_called_once()

    def test_inspiral_impulse_mode(self):
        """Test inspiral function in impulse mode."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(
            data_source="impulse", ifos=["H1"]
        )
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral, ifos=["H1"]
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        impulse_bank="templates.xml",
                        impulse_bankno=0,
                        impulse_ifo="H1",
                    )

            inspiral.ImpulseSink.assert_called_once()

    def test_inspiral_online_mode(self):
        """Test inspiral function in online mode."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="arrakis")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        fake_sink=True,
                    )

            inspiral.StrikeTransform.assert_called_once()

    def test_inspiral_online_with_kafka(self):
        """Test inspiral function in online mode with Kafka."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="arrakis")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        event_config="config.yaml",
                        output_kafka_server="localhost:9092",
                        job_tag="0001_test",
                    )

            inspiral.GraceDBSink.assert_called_once()
            inspiral.EyeCandy.assert_called_once()
            inspiral.KafkaSink.assert_called_once()

    def test_inspiral_online_with_injections(self):
        """Test inspiral function in online mode with injections."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="arrakis")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        event_config="config.yaml",
                        injections=True,
                        fake_sink=True,
                    )

            # Verify function completed - StrikeSink is called for online injections
            mock_pipeline.run.assert_called_once()

    def test_inspiral_with_snr_timeseries_output(self):
        """Test inspiral function with SNR timeseries output."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        trigger_output=["output.sqlite"],
                        event_config="config.yaml",
                        snr_timeseries_output="snr.h5",
                    )

            inspiral.DumpSeriesSink.assert_called()

    def test_inspiral_with_graph_name(self):
        """Test inspiral function with graph visualization."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        fake_sink=True,
                        graph_name="pipeline.png",
                    )

            mock_pipeline.visualize.assert_called_once_with("pipeline.png")

    def test_inspiral_with_injection_file_and_segments(self):
        """Test inspiral with injection file and segment reconstruction."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        # Setup injection mocks
        mock_row = mock.MagicMock()
        mock_row.end = -10.0
        mock_row.time_geocent = 1000000000
        data_source_info.seg.__contains__ = mock.MagicMock(return_value=True)

        with stack:
            # Override group_and_read_banks to return injection bank
            mock_inj_bank = mock.MagicMock()
            mock_inj_bank.sngl_inspiral_table = [mock_row]
            mock_inj_bank.template_bank_filename = "/mock/test.xml"
            inspiral.group_and_read_banks.return_value = {
                "H1": [mock_inj_bank],
                "L1": [mock_inj_bank],
            }

            mock_xmldoc = mock.MagicMock()
            inspiral.ligolw_utils.load_filename.return_value = mock_xmldoc

            mock_sim_table = mock.MagicMock()
            mock_sim_table.__iter__ = mock.MagicMock(return_value=iter([mock_row]))
            inspiral.lsctables.SimInspiralTable.get_table.return_value = mock_sim_table

            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        trigger_output=["output.sqlite"],
                        event_config="config.yaml",
                        injections=True,
                        injection_file="injections.xml",
                        reconstruct_inj_segments=True,
                    )

            inspiral.simulation.sim_inspiral_to_segment_list.assert_called_once()

    def test_inspiral_with_nsubbank_pretend(self):
        """Test inspiral function with nsubbank_pretend for cleanup."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            # Override group_and_read_banks
            mock_bank_new = mock.MagicMock()
            mock_bank_new.template_bank_filename = "/mock/test_bank.xml"
            inspiral.group_and_read_banks.return_value = {
                "H1": [mock_bank_new],
                "L1": [mock_bank_new],
            }

            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove") as mock_remove:
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        fake_sink=True,
                        nsubbank_pretend=5,
                        verbose=True,
                    )

            # Should only remove first bank per ifo when nsubbank_pretend is set
            assert mock_remove.call_count == 2  # One for H1, one for L1

    def test_inspiral_online_with_job_tag_numeric(self):
        """Test inspiral function online with numeric job tag."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="arrakis")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        fake_sink=True,
                        job_tag="0001_test",
                    )

    def test_inspiral_online_with_job_tag_inj(self):
        """Test inspiral function online with job tag containing _inj."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="arrakis")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        fake_sink=True,
                        job_tag="0001_inj",
                        injections=True,
                    )

    def test_inspiral_search_ew(self):
        """Test inspiral function with EW search."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        fake_sink=True,
                        search="ew",
                    )

            # Should use ew config
            inspiral.get_analysis_config.assert_called_once()

    def test_inspiral_without_nsubbank_pretend_cleanup(self):
        """Test inspiral cleanup without nsubbank_pretend."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="frames")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            mock_bank1 = mock.MagicMock()
            mock_bank1.template_bank_filename = "/mock/test_bank1.xml"
            mock_bank2 = mock.MagicMock()
            mock_bank2.template_bank_filename = "/mock/test_bank2.xml"
            inspiral.group_and_read_banks.return_value = {
                "H1": [mock_bank1, mock_bank2],
                "L1": [mock_bank1, mock_bank2],
            }

            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove") as mock_remove:
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        fake_sink=True,
                        verbose=True,
                    )

            # Should remove all banks when nsubbank_pretend is not set
            assert mock_remove.call_count == 4  # 2 per ifo * 2 ifos

    def test_inspiral_devshm_data_source(self):
        """Test inspiral function with devshm data source (online)."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(data_source="devshm")
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        fake_sink=True,
                    )

            # Should be online mode
            inspiral.StrikeTransform.assert_called_once()

    def test_inspiral_white_realtime_data_source(self):
        """Test inspiral function with white-realtime data source (online)."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(
            data_source="white-realtime"
        )
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        fake_sink=True,
                    )

            # Should be online mode
            inspiral.StrikeTransform.assert_called_once()

    def test_inspiral_gwdata_noise_realtime_data_source(self):
        """Test inspiral function with gwdata-noise-realtime data source."""
        from sgnl.bin import inspiral

        data_source_info = self._create_mock_data_source_info(
            data_source="gwdata-noise-realtime"
        )
        condition_info = self._create_mock_condition_info()

        stack, mock_sorted_bank, mock_pipeline, mock_bank = self._setup_pipeline_mocks(
            inspiral
        )

        with stack:
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch("os.remove"):
                    inspiral.inspiral(
                        data_source_info=data_source_info,
                        condition_info=condition_info,
                        svd_bank=["bank.xml"],
                        fake_sink=True,
                    )

            # Should be online mode
            inspiral.StrikeTransform.assert_called_once()


class TestMain:
    """Tests for main function."""

    def test_main_calls_inspiral(self):
        """Test that main calls inspiral with parsed options."""
        from argparse import Namespace

        from sgnl.bin import inspiral

        # Create a proper Namespace object instead of MagicMock
        mock_options = Namespace(
            svd_bank=["bank.xml"],
            gracedb_aggregator_far_threshold=3.84e-07,
            gracedb_aggregator_far_trials_factor=1,
            all_triggers_to_background=False,
            analysis_tag="test",
            coincidence_threshold=0.005,
            compress_likelihood_ratio=False,
            compress_likelihood_ratio_threshold=0.03,
            event_config=None,
            fake_sink=True,
            search=None,
            far_trials_factor=1.0,
            gracedb_far_threshold=-1,
            gracedb_group="Test",
            gracedb_label=None,
            gracedb_pipeline="SGNL",
            gracedb_search="MOCK",
            gracedb_service_url=None,
            graph_name=None,
            impulse_bank=None,
            impulse_bankno=None,
            impulse_ifo=None,
            injections=False,
            injection_file=None,
            input_likelihood_file=None,
            job_tag="",
            min_instruments_candidates=1,
            nsubbank_pretend=0,
            nslice=-1,
            output_kafka_server=None,
            output_likelihood_file=None,
            rank_stat_pdf_file=None,
            reconstruct_inj_segments=False,
            snapshot_interval=14400,
            snapshot_multiprocess=False,
            snr_min=4.0,
            snr_timeseries_output=None,
            torch_device="cpu",
            torch_dtype="float32",
            trigger_finding_duration=1.0,
            trigger_output=None,
            verbose=False,
            zerolag_rank_stat_pdf_file=None,
            cap_singles=False,
            use_gstlal_cpu_upsample=False,
        )

        inspiral.parse_command_line = mock.MagicMock(return_value=mock_options)

        # Mock DataSourceInfo and ConditionInfo
        mock_data_source_info = mock.MagicMock()
        mock_condition_info = mock.MagicMock()
        inspiral.DataSourceInfo.from_options = mock.MagicMock(
            return_value=mock_data_source_info
        )
        inspiral.ConditionInfo.from_options = mock.MagicMock(
            return_value=mock_condition_info
        )

        # Mock inspiral function
        with mock.patch.object(inspiral, "inspiral") as mock_inspiral_func:
            inspiral.main()

        mock_inspiral_func.assert_called_once()
        inspiral.DataSourceInfo.from_options.assert_called_once_with(mock_options)
        inspiral.ConditionInfo.from_options.assert_called_once_with(mock_options)

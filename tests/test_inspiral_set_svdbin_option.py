"""Tests for sgnl.bin.inspiral_set_svdbin_option"""

import sys
from unittest import mock

import pytest


@pytest.fixture(autouse=True)
def mock_dependencies():
    """Mock external dependencies and clean up after tests."""
    sys.modules.pop("sgnl.bin.inspiral_set_svdbin_option", None)

    original_modules = {}

    # Create lal mock with rate.IrregularBins
    lal_mock = mock.MagicMock()
    rate_mock = mock.MagicMock()

    class MockIrregularBins:
        """Mock for lal.rate.IrregularBins"""

        def __init__(self, boundaries):
            self.boundaries = boundaries

        def __getitem__(self, value):
            """Return bin index for a given value."""
            for i in range(len(self.boundaries) - 1):
                if self.boundaries[i] <= value < self.boundaries[i + 1]:
                    return i
            # Handle edge case for last boundary
            if value == self.boundaries[-1]:
                return len(self.boundaries) - 2
            raise IndexError(f"Value {value} out of bounds")

    rate_mock.IrregularBins = MockIrregularBins
    lal_mock.rate = rate_mock

    # Create sgnl.dags mocks
    dags_config_mock = mock.MagicMock()
    dags_util_mock = mock.MagicMock()

    modules_to_mock = {
        "lal": lal_mock,
        "lal.rate": rate_mock,
        "sgnl.dags.config": dags_config_mock,
        "sgnl.dags.util": dags_util_mock,
    }

    for mod, mock_obj in modules_to_mock.items():
        original_modules[mod] = sys.modules.get(mod)
        sys.modules[mod] = mock_obj

    yield {
        "lal": lal_mock,
        "rate": rate_mock,
        "dags_config": dags_config_mock,
        "dags_util": dags_util_mock,
    }

    # Restore originals
    for mod in modules_to_mock:
        if original_modules[mod] is None:
            sys.modules.pop(mod, None)
        else:
            sys.modules[mod] = original_modules[mod]

    sys.modules.pop("sgnl.bin.inspiral_set_svdbin_option", None)


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_default_values(self):
        """Test default values for optional arguments."""
        from sgnl.bin import inspiral_set_svdbin_option

        with mock.patch(
            "sys.argv",
            ["inspiral_set_svdbin_option"],
        ):
            options = inspiral_set_svdbin_option.parse_command_line()
            assert options.config is None
            assert options.workflow == "full"
            assert options.dag_dir == "."

    def test_config_option(self):
        """Test --config option."""
        from sgnl.bin import inspiral_set_svdbin_option

        with mock.patch(
            "sys.argv",
            ["inspiral_set_svdbin_option", "--config", "config.yaml"],
        ):
            options = inspiral_set_svdbin_option.parse_command_line()
            assert options.config == "config.yaml"

    def test_config_short_option(self):
        """Test -c short option."""
        from sgnl.bin import inspiral_set_svdbin_option

        with mock.patch(
            "sys.argv",
            ["inspiral_set_svdbin_option", "-c", "config.yaml"],
        ):
            options = inspiral_set_svdbin_option.parse_command_line()
            assert options.config == "config.yaml"

    def test_workflow_option(self):
        """Test --workflow option."""
        from sgnl.bin import inspiral_set_svdbin_option

        with mock.patch(
            "sys.argv",
            ["inspiral_set_svdbin_option", "--workflow", "online"],
        ):
            options = inspiral_set_svdbin_option.parse_command_line()
            assert options.workflow == "online"

    def test_dag_dir_option(self):
        """Test --dag-dir option."""
        from sgnl.bin import inspiral_set_svdbin_option

        with mock.patch(
            "sys.argv",
            ["inspiral_set_svdbin_option", "--dag-dir", "/path/to/dag"],
        ):
            options = inspiral_set_svdbin_option.parse_command_line()
            assert options.dag_dir == "/path/to/dag"

    def test_all_options(self):
        """Test all options together."""
        from sgnl.bin import inspiral_set_svdbin_option

        with mock.patch(
            "sys.argv",
            [
                "inspiral_set_svdbin_option",
                "-c",
                "config.yaml",
                "-w",
                "offline",
                "--dag-dir",
                "/output",
            ],
        ):
            options = inspiral_set_svdbin_option.parse_command_line()
            assert options.config == "config.yaml"
            assert options.workflow == "offline"
            assert options.dag_dir == "/output"


class TestCalcGateThreshold:
    """Tests for calc_gate_threshold function."""

    def test_uniform_threshold(self):
        """Test with uniform (non-string) threshold."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.filter.ht_gate_threshold = 50.0

        mock_svd_stats = mock.MagicMock()
        mock_svd_stats.bins = {"bin1": {}}

        result = inspiral_set_svdbin_option.calc_gate_threshold(
            mock_config, "bin1", mock_svd_stats
        )
        assert result == 50.0
        assert mock_svd_stats.bins["bin1"]["ht_gate_threshold"] == 50.0

    def test_mchirp_dependent_threshold(self):
        """Test with mchirp-dependent (string) threshold."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        # Format: min_mchirp:min_threshold-max_mchirp:max_threshold
        mock_config.filter.ht_gate_threshold = "1.0:20.0-10.0:100.0"

        # svd_stats needs to support both dict access ["bins"] and .bins
        bins_dict = {"bin1": {"max_mchirp": 5.5}}
        mock_svd_stats = {"bins": bins_dict}
        mock_svd_stats_obj = mock.MagicMock()
        mock_svd_stats_obj.bins = bins_dict
        mock_svd_stats_obj.__getitem__ = lambda self, k: mock_svd_stats[k]

        result = inspiral_set_svdbin_option.calc_gate_threshold(
            mock_config, "bin1", mock_svd_stats_obj, aggregate="max"
        )
        # (100-20)/(10-1) = 80/9 ≈ 8.889
        # 8.889 * (5.5 - 1.0) + 20 = 8.889 * 4.5 + 20 ≈ 60
        assert result == 60.0
        assert bins_dict["bin1"]["ht_gate_threshold"] == 60.0

    def test_mchirp_dependent_threshold_min_boundary(self):
        """Test threshold at minimum mchirp boundary."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.filter.ht_gate_threshold = "1.0:20.0-10.0:100.0"

        bins_dict = {"bin1": {"max_mchirp": 1.0}}
        mock_svd_stats = {"bins": bins_dict}
        mock_svd_stats_obj = mock.MagicMock()
        mock_svd_stats_obj.bins = bins_dict
        mock_svd_stats_obj.__getitem__ = lambda self, k: mock_svd_stats[k]

        result = inspiral_set_svdbin_option.calc_gate_threshold(
            mock_config, "bin1", mock_svd_stats_obj, aggregate="max"
        )
        # At min_mchirp (1.0), threshold should be min_threshold (20.0)
        assert result == 20.0

    def test_mchirp_dependent_threshold_max_boundary(self):
        """Test threshold at maximum mchirp boundary."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.filter.ht_gate_threshold = "1.0:20.0-10.0:100.0"

        bins_dict = {"bin1": {"max_mchirp": 10.0}}
        mock_svd_stats = {"bins": bins_dict}
        mock_svd_stats_obj = mock.MagicMock()
        mock_svd_stats_obj.bins = bins_dict
        mock_svd_stats_obj.__getitem__ = lambda self, k: mock_svd_stats[k]

        result = inspiral_set_svdbin_option.calc_gate_threshold(
            mock_config, "bin1", mock_svd_stats_obj, aggregate="max"
        )
        # At max_mchirp (10.0), threshold should be max_threshold (100.0)
        assert result == 100.0


class TestAutocorrelationLengthMap:
    """Tests for autocorrelation_length_map function."""

    def test_single_value(self):
        """Test with a single autocorrelation value (non-string, non-iterable)."""
        from sgnl.bin import inspiral_set_svdbin_option

        # Single integer value
        mchirp_to_ac = inspiral_set_svdbin_option.autocorrelation_length_map(701)
        # Should return same value for any mchirp
        assert mchirp_to_ac(1.0) == 701
        assert mchirp_to_ac(10.0) == 701
        assert mchirp_to_ac(100.0) == 701

    def test_single_string_range(self):
        """Test with a single string range."""
        from sgnl.bin import inspiral_set_svdbin_option

        mchirp_to_ac = inspiral_set_svdbin_option.autocorrelation_length_map("0:15:701")
        assert mchirp_to_ac(5.0) == 701

    def test_multiple_ranges(self):
        """Test with multiple ranges."""
        from sgnl.bin import inspiral_set_svdbin_option

        ranges = ["0:5:201", "5:15:401", "15:100:701"]
        mchirp_to_ac = inspiral_set_svdbin_option.autocorrelation_length_map(ranges)

        assert mchirp_to_ac(2.0) == 201  # In first range
        assert mchirp_to_ac(10.0) == 401  # In second range
        assert mchirp_to_ac(50.0) == 701  # In third range

    def test_boundary_values(self):
        """Test at exact boundary values."""
        from sgnl.bin import inspiral_set_svdbin_option

        ranges = ["0:5:201", "5:15:401"]
        mchirp_to_ac = inspiral_set_svdbin_option.autocorrelation_length_map(ranges)

        assert mchirp_to_ac(0.0) == 201  # At min boundary
        assert mchirp_to_ac(5.0) == 401  # At boundary between ranges

    def test_gaps_not_allowed(self):
        """Test that gaps in ranges raise assertion error."""
        from sgnl.bin import inspiral_set_svdbin_option

        # Ranges with gap: 0-5, 6-15 (gap at 5-6)
        ranges = ["0:5:201", "6:15:401"]

        with pytest.raises(AssertionError, match="gaps not allowed"):
            inspiral_set_svdbin_option.autocorrelation_length_map(ranges)


class TestSvdBinToDtdphiFile:
    """Tests for svd_bin_to_dtdphi_file function."""

    def test_uniform_dtdphi_file(self):
        """Test with a single dtdphi file (non-mapping)."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.prior.dtdphi_file = "/path/to/dtdphi.h5"

        result = inspiral_set_svdbin_option.svd_bin_to_dtdphi_file(
            mock_config, "bin1", {"mean_mtotal": 50, "mean_mratio": 2}
        )
        assert result == "/path/to/dtdphi.h5"

    def test_dtdphi_file_by_bank_name(self):
        """Test dtdphi file selection by bank name."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.prior.dtdphi_file = {
            "BNS": "/path/to/bns_dtdphi.h5",
            "BBH": "/path/to/bbh_dtdphi.h5",
        }

        stats_bin = {"bank_name": "BNS", "mean_mtotal": 3.0}
        result = inspiral_set_svdbin_option.svd_bin_to_dtdphi_file(
            mock_config, "bin1", stats_bin
        )
        assert result == "/path/to/bns_dtdphi.h5"

    def test_dtdphi_file_imbh_category(self):
        """Test dtdphi file selection for IMBH category."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.prior.dtdphi_file = {
            "IMBH": "/path/to/imbh_dtdphi.h5",
            "others": "/path/to/default_dtdphi.h5",
        }

        # IMBH: mtotal > 100 and mratio < 10
        stats_bin = {"mean_mtotal": 150, "mean_mratio": 3}
        result = inspiral_set_svdbin_option.svd_bin_to_dtdphi_file(
            mock_config, "bin1", stats_bin
        )
        assert result == "/path/to/imbh_dtdphi.h5"

    def test_dtdphi_file_others_category(self):
        """Test dtdphi file selection falls back to others."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.prior.dtdphi_file = {
            "IMBH": "/path/to/imbh_dtdphi.h5",
            "others": "/path/to/default_dtdphi.h5",
        }

        # Not IMBH: mtotal <= 100
        stats_bin = {"mean_mtotal": 50, "mean_mratio": 2}
        result = inspiral_set_svdbin_option.svd_bin_to_dtdphi_file(
            mock_config, "bin1", stats_bin
        )
        assert result == "/path/to/default_dtdphi.h5"

    def test_dtdphi_file_no_match_raises(self):
        """Test that no matching category raises ValueError."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.prior.dtdphi_file = {
            "IMBH": "/path/to/imbh_dtdphi.h5",
            # No 'others' fallback
        }

        # Not IMBH
        stats_bin = {"mean_mtotal": 50, "mean_mratio": 2}

        with pytest.raises(ValueError, match="does not meet a condition"):
            inspiral_set_svdbin_option.svd_bin_to_dtdphi_file(
                mock_config, "bin1", stats_bin
            )

    def test_dtdphi_file_undefined_category_raises(self):
        """Test that undefined categories raise AssertionError."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.prior.dtdphi_file = {
            "undefined_category": "/path/to/dtdphi.h5",
            "others": "/path/to/default_dtdphi.h5",
        }

        stats_bin = {"mean_mtotal": 50, "mean_mratio": 2}

        with pytest.raises(
            AssertionError, match="not defined in those in the source code"
        ):
            inspiral_set_svdbin_option.svd_bin_to_dtdphi_file(
                mock_config, "bin1", stats_bin
            )


class MockSvdStats:
    """Mock class for svd_stats that supports both dict and attribute access."""

    def __init__(self, bins):
        self.bins = bins

    def __getitem__(self, key):
        if key == "bins":
            return self.bins
        raise KeyError(key)


class TestSetSvdbinOption:
    """Tests for set_svdbin_option function."""

    def test_basic_set_svdbin_option(self, mock_dependencies):
        """Test basic set_svdbin_option functionality."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.svd.option_file = "/path/to/options.json"
        mock_config.svd.autocorrelation_length = 701
        mock_config.filter.ht_gate_threshold = 50.0
        mock_config.prior.mass_model = "/path/to/mass_model.h5"
        mock_config.prior.idq_timeseries = None
        mock_config.prior.dtdphi_file = None
        mock_config.filter.reconstruction_segment = None
        mock_config.prior.seed_likelihood = None

        mock_svd_stats = MockSvdStats(
            {
                "bin1": {"mean_mchirp": 5.0},
                "bin2": {"mean_mchirp": 10.0},
            }
        )

        mock_open = mock.mock_open()
        with mock.patch.object(
            inspiral_set_svdbin_option,
            "load_svd_options",
            return_value=(None, mock_svd_stats),
        ):
            with mock.patch("builtins.open", mock_open):
                with mock.patch("json.dumps", return_value="{}"):
                    inspiral_set_svdbin_option.set_svdbin_option(mock_config)

        # Verify bins were updated
        assert mock_svd_stats.bins["bin1"]["ac_length"] == 701
        assert mock_svd_stats.bins["bin1"]["ht_gate_threshold"] == 50.0
        assert (
            mock_svd_stats.bins["bin1"]["mass_model_file"] == "/path/to/mass_model.h5"
        )

        # Verify file was written
        mock_open.assert_called_once_with("/path/to/options.json", "w")

    def test_set_svdbin_option_with_idq(self, mock_dependencies):
        """Test set_svdbin_option with idq_timeseries."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.svd.option_file = "/path/to/options.json"
        mock_config.svd.autocorrelation_length = 701
        mock_config.filter.ht_gate_threshold = 50.0
        mock_config.prior.mass_model = "/path/to/mass_model.h5"
        mock_config.prior.idq_timeseries = "/path/to/idq.h5"
        mock_config.prior.dtdphi_file = None
        mock_config.filter.reconstruction_segment = None
        mock_config.prior.seed_likelihood = None

        mock_svd_stats = MockSvdStats({"bin1": {"mean_mchirp": 5.0}})

        mock_open = mock.mock_open()
        with mock.patch.object(
            inspiral_set_svdbin_option,
            "load_svd_options",
            return_value=(None, mock_svd_stats),
        ):
            with mock.patch("builtins.open", mock_open):
                with mock.patch("json.dumps", return_value="{}"):
                    inspiral_set_svdbin_option.set_svdbin_option(mock_config)

        assert mock_svd_stats.bins["bin1"]["idq_file"] == "/path/to/idq.h5"

    def test_set_svdbin_option_with_dtdphi(self, mock_dependencies):
        """Test set_svdbin_option with dtdphi_file."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.svd.option_file = "/path/to/options.json"
        mock_config.svd.autocorrelation_length = 701
        mock_config.filter.ht_gate_threshold = 50.0
        mock_config.prior.mass_model = "/path/to/mass_model.h5"
        mock_config.prior.idq_timeseries = None
        mock_config.prior.dtdphi_file = "/path/to/dtdphi.h5"
        mock_config.filter.reconstruction_segment = None
        mock_config.prior.seed_likelihood = None

        mock_svd_stats = MockSvdStats({"bin1": {"mean_mchirp": 5.0}})

        mock_open = mock.mock_open()
        with mock.patch.object(
            inspiral_set_svdbin_option,
            "load_svd_options",
            return_value=(None, mock_svd_stats),
        ):
            with mock.patch("builtins.open", mock_open):
                with mock.patch("json.dumps", return_value="{}"):
                    inspiral_set_svdbin_option.set_svdbin_option(mock_config)

        assert mock_svd_stats.bins["bin1"]["dtdphi_file"] == "/path/to/dtdphi.h5"

    def test_set_svdbin_option_with_reconstruction_segment(self, mock_dependencies):
        """Test set_svdbin_option with reconstruction_segment."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.svd.option_file = "/path/to/options.json"
        mock_config.svd.autocorrelation_length = 701
        mock_config.filter.ht_gate_threshold = 50.0
        mock_config.prior.mass_model = "/path/to/mass_model.h5"
        mock_config.prior.idq_timeseries = None
        mock_config.prior.dtdphi_file = None
        mock_config.filter.reconstruction_segment = "/path/to/segments"
        mock_config.prior.seed_likelihood = None

        mock_svd_stats = MockSvdStats({"bin1": {"mean_mchirp": 5.0}})

        mock_open = mock.mock_open()
        with mock.patch.object(
            inspiral_set_svdbin_option,
            "load_svd_options",
            return_value=(None, mock_svd_stats),
        ):
            with mock.patch("builtins.open", mock_open):
                with mock.patch("json.dumps", return_value="{}"):
                    with mock.patch.object(
                        inspiral_set_svdbin_option.glob,
                        "glob",
                        return_value=["/path/to/segments/H1-bin1_SEGMENTS-123.xml.gz"],
                    ):
                        inspiral_set_svdbin_option.set_svdbin_option(mock_config)

        assert (
            mock_svd_stats.bins["bin1"]["reconstruction_segment"]
            == "/path/to/segments/H1-bin1_SEGMENTS-123.xml.gz"
        )

    def test_set_svdbin_option_no_segment_file_found(self, mock_dependencies):
        """Test error when no segment file found."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.svd.option_file = "/path/to/options.json"
        mock_config.svd.autocorrelation_length = 701
        mock_config.filter.ht_gate_threshold = 50.0
        mock_config.prior.mass_model = "/path/to/mass_model.h5"
        mock_config.prior.idq_timeseries = None
        mock_config.prior.dtdphi_file = None
        mock_config.filter.reconstruction_segment = "/path/to/segments"
        mock_config.prior.seed_likelihood = None

        mock_svd_stats = MockSvdStats({"bin1": {"mean_mchirp": 5.0}})

        with mock.patch.object(
            inspiral_set_svdbin_option,
            "load_svd_options",
            return_value=(None, mock_svd_stats),
        ):
            with mock.patch.object(
                inspiral_set_svdbin_option.glob, "glob", return_value=[]
            ):
                with pytest.raises(ValueError, match="No segment file is found"):
                    inspiral_set_svdbin_option.set_svdbin_option(mock_config)

    def test_set_svdbin_option_multiple_segment_files(self, mock_dependencies):
        """Test error when multiple segment files found."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.svd.option_file = "/path/to/options.json"
        mock_config.svd.autocorrelation_length = 701
        mock_config.filter.ht_gate_threshold = 50.0
        mock_config.prior.mass_model = "/path/to/mass_model.h5"
        mock_config.prior.idq_timeseries = None
        mock_config.prior.dtdphi_file = None
        mock_config.filter.reconstruction_segment = "/path/to/segments"
        mock_config.prior.seed_likelihood = None

        mock_svd_stats = MockSvdStats({"bin1": {"mean_mchirp": 5.0}})

        with mock.patch.object(
            inspiral_set_svdbin_option,
            "load_svd_options",
            return_value=(None, mock_svd_stats),
        ):
            with mock.patch.object(
                inspiral_set_svdbin_option.glob,
                "glob",
                return_value=[
                    "/path/to/segments/file1.xml.gz",
                    "/path/to/segments/file2.xml.gz",
                ],
            ):
                with pytest.raises(ValueError, match="more than one segment file"):
                    inspiral_set_svdbin_option.set_svdbin_option(mock_config)

    def test_set_svdbin_option_with_seed_likelihood(self, mock_dependencies):
        """Test set_svdbin_option with seed_likelihood."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.svd.option_file = "/path/to/options.json"
        mock_config.svd.autocorrelation_length = 701
        mock_config.filter.ht_gate_threshold = 50.0
        mock_config.prior.mass_model = "/path/to/mass_model.h5"
        mock_config.prior.idq_timeseries = None
        mock_config.prior.dtdphi_file = None
        mock_config.filter.reconstruction_segment = None

        # seed_likelihood with custom options
        mock_seed = mock.MagicMock()
        mock_seed.items.return_value = [
            ("custom_option", "custom_value"),
            ("mass_model", None),
        ]
        mock_seed.mass_model = None
        mock_config.prior.seed_likelihood = mock_seed

        mock_svd_stats = MockSvdStats({"bin1": {"mean_mchirp": 5.0}})

        mock_open = mock.mock_open()
        with mock.patch.object(
            inspiral_set_svdbin_option,
            "load_svd_options",
            return_value=(None, mock_svd_stats),
        ):
            with mock.patch("builtins.open", mock_open):
                with mock.patch("json.dumps", return_value="{}"):
                    inspiral_set_svdbin_option.set_svdbin_option(mock_config)

        assert mock_svd_stats.bins["bin1"]["custom_option"] == "custom_value"

    def test_set_svdbin_option_with_seed_mass_model(self, mock_dependencies):
        """Test set_svdbin_option with seed_likelihood mass_model."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.svd.option_file = "/path/to/options.json"
        mock_config.svd.autocorrelation_length = 701
        mock_config.filter.ht_gate_threshold = 50.0
        mock_config.prior.mass_model = "/path/to/mass_model.h5"
        mock_config.prior.idq_timeseries = None
        mock_config.prior.dtdphi_file = None
        mock_config.filter.reconstruction_segment = None

        mock_seed = mock.MagicMock()
        mock_seed.items.return_value = [("mass_model", "/new/mass_model.h5")]
        mock_seed.mass_model = "/new/mass_model.h5"
        mock_config.prior.seed_likelihood = mock_seed

        mock_svd_stats = MockSvdStats({"bin1": {"mean_mchirp": 5.0}})

        mock_open = mock.mock_open()
        with mock.patch.object(
            inspiral_set_svdbin_option,
            "load_svd_options",
            return_value=(None, mock_svd_stats),
        ):
            with mock.patch("builtins.open", mock_open):
                with mock.patch("json.dumps", return_value="{}"):
                    inspiral_set_svdbin_option.set_svdbin_option(mock_config)

        assert mock_svd_stats.bins["bin1"]["mass_model_file"] == "/new/mass_model.h5"
        assert mock_svd_stats.bins["bin1"]["old_mass_model_file"] == (
            "/path/to/mass_model.h5"
        )


class TestMain:
    """Tests for main function."""

    def test_main(self, mock_dependencies):
        """Test main function calls parse_command_line and set_svdbin_option."""
        from sgnl.bin import inspiral_set_svdbin_option

        mock_config = mock.MagicMock()
        mock_config.svd.option_file = "/path/to/options.json"
        mock_config.svd.autocorrelation_length = 701
        mock_config.filter.ht_gate_threshold = 50.0
        mock_config.prior.mass_model = "/path/to/mass_model.h5"
        mock_config.prior.idq_timeseries = None
        mock_config.prior.dtdphi_file = None
        mock_config.filter.reconstruction_segment = None
        mock_config.prior.seed_likelihood = None

        mock_svd_stats = MockSvdStats({"bin1": {"mean_mchirp": 5.0}})

        mock_open = mock.mock_open()
        with mock.patch(
            "sys.argv",
            ["inspiral_set_svdbin_option", "-c", "config.yaml", "--dag-dir", "/dag"],
        ):
            with mock.patch.object(
                inspiral_set_svdbin_option,
                "build_config",
                return_value=mock_config,
            ) as mock_build_config:
                with mock.patch.object(
                    inspiral_set_svdbin_option,
                    "load_svd_options",
                    return_value=(None, mock_svd_stats),
                ):
                    with mock.patch("builtins.open", mock_open):
                        with mock.patch("json.dumps", return_value="{}"):
                            inspiral_set_svdbin_option.main()

        mock_build_config.assert_called_once_with("config.yaml", "/dag")

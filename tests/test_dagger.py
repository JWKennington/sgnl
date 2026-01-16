"""Tests for sgnl.bin.dagger"""

import os
import sys
from unittest import mock

import pytest


@pytest.fixture(autouse=True)
def mock_dependencies():
    """Mock external dependencies and clean up after tests."""
    # Store original modules locally within the fixture
    original_modules = {}
    modules_to_mock = [
        "ezdag",
        "sgnl.bin.inspiral_bank_splitter",
        "sgnl.bin.inspiral_set_svdbin_option",
        "sgnl.dags.layers",
        "sgnl.dags.config",
        "sgnl.dags.util",
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

    # Clear the cached import
    sys.modules.pop("sgnl.bin.dagger", None)


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_basic_init_flag(self):
        """Test parsing with --init flag."""
        from sgnl.bin import dagger

        args = dagger.parse_command_line(["--config", "config.yaml", "--init"])
        assert args.config == "config.yaml"
        assert args.init is True
        assert args.workflow is None

    def test_basic_workflow(self):
        """Test parsing with --workflow flag."""
        from sgnl.bin import dagger

        args = dagger.parse_command_line(
            ["--config", "config.yaml", "--workflow", "psd"]
        )
        assert args.config == "config.yaml"
        assert args.init is False
        assert args.workflow == "psd"

    def test_dag_dir_option(self):
        """Test --dag-dir option."""
        from sgnl.bin import dagger

        args = dagger.parse_command_line(
            [
                "--config",
                "config.yaml",
                "--workflow",
                "psd",
                "--dag-dir",
                "/path/to/dir",
            ]
        )
        assert args.dag_dir == "/path/to/dir"

    def test_dag_dir_default(self):
        """Test default --dag-dir value."""
        from sgnl.bin import dagger

        args = dagger.parse_command_line(
            ["--config", "config.yaml", "--workflow", "psd"]
        )
        assert args.dag_dir == "."

    def test_dag_name_option(self):
        """Test --dag-name option."""
        from sgnl.bin import dagger

        args = dagger.parse_command_line(
            ["--config", "config.yaml", "--workflow", "psd", "--dag-name", "my_dag"]
        )
        assert args.dag_name == "my_dag"

    def test_force_segments_option(self):
        """Test --force-segments option."""
        from sgnl.bin import dagger

        args = dagger.parse_command_line(
            ["--config", "config.yaml", "--workflow", "psd", "--force-segments"]
        )
        assert args.force_segments is True

    def test_dag_name_with_path_raises_error(self):
        """Test that dag name with path separator raises error."""
        from sgnl.bin import dagger

        with pytest.raises(ValueError, match="must not be a path"):
            dagger.parse_command_line(
                [
                    "--config",
                    "config.yaml",
                    "--workflow",
                    "psd",
                    "--dag-name",
                    "path/name",
                ]
            )

    def test_dag_name_with_dag_extension_raises_error(self):
        """Test that dag name ending in .dag raises error."""
        from sgnl.bin import dagger

        with pytest.raises(ValueError, match='ends with ".dag"'):
            dagger.parse_command_line(
                [
                    "--config",
                    "config.yaml",
                    "--workflow",
                    "psd",
                    "--dag-name",
                    "mydag.dag",
                ]
            )

    def test_init_and_workflow_together_raises_error(self):
        """Test that --init and --workflow together raises error."""
        from sgnl.bin import dagger

        with pytest.raises(ValueError, match="Cannot specify both"):
            dagger.parse_command_line(
                ["--config", "config.yaml", "--init", "--workflow", "psd"]
            )

    def test_neither_init_nor_workflow_raises_error(self):
        """Test that neither --init nor --workflow raises error."""
        from sgnl.bin import dagger

        with pytest.raises(ValueError, match="Must specify either"):
            dagger.parse_command_line(["--config", "config.yaml"])

    def test_missing_config_raises_error(self):
        """Test that missing --config raises error."""
        from sgnl.bin import dagger

        with pytest.raises(SystemExit):
            dagger.parse_command_line(["--init"])

    def test_uses_sys_argv_when_no_args(self):
        """Test that sys.argv[1:] is used when args is None."""
        from sgnl.bin import dagger

        with mock.patch("sys.argv", ["dagger", "--config", "test.yaml", "--init"]):
            args = dagger.parse_command_line(None)
            assert args.config == "test.yaml"
            assert args.init is True


class TestPrepareOsdfSupport:
    """Tests for prepare_osdf_support function."""

    def test_no_source_config(self):
        """Test when config has no source."""
        from sgnl.bin import dagger

        config = mock.MagicMock()
        config.source = None
        dagger.prepare_osdf_support(config, "/dag/dir")
        # Should not raise any errors

    def test_no_frame_cache(self):
        """Test when source has no frame_cache."""
        from sgnl.bin import dagger

        config = mock.MagicMock()
        config.source.frame_cache = None
        config.source.inj_frame_cache = None
        dagger.prepare_osdf_support(config, "/dag/dir")
        # Should not raise any errors

    def test_with_frame_cache(self):
        """Test with frame_cache set."""
        from sgnl.bin import dagger

        config = mock.MagicMock()
        config.source.frame_cache = "/path/to/cache"
        config.source.inj_frame_cache = None

        with mock.patch.object(dagger, "osdf_flatten_frame_cache") as mock_flatten:
            mock_flatten.return_value = (True, "/path/to/flat_cache")
            dagger.prepare_osdf_support(config, "/dag/dir")

            mock_flatten.assert_called_once_with(
                "/path/to/cache",
                new_cache_name=os.path.join("/dag/dir", "flat_frame_cache.cache"),
            )
            assert config.source.frames_in_osdf is True
            assert config.source.transfer_frame_cache == "/path/to/flat_cache"

    def test_with_inj_frame_cache(self):
        """Test with inj_frame_cache set."""
        from sgnl.bin import dagger

        config = mock.MagicMock()
        config.source.frame_cache = None
        config.source.inj_frame_cache = "/path/to/inj_cache"

        with mock.patch.object(dagger, "osdf_flatten_frame_cache") as mock_flatten:
            mock_flatten.return_value = (True, "/path/to/flat_inj_cache")
            dagger.prepare_osdf_support(config, "/dag/dir")

            mock_flatten.assert_called_once_with(
                "/path/to/inj_cache",
                new_cache_name=os.path.join("/dag/dir", "flat_inj_frame_cache.cache"),
            )
            assert config.source.inj_frames_in_osdf is True
            assert config.source.transfer_inj_frame_cache == "/path/to/flat_inj_cache"

    def test_with_both_caches(self):
        """Test with both frame_cache and inj_frame_cache set."""
        from sgnl.bin import dagger

        config = mock.MagicMock()
        config.source.frame_cache = "/path/to/cache"
        config.source.inj_frame_cache = "/path/to/inj_cache"

        with mock.patch.object(dagger, "osdf_flatten_frame_cache") as mock_flatten:
            mock_flatten.side_effect = [
                (True, "/path/to/flat_cache"),
                (False, "/path/to/flat_inj_cache"),
            ]
            dagger.prepare_osdf_support(config, "/dag/dir")

            assert mock_flatten.call_count == 2


class TestMain:
    """Tests for main function."""

    def _create_mock_config(self, with_injections=False):
        """Create a mock config object."""
        config = mock.MagicMock()
        config.instruments = "H1L1"
        config.ifos = ["H1", "L1"]
        config.ifo_combos = [
            frozenset(["H1"]),
            frozenset(["L1"]),
            frozenset(["H1", "L1"]),
        ]
        config.all_ifos = frozenset(["H1", "L1"])
        config.time_bins = [mock.MagicMock()]
        config.span = mock.MagicMock()
        config.paths = mock.MagicMock()
        config.paths.input_data = "/input/data"
        config.paths.storage = "/storage"
        config.paths.filter_dir = None
        config.paths.injection_dir = None
        config.paths.rank_dir = "/rank/dir"
        config.psd = mock.MagicMock()
        config.psd.fft_length = 8
        config.svd = mock.MagicMock()
        config.svd.max_f_final = 1024
        config.svd.f_low = 15
        config.svd.num_split_templates = 1000
        config.svd.num_banks = 4
        config.svd.option_file = "/path/to/options.json"
        config.svd.sort_by = None
        config.svd.overlap = None
        config.svd.approximant = "IMRPhenomD"
        config.filter = mock.MagicMock()
        config.filter.min_instruments_candidates = 2
        config.filter.coincidence_threshold = 0.005
        config.filter.event_config_file = "/path/to/event_config.yaml"
        config.prior = mock.MagicMock()
        config.prior.mass_model = "/path/to/mass_model.h5"
        config.condor = mock.MagicMock()
        config.source = mock.MagicMock()
        config.source.frame_cache = None
        config.source.inj_frame_cache = None
        config.source.frame_segments_file = "/path/to/segments.xml"
        config.source.frame_segments_name = "datasegments"
        config.echo = mock.MagicMock()
        config.rank = mock.MagicMock()
        config.rank.calc_pdf_jobs = None
        config.summary = mock.MagicMock()
        config.summary.webdir = "/web/dir"

        if with_injections:
            config.injections = mock.MagicMock()
            config.injections.filter = {
                "inj1": {"range": "1.0:10.0"},
                "inj2": {"range": "10.0:50.0"},
            }
        else:
            config.injections = None
        return config

    def _create_mock_svd_stats(self):
        """Create mock SVD stats."""
        svd_stats = mock.MagicMock()
        svd_stats.bins = {
            "0": {"max_dur": 64, "min_mchirp": 1.0, "max_mchirp": 10.0},
            "1": {"max_dur": 32, "min_mchirp": 10.0, "max_mchirp": 50.0},
        }
        return svd_stats

    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.inspiral_bank_splitter")
    @mock.patch("sgnl.bin.dagger.inspiral_set_svdbin_option")
    def test_main_init_basic(
        self, mock_set_svdbin, mock_bank_splitter, mock_build_config, tmp_path
    ):
        """Test main with --init flag."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        mock_build_config.return_value = config

        with mock.patch(
            "sys.argv",
            ["dagger", "--config", str(tmp_path / "config.yaml"), "--init"],
        ):
            dagger.main()

        mock_build_config.assert_called_once()
        mock_bank_splitter.split_bank.assert_called_once()
        mock_set_svdbin.set_svdbin_option.assert_called_once_with(config)

    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.inspiral_bank_splitter")
    @mock.patch("sgnl.bin.dagger.inspiral_set_svdbin_option")
    def test_main_init_with_sort_by_chi(
        self, mock_set_svdbin, mock_bank_splitter, mock_build_config, tmp_path
    ):
        """Test main with --init flag and sort_by chi."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        config.svd.sort_by = "chi"
        config.svd.num_chi_bins = 5
        mock_build_config.return_value = config

        with mock.patch(
            "sys.argv",
            ["dagger", "--config", str(tmp_path / "config.yaml"), "--init"],
        ):
            dagger.main()

        call_kwargs = mock_bank_splitter.split_bank.call_args
        assert call_kwargs[1]["sort_by"] == "chi"
        assert call_kwargs[1]["group_by_chi"] == 5

    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.inspiral_bank_splitter")
    @mock.patch("sgnl.bin.dagger.inspiral_set_svdbin_option")
    def test_main_init_with_sort_by_mu(
        self, mock_set_svdbin, mock_bank_splitter, mock_build_config, tmp_path
    ):
        """Test main with --init flag and sort_by mu."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        config.svd.sort_by = "mu"
        config.svd.num_mu_bins = 3
        mock_build_config.return_value = config

        with mock.patch(
            "sys.argv",
            ["dagger", "--config", str(tmp_path / "config.yaml"), "--init"],
        ):
            dagger.main()

        call_kwargs = mock_bank_splitter.split_bank.call_args
        assert call_kwargs[1]["sort_by"] == "mu"
        assert call_kwargs[1]["group_by_mu"] == 3

    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.inspiral_bank_splitter")
    @mock.patch("sgnl.bin.dagger.inspiral_set_svdbin_option")
    def test_main_init_with_overlap(
        self, mock_set_svdbin, mock_bank_splitter, mock_build_config, tmp_path
    ):
        """Test main with --init flag and overlap option."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        config.svd.overlap = 0.1
        mock_build_config.return_value = config

        with mock.patch(
            "sys.argv",
            ["dagger", "--config", str(tmp_path / "config.yaml"), "--init"],
        ):
            dagger.main()

        call_kwargs = mock_bank_splitter.split_bank.call_args
        assert call_kwargs[1]["overlap"] == 0.1

    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.inspiral_bank_splitter")
    @mock.patch("sgnl.bin.dagger.inspiral_set_svdbin_option")
    def test_main_init_with_num_banks_list(
        self, mock_set_svdbin, mock_bank_splitter, mock_build_config, tmp_path
    ):
        """Test main with --init flag and num_banks as list."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        config.svd.num_banks = [4, 8, 16]
        mock_build_config.return_value = config

        with mock.patch(
            "sys.argv",
            ["dagger", "--config", str(tmp_path / "config.yaml"), "--init"],
        ):
            dagger.main()

        call_kwargs = mock_bank_splitter.split_bank.call_args
        assert call_kwargs[1]["num_banks"] == [4, 8, 16]

    @mock.patch("sgnl.bin.dagger.DAG")
    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.load_svd_options")
    @mock.patch("sgnl.bin.dagger.create_time_bins")
    @mock.patch("sgnl.bin.dagger.prepare_osdf_support")
    @mock.patch("sgnl.bin.dagger.layers")
    def test_main_workflow_test(
        self,
        mock_layers,
        mock_prepare_osdf,
        mock_create_time_bins,
        mock_load_svd,
        mock_build_config,
        mock_dag_class,
        tmp_path,
    ):
        """Test main with test workflow."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        mock_build_config.return_value = config
        mock_load_svd.return_value = (["0", "1"], self._create_mock_svd_stats())
        mock_dag = mock.MagicMock()
        mock_dag_class.return_value = mock_dag

        with mock.patch(
            "sys.argv",
            [
                "dagger",
                "--config",
                str(tmp_path / "config.yaml"),
                "--workflow",
                "test",
                "--dag-dir",
                str(tmp_path),
            ],
        ):
            dagger.main()

        mock_dag_class.assert_called_once_with("sgnl_test")
        mock_layers.test.assert_called_once()
        mock_dag.attach.assert_called()
        mock_dag.write.assert_called_once()
        mock_dag.create_log_dir.assert_called_once()

    @mock.patch("sgnl.bin.dagger.DAG")
    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.load_svd_options")
    @mock.patch("sgnl.bin.dagger.create_time_bins")
    @mock.patch("sgnl.bin.dagger.prepare_osdf_support")
    @mock.patch("sgnl.bin.dagger.layers")
    @mock.patch("sgnl.bin.dagger.DataCache")
    @mock.patch("sgnl.bin.dagger.add_osdf_support_to_layer")
    def test_main_workflow_psd(
        self,
        mock_add_osdf,
        mock_datacache,
        mock_layers,
        mock_prepare_osdf,
        mock_create_time_bins,
        mock_load_svd,
        mock_build_config,
        mock_dag_class,
        tmp_path,
    ):
        """Test main with psd workflow."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        mock_build_config.return_value = config
        mock_load_svd.return_value = (["0", "1"], self._create_mock_svd_stats())
        mock_dag = mock.MagicMock()
        mock_dag_class.return_value = mock_dag

        with mock.patch(
            "sys.argv",
            [
                "dagger",
                "--config",
                str(tmp_path / "config.yaml"),
                "--workflow",
                "psd",
                "--dag-dir",
                str(tmp_path),
            ],
        ):
            dagger.main()

        mock_dag_class.assert_called_once_with("sgnl_psd")
        mock_layers.reference_psd.assert_called_once()
        mock_layers.median_psd.assert_called_once()
        assert mock_dag.attach.call_count == 2

    @mock.patch("sgnl.bin.dagger.DAG")
    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.load_svd_options")
    @mock.patch("sgnl.bin.dagger.create_time_bins")
    @mock.patch("sgnl.bin.dagger.prepare_osdf_support")
    @mock.patch("sgnl.bin.dagger.layers")
    @mock.patch("sgnl.bin.dagger.DataCache")
    def test_main_workflow_svd(
        self,
        mock_datacache,
        mock_layers,
        mock_prepare_osdf,
        mock_create_time_bins,
        mock_load_svd,
        mock_build_config,
        mock_dag_class,
        tmp_path,
    ):
        """Test main with svd workflow."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        mock_build_config.return_value = config
        mock_load_svd.return_value = (["0", "1"], self._create_mock_svd_stats())
        mock_dag = mock.MagicMock()
        mock_dag_class.return_value = mock_dag

        with mock.patch(
            "sys.argv",
            [
                "dagger",
                "--config",
                str(tmp_path / "config.yaml"),
                "--workflow",
                "svd",
                "--dag-dir",
                str(tmp_path),
            ],
        ):
            dagger.main()

        mock_dag_class.assert_called_once_with("sgnl_svd")
        mock_layers.svd_bank.assert_called_once()

    @mock.patch("sgnl.bin.dagger.DAG")
    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.load_svd_options")
    @mock.patch("sgnl.bin.dagger.create_time_bins")
    @mock.patch("sgnl.bin.dagger.prepare_osdf_support")
    @mock.patch("sgnl.bin.dagger.layers")
    @mock.patch("sgnl.bin.dagger.DataCache")
    @mock.patch("sgnl.bin.dagger.add_osdf_support_to_layer")
    def test_main_workflow_filter(
        self,
        mock_add_osdf,
        mock_datacache,
        mock_layers,
        mock_prepare_osdf,
        mock_create_time_bins,
        mock_load_svd,
        mock_build_config,
        mock_dag_class,
        tmp_path,
    ):
        """Test main with filter workflow."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        mock_build_config.return_value = config
        mock_load_svd.return_value = (["0", "1"], self._create_mock_svd_stats())
        mock_dag = mock.MagicMock()
        mock_dag_class.return_value = mock_dag

        # Mock DataCache for groupby
        mock_lr_cache = mock.MagicMock()
        mock_lr_cache.groupby.return_value = {
            "0": mock.MagicMock(),
            "1": mock.MagicMock(),
        }
        mock_datacache.generate.return_value = mock_lr_cache
        mock_datacache.find.return_value = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "dagger",
                "--config",
                str(tmp_path / "config.yaml"),
                "--workflow",
                "filter",
                "--dag-dir",
                str(tmp_path),
            ],
        ):
            dagger.main()

        mock_dag_class.assert_called_once_with("sgnl_filter")
        mock_layers.filter.assert_called_once()
        mock_layers.marginalize_likelihood_ratio.assert_called_once()
        mock_add_osdf.assert_called()

    @mock.patch("sgnl.bin.dagger.DAG")
    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.load_svd_options")
    @mock.patch("sgnl.bin.dagger.create_time_bins")
    @mock.patch("sgnl.bin.dagger.prepare_osdf_support")
    @mock.patch("sgnl.bin.dagger.layers")
    @mock.patch("sgnl.bin.dagger.DataCache")
    @mock.patch("sgnl.bin.dagger.add_osdf_support_to_layer")
    def test_main_workflow_filter_sets_filter_dir(
        self,
        mock_add_osdf,
        mock_datacache,
        mock_layers,
        mock_prepare_osdf,
        mock_create_time_bins,
        mock_load_svd,
        mock_build_config,
        mock_dag_class,
        tmp_path,
    ):
        """Test main with filter workflow sets filter_dir if not set."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        config.paths.filter_dir = None
        mock_build_config.return_value = config
        mock_load_svd.return_value = (["0", "1"], self._create_mock_svd_stats())
        mock_dag = mock.MagicMock()
        mock_dag_class.return_value = mock_dag

        mock_lr_cache = mock.MagicMock()
        mock_lr_cache.groupby.return_value = {"0": mock.MagicMock()}
        mock_datacache.generate.return_value = mock_lr_cache
        mock_datacache.find.return_value = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "dagger",
                "--config",
                str(tmp_path / "config.yaml"),
                "--workflow",
                "filter",
                "--dag-dir",
                str(tmp_path),
            ],
        ):
            dagger.main()

        assert config.paths.filter_dir == config.paths.storage

    @mock.patch("sgnl.bin.dagger.DAG")
    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.load_svd_options")
    @mock.patch("sgnl.bin.dagger.create_time_bins")
    @mock.patch("sgnl.bin.dagger.prepare_osdf_support")
    @mock.patch("sgnl.bin.dagger.layers")
    @mock.patch("sgnl.bin.dagger.DataCache")
    @mock.patch("sgnl.bin.dagger.add_osdf_support_to_layer")
    @mock.patch("sgnl.bin.dagger.mchirp_range_to_bins")
    def test_main_workflow_injection_filter(
        self,
        mock_mchirp_range,
        mock_add_osdf,
        mock_datacache,
        mock_layers,
        mock_prepare_osdf,
        mock_create_time_bins,
        mock_load_svd,
        mock_build_config,
        mock_dag_class,
        tmp_path,
    ):
        """Test main with injection-filter workflow."""
        from sgnl.bin import dagger

        config = self._create_mock_config(with_injections=True)
        mock_build_config.return_value = config
        mock_load_svd.return_value = (["0", "1"], self._create_mock_svd_stats())
        mock_dag = mock.MagicMock()
        mock_dag_class.return_value = mock_dag
        mock_mchirp_range.return_value = ["0"]

        # Mock DataCache
        mock_trigger_cache = mock.MagicMock()
        mock_trigger_cache.__add__ = mock.MagicMock(return_value=mock_trigger_cache)
        mock_datacache.return_value = mock_trigger_cache
        mock_datacache.generate.return_value = mock.MagicMock()
        mock_datacache.find.return_value = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "dagger",
                "--config",
                str(tmp_path / "config.yaml"),
                "--workflow",
                "injection-filter",
                "--dag-dir",
                str(tmp_path),
            ],
        ):
            dagger.main()

        mock_dag_class.assert_called_once_with("sgnl_injection-filter")
        mock_layers.injection_filter.assert_called_once()
        mock_add_osdf.assert_called_with(
            mock_layers.injection_filter.return_value,
            config.source,
            is_injection_workflow=True,
        )

    @mock.patch("sgnl.bin.dagger.DAG")
    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.load_svd_options")
    @mock.patch("sgnl.bin.dagger.create_time_bins")
    @mock.patch("sgnl.bin.dagger.prepare_osdf_support")
    @mock.patch("sgnl.bin.dagger.layers")
    @mock.patch("sgnl.bin.dagger.DataCache")
    @mock.patch("sgnl.bin.dagger.mchirp_range_to_bins")
    @mock.patch("os.path.exists")
    @mock.patch("os.makedirs")
    def test_main_workflow_rank(
        self,
        mock_makedirs,
        mock_exists,
        mock_mchirp_range,
        mock_datacache,
        mock_layers,
        mock_prepare_osdf,
        mock_create_time_bins,
        mock_load_svd,
        mock_build_config,
        mock_dag_class,
        tmp_path,
    ):
        """Test main with rank workflow."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        mock_build_config.return_value = config
        mock_load_svd.return_value = (["0", "1"], self._create_mock_svd_stats())
        mock_dag = mock.MagicMock()
        mock_dag_class.return_value = mock_dag
        mock_mchirp_range.return_value = ["0"]
        mock_exists.return_value = False

        # Mock DataCache
        mock_cache = mock.MagicMock()
        mock_cache.__add__ = mock.MagicMock(return_value=mock_cache)
        mock_datacache.return_value = mock_cache
        mock_datacache.generate.return_value = mock_cache
        mock_datacache.find.return_value = mock_cache

        # Mock layers return values
        mock_layers.marginalize_pdf.return_value = [mock.MagicMock()]
        mock_layers.assign_far.return_value = [mock.MagicMock()]
        mock_layers.summary_page.return_value = [mock.MagicMock()]

        with mock.patch(
            "sys.argv",
            [
                "dagger",
                "--config",
                str(tmp_path / "config.yaml"),
                "--workflow",
                "rank",
                "--dag-dir",
                str(tmp_path),
            ],
        ):
            dagger.main()

        mock_dag_class.assert_called_once_with("sgnl_rank")
        mock_layers.create_prior.assert_called_once()
        mock_layers.add_trigger_dbs.assert_called_once()
        mock_layers.assign_likelihood.assert_called_once()
        mock_layers.merge_and_reduce.assert_called_once()
        mock_layers.calc_pdf.assert_called_once()
        mock_layers.extinct_bin.assert_called_once()
        mock_layers.marginalize_pdf.assert_called_once()
        mock_makedirs.assert_called_once()

    @mock.patch("sgnl.bin.dagger.DAG")
    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.load_svd_options")
    @mock.patch("sgnl.bin.dagger.create_time_bins")
    @mock.patch("sgnl.bin.dagger.prepare_osdf_support")
    @mock.patch("sgnl.bin.dagger.layers")
    @mock.patch("sgnl.bin.dagger.DataCache")
    @mock.patch("sgnl.bin.dagger.mchirp_range_to_bins")
    @mock.patch("os.path.exists")
    def test_main_workflow_rank_with_injections(
        self,
        mock_exists,
        mock_mchirp_range,
        mock_datacache,
        mock_layers,
        mock_prepare_osdf,
        mock_create_time_bins,
        mock_load_svd,
        mock_build_config,
        mock_dag_class,
        tmp_path,
    ):
        """Test main with rank workflow and injections."""
        from sgnl.bin import dagger

        config = self._create_mock_config(with_injections=True)
        mock_build_config.return_value = config
        mock_load_svd.return_value = (["0", "1"], self._create_mock_svd_stats())
        mock_dag = mock.MagicMock()
        mock_dag_class.return_value = mock_dag
        mock_mchirp_range.return_value = ["0"]
        mock_exists.return_value = True

        # Mock DataCache
        mock_cache = mock.MagicMock()
        mock_cache.__add__ = mock.MagicMock(return_value=mock_cache)
        mock_datacache.return_value = mock_cache
        mock_datacache.generate.return_value = mock_cache
        mock_datacache.find.return_value = mock_cache

        # Mock layers return values
        mock_layers.marginalize_pdf.return_value = [mock.MagicMock()]
        mock_layers.assign_far.return_value = [mock.MagicMock()]
        mock_layers.summary_page.return_value = [mock.MagicMock()]

        with mock.patch(
            "sys.argv",
            [
                "dagger",
                "--config",
                str(tmp_path / "config.yaml"),
                "--workflow",
                "rank",
                "--dag-dir",
                str(tmp_path),
            ],
        ):
            dagger.main()

        mock_dag_class.assert_called_once_with("sgnl_rank")
        # Verify injections-related calls
        mock_mchirp_range.assert_called()

    @mock.patch("sgnl.bin.dagger.DAG")
    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.load_svd_options")
    @mock.patch("sgnl.bin.dagger.create_time_bins")
    @mock.patch("sgnl.bin.dagger.prepare_osdf_support")
    @mock.patch("sgnl.bin.dagger.layers")
    @mock.patch("sgnl.bin.dagger.DataCache")
    @mock.patch("sgnl.bin.dagger.mchirp_range_to_bins")
    @mock.patch("os.path.exists")
    def test_main_workflow_rank_with_calc_pdf_jobs(
        self,
        mock_exists,
        mock_mchirp_range,
        mock_datacache,
        mock_layers,
        mock_prepare_osdf,
        mock_create_time_bins,
        mock_load_svd,
        mock_build_config,
        mock_dag_class,
        tmp_path,
    ):
        """Test main with rank workflow and calc_pdf_jobs > 1."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        config.rank.calc_pdf_jobs = 2
        mock_build_config.return_value = config
        mock_load_svd.return_value = (["0", "1"], self._create_mock_svd_stats())
        mock_dag = mock.MagicMock()
        mock_dag_class.return_value = mock_dag
        mock_mchirp_range.return_value = ["0"]
        mock_exists.return_value = True

        # Mock DataCache
        mock_cache = mock.MagicMock()
        mock_cache.__add__ = mock.MagicMock(return_value=mock_cache)
        mock_datacache.return_value = mock_cache
        mock_datacache.generate.return_value = mock_cache
        mock_datacache.find.return_value = mock_cache

        # Mock layers return values
        mock_layers.marginalize_pdf.return_value = [mock.MagicMock()]
        mock_layers.assign_far.return_value = [mock.MagicMock()]
        mock_layers.summary_page.return_value = [mock.MagicMock()]

        with mock.patch(
            "sys.argv",
            [
                "dagger",
                "--config",
                str(tmp_path / "config.yaml"),
                "--workflow",
                "rank",
                "--dag-dir",
                str(tmp_path),
            ],
        ):
            dagger.main()

        # Verify calc_pdf uses expanded bins for multiple jobs
        mock_layers.calc_pdf.assert_called_once()

    @mock.patch("sgnl.bin.dagger.DAG")
    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.load_svd_options")
    @mock.patch("sgnl.bin.dagger.create_time_bins")
    @mock.patch("sgnl.bin.dagger.prepare_osdf_support")
    def test_main_workflow_unknown(
        self,
        mock_prepare_osdf,
        mock_create_time_bins,
        mock_load_svd,
        mock_build_config,
        mock_dag_class,
        tmp_path,
    ):
        """Test main with unknown workflow raises error."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        config.source.frame_cache = None
        config.source.inj_frame_cache = None
        mock_build_config.return_value = config
        mock_load_svd.return_value = (["0", "1"], self._create_mock_svd_stats())
        mock_dag = mock.MagicMock()
        mock_dag_class.return_value = mock_dag

        with pytest.raises(ValueError, match="Unrecognized workflow"):
            with mock.patch(
                "sys.argv",
                [
                    "dagger",
                    "--config",
                    str(tmp_path / "config.yaml"),
                    "--workflow",
                    "unknown",
                    "--dag-dir",
                    str(tmp_path),
                ],
            ):
                dagger.main()

    @mock.patch("sgnl.bin.dagger.DAG")
    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.load_svd_options")
    @mock.patch("sgnl.bin.dagger.create_time_bins")
    @mock.patch("sgnl.bin.dagger.prepare_osdf_support")
    @mock.patch("sgnl.bin.dagger.layers")
    def test_main_with_custom_dag_name(
        self,
        mock_layers,
        mock_prepare_osdf,
        mock_create_time_bins,
        mock_load_svd,
        mock_build_config,
        mock_dag_class,
        tmp_path,
    ):
        """Test main with custom dag name."""
        from sgnl.bin import dagger

        config = self._create_mock_config()
        mock_build_config.return_value = config
        mock_load_svd.return_value = (["0", "1"], self._create_mock_svd_stats())
        mock_dag = mock.MagicMock()
        mock_dag_class.return_value = mock_dag

        with mock.patch(
            "sys.argv",
            [
                "dagger",
                "--config",
                str(tmp_path / "config.yaml"),
                "--workflow",
                "test",
                "--dag-name",
                "my_custom_dag",
                "--dag-dir",
                str(tmp_path),
            ],
        ):
            dagger.main()

        mock_dag_class.assert_called_once_with("my_custom_dag")

    @mock.patch("sgnl.bin.dagger.DAG")
    @mock.patch("sgnl.bin.dagger.build_config")
    @mock.patch("sgnl.bin.dagger.load_svd_options")
    @mock.patch("sgnl.bin.dagger.create_time_bins")
    @mock.patch("sgnl.bin.dagger.prepare_osdf_support")
    @mock.patch("sgnl.bin.dagger.layers")
    @mock.patch("sgnl.bin.dagger.DataCache")
    @mock.patch("sgnl.bin.dagger.add_osdf_support_to_layer")
    @mock.patch("sgnl.bin.dagger.mchirp_range_to_bins")
    def test_main_workflow_injection_filter_sets_injection_dir(
        self,
        mock_mchirp_range,
        mock_add_osdf,
        mock_datacache,
        mock_layers,
        mock_prepare_osdf,
        mock_create_time_bins,
        mock_load_svd,
        mock_build_config,
        mock_dag_class,
        tmp_path,
    ):
        """Test injection-filter workflow sets injection_dir if not set."""
        from sgnl.bin import dagger

        config = self._create_mock_config(with_injections=True)
        config.paths.injection_dir = None
        mock_build_config.return_value = config
        mock_load_svd.return_value = (["0", "1"], self._create_mock_svd_stats())
        mock_dag = mock.MagicMock()
        mock_dag_class.return_value = mock_dag
        mock_mchirp_range.return_value = ["0"]

        mock_trigger_cache = mock.MagicMock()
        mock_trigger_cache.__add__ = mock.MagicMock(return_value=mock_trigger_cache)
        mock_datacache.return_value = mock_trigger_cache
        mock_datacache.generate.return_value = mock.MagicMock()
        mock_datacache.find.return_value = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "dagger",
                "--config",
                str(tmp_path / "config.yaml"),
                "--workflow",
                "injection-filter",
                "--dag-dir",
                str(tmp_path),
            ],
        ):
            dagger.main()

        assert config.paths.injection_dir == config.paths.storage

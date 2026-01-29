"""Tests for sgnl.bin.ll_dagger"""

import sys
from unittest import mock

import pytest


@pytest.fixture(autouse=True)
def mock_dependencies():
    """Mock external dependencies and clean up after tests."""
    sys.modules.pop("sgnl.bin.ll_dagger", None)

    original_modules = {}

    # Create mocks for external dependencies
    ezdag_mock = mock.MagicMock()
    layers_mock = mock.MagicMock()
    config_mock = mock.MagicMock()
    util_mock = mock.MagicMock()

    # Create DataType enum mock
    data_type_mock = mock.MagicMock()
    data_type_mock.REFERENCE_PSD = "REFERENCE_PSD"
    data_type_mock.SPLIT_BANK = "SPLIT_BANK"
    data_type_mock.SVD_BANK = "SVD_BANK"
    data_type_mock.LIKELIHOOD_RATIO = "LIKELIHOOD_RATIO"
    data_type_mock.ZEROLAG_RANK_STAT_PDFS = "ZEROLAG_RANK_STAT_PDFS"
    data_type_mock.RANK_STAT_PDFS = "RANK_STAT_PDFS"
    util_mock.DataType = data_type_mock

    modules_to_mock = {
        "ezdag": ezdag_mock,
        "sgnl.dags": mock.MagicMock(),
        "sgnl.dags.layers": layers_mock,
        "sgnl.dags.config": config_mock,
        "sgnl.dags.util": util_mock,
    }

    for mod, mock_obj in modules_to_mock.items():
        original_modules[mod] = sys.modules.get(mod)
        sys.modules[mod] = mock_obj

    yield {
        "ezdag": ezdag_mock,
        "layers": layers_mock,
        "config": config_mock,
        "util": util_mock,
        "DataType": data_type_mock,
    }

    # Restore originals
    for mod in modules_to_mock:
        if original_modules[mod] is None:
            sys.modules.pop(mod, None)
        else:
            sys.modules[mod] = original_modules[mod]

    sys.modules.pop("sgnl.bin.ll_dagger", None)


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_required_args(self):
        """Test parsing with required arguments."""
        from sgnl.bin import ll_dagger

        args = ll_dagger.parse_command_line(["-c", "config.yaml", "-w", "setup"])
        assert args.config == "config.yaml"
        assert args.workflow == "setup"
        assert args.dag_dir == "."
        assert args.dag_name is None

    def test_config_long_option(self):
        """Test --config long option."""
        from sgnl.bin import ll_dagger

        args = ll_dagger.parse_command_line(
            ["--config", "config.yaml", "--workflow", "inspiral"]
        )
        assert args.config == "config.yaml"
        assert args.workflow == "inspiral"

    def test_dag_dir_option(self):
        """Test --dag-dir option."""
        from sgnl.bin import ll_dagger

        args = ll_dagger.parse_command_line(
            ["-c", "config.yaml", "-w", "setup", "--dag-dir", "/output/dir"]
        )
        assert args.dag_dir == "/output/dir"

    def test_dag_name_option(self):
        """Test --dag-name option."""
        from sgnl.bin import ll_dagger

        args = ll_dagger.parse_command_line(
            ["-c", "config.yaml", "-w", "setup", "--dag-name", "my_dag"]
        )
        assert args.dag_name == "my_dag"

    def test_dag_name_with_slash_raises(self):
        """Test that dag name with slash raises ValueError."""
        from sgnl.bin import ll_dagger

        with pytest.raises(ValueError, match="must not be a path"):
            ll_dagger.parse_command_line(
                ["-c", "config.yaml", "-w", "setup", "--dag-name", "path/to/dag"]
            )

    def test_dag_name_with_dag_extension_raises(self):
        """Test that dag name ending with .dag raises ValueError."""
        from sgnl.bin import ll_dagger

        with pytest.raises(ValueError, match='ends with ".dag"'):
            ll_dagger.parse_command_line(
                ["-c", "config.yaml", "-w", "setup", "--dag-name", "my_dag.dag"]
            )

    def test_init_and_workflow_raises(self):
        """Test that specifying both --init and --workflow raises ValueError."""
        from sgnl.bin import ll_dagger

        with pytest.raises(ValueError, match="Cannot specify both"):
            ll_dagger.parse_command_line(["-c", "config.yaml", "--init", "-w", "setup"])

    def test_neither_init_nor_workflow_raises(self):
        """Test that specifying neither --init nor --workflow raises ValueError."""
        from sgnl.bin import ll_dagger

        with pytest.raises(ValueError, match="Must specify either"):
            ll_dagger.parse_command_line(["-c", "config.yaml"])

    def test_uses_sys_argv_when_args_none(self):
        """Test that sys.argv is used when args is None."""
        from sgnl.bin import ll_dagger

        with mock.patch("sys.argv", ["ll_dagger", "-c", "config.yaml", "-w", "setup"]):
            args = ll_dagger.parse_command_line()
            assert args.config == "config.yaml"
            assert args.workflow == "setup"


class MockConfig:
    """Mock configuration object."""

    def __init__(self, **kwargs):
        # Set default attributes
        self.tag = "test_tag"
        self.ifos = ["H1", "L1"]
        self.instruments = ["H1", "L1"]
        self.all_ifos = {"H1", "L1"}
        self.span = (1000000000, 1000001000)

        # Set nested config objects
        self.paths = mock.MagicMock()
        self.paths.reference_psd = "/path/to/psd.xml"

        self.svd = mock.MagicMock()
        self.svd.option_file = "/path/to/options.json"

        self.condor = mock.MagicMock()
        self.filter = mock.MagicMock()
        self.filter.coincidence_threshold = 0.005
        self.filter.min_instruments_candidates = 2
        self.filter.injections = False

        self.prior = mock.MagicMock()
        self.psd = mock.MagicMock()
        self.upload = mock.MagicMock()
        self.services = mock.MagicMock()
        self.services.kafka_server = None
        self.source = mock.MagicMock()
        self.rank = mock.MagicMock()
        self.rank.extinct_percent = 0.1
        self.rank.fast_burnin = True
        self.rank.calc_pdf_cores = 4
        self.metrics = mock.MagicMock()
        self.pastro = mock.MagicMock()

        # Override with any provided kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)


class TestMain:
    """Tests for main function."""

    def test_main_setup_workflow(self, mock_dependencies):
        """Test main function with setup workflow."""
        from sgnl.bin import ll_dagger

        mock_config = MockConfig()
        mock_dag = mock.MagicMock()
        mock_svd_bins = ["bin0", "bin1"]
        mock_svd_stats = {"bin0": {}, "bin1": {}}

        with mock.patch.object(ll_dagger, "parse_command_line") as mock_parse:
            mock_parse.return_value = mock.MagicMock(
                config="config.yaml",
                workflow="setup",
                dag_dir="/output",
                dag_name=None,
                init=False,
            )
            with mock.patch.object(ll_dagger, "build_config", return_value=mock_config):
                with mock.patch.object(ll_dagger, "DAG", return_value=mock_dag):
                    with mock.patch.object(
                        ll_dagger,
                        "load_svd_options",
                        return_value=(mock_svd_bins, mock_svd_stats),
                    ):
                        with mock.patch.object(ll_dagger.DataCache, "from_files"):
                            with mock.patch.object(ll_dagger.DataCache, "find"):
                                with mock.patch.object(ll_dagger.DataCache, "generate"):
                                    with mock.patch.object(
                                        ll_dagger, "inspiral_bank_splitter"
                                    ):
                                        with mock.patch.object(
                                            ll_dagger, "inspiral_set_svdbin_option"
                                        ):
                                            ll_dagger.main()

        # Verify DAG operations
        mock_dag.attach.assert_called()
        mock_dag.write.assert_called_once()
        mock_dag.create_log_dir.assert_called_once()

    def test_main_setup_workflow_with_custom_dag_name(self, mock_dependencies):
        """Test main function with setup workflow and custom dag name."""
        from sgnl.bin import ll_dagger

        mock_config = MockConfig()
        mock_dag = mock.MagicMock()
        mock_svd_bins = ["bin0"]
        mock_svd_stats = {"bin0": {}}

        with mock.patch.object(ll_dagger, "parse_command_line") as mock_parse:
            mock_parse.return_value = mock.MagicMock(
                config="config.yaml",
                workflow="setup",
                dag_dir="/output",
                dag_name="custom_name",
                init=False,
            )
            with mock.patch.object(ll_dagger, "build_config", return_value=mock_config):
                with mock.patch.object(
                    ll_dagger, "DAG", return_value=mock_dag
                ) as dag_constructor:
                    with mock.patch.object(
                        ll_dagger,
                        "load_svd_options",
                        return_value=(mock_svd_bins, mock_svd_stats),
                    ):
                        with mock.patch.object(ll_dagger.DataCache, "from_files"):
                            with mock.patch.object(ll_dagger.DataCache, "find"):
                                with mock.patch.object(ll_dagger.DataCache, "generate"):
                                    with mock.patch.object(
                                        ll_dagger, "inspiral_bank_splitter"
                                    ):
                                        with mock.patch.object(
                                            ll_dagger, "inspiral_set_svdbin_option"
                                        ):
                                            ll_dagger.main()

        # Verify DAG was created with custom name
        dag_constructor.assert_called_once_with("custom_name")

    def test_main_setup_prior_workflow(self, mock_dependencies):
        """Test main function with setup-prior workflow."""
        from sgnl.bin import ll_dagger

        mock_config = MockConfig()
        mock_dag = mock.MagicMock()
        mock_svd_bins = ["bin0"]
        mock_svd_stats = {"bin0": {}}

        with mock.patch.object(ll_dagger, "parse_command_line") as mock_parse:
            mock_parse.return_value = mock.MagicMock(
                config="config.yaml",
                workflow="setup-prior",
                dag_dir="/output",
                dag_name=None,
                init=False,
            )
            with mock.patch.object(ll_dagger, "build_config", return_value=mock_config):
                with mock.patch.object(ll_dagger, "DAG", return_value=mock_dag):
                    with mock.patch.object(
                        ll_dagger,
                        "load_svd_options",
                        return_value=(mock_svd_bins, mock_svd_stats),
                    ):
                        with mock.patch.object(ll_dagger.DataCache, "find"):
                            with mock.patch.object(ll_dagger.DataCache, "generate"):
                                with mock.patch.object(
                                    ll_dagger, "inspiral_bank_splitter"
                                ):
                                    with mock.patch.object(
                                        ll_dagger, "inspiral_set_svdbin_option"
                                    ):
                                        ll_dagger.main()

        mock_dag.attach.assert_called()
        mock_dag.write.assert_called_once()

    def test_main_inspiral_workflow_basic(self, mock_dependencies):
        """Test main function with inspiral workflow (basic, no injections/kafka)."""
        from sgnl.bin import ll_dagger

        mock_config = MockConfig()
        mock_config.filter.injections = False
        mock_config.services.kafka_server = None

        mock_dag = mock.MagicMock()
        mock_svd_bins = ["bin0"]
        mock_svd_stats = {"bin0": {}}

        # Mock marg_zerolag_pdf to return exactly 1 item
        mock_marg_zerolag = [mock.MagicMock()]

        with mock.patch.object(ll_dagger, "parse_command_line") as mock_parse:
            mock_parse.return_value = mock.MagicMock(
                config="config.yaml",
                workflow="inspiral",
                dag_dir="/output",
                dag_name=None,
                init=False,
            )
            with mock.patch.object(ll_dagger, "build_config", return_value=mock_config):
                with mock.patch.object(ll_dagger, "DAG", return_value=mock_dag):
                    with mock.patch.object(
                        ll_dagger,
                        "load_svd_options",
                        return_value=(mock_svd_bins, mock_svd_stats),
                    ):
                        with mock.patch.object(
                            ll_dagger.DataCache,
                            "find",
                            side_effect=[
                                mock.MagicMock(),  # svd_banks
                                mock.MagicMock(),  # lrs
                                mock_marg_zerolag,  # marg_zerolag_pdf
                                mock.MagicMock(),  # zerolag_pdfs
                            ],
                        ):
                            with mock.patch.object(ll_dagger.DataCache, "generate"):
                                with mock.patch.object(
                                    ll_dagger, "inspiral_bank_splitter"
                                ):
                                    with mock.patch.object(
                                        ll_dagger, "inspiral_set_svdbin_option"
                                    ):
                                        ll_dagger.main()

        mock_dag.attach.assert_called()
        mock_dag.write.assert_called_once()

    def test_main_inspiral_workflow_with_injections(self, mock_dependencies):
        """Test main function with inspiral workflow including injections."""
        from sgnl.bin import ll_dagger

        mock_config = MockConfig()
        mock_config.filter.injections = True
        mock_config.services.kafka_server = None

        mock_dag = mock.MagicMock()
        mock_svd_bins = ["bin0"]
        mock_svd_stats = {"bin0": {}}
        mock_marg_zerolag = [mock.MagicMock()]

        with mock.patch.object(ll_dagger, "parse_command_line") as mock_parse:
            mock_parse.return_value = mock.MagicMock(
                config="config.yaml",
                workflow="inspiral",
                dag_dir="/output",
                dag_name=None,
                init=False,
            )
            with mock.patch.object(ll_dagger, "build_config", return_value=mock_config):
                with mock.patch.object(ll_dagger, "DAG", return_value=mock_dag):
                    with mock.patch.object(
                        ll_dagger,
                        "load_svd_options",
                        return_value=(mock_svd_bins, mock_svd_stats),
                    ):
                        with mock.patch.object(
                            ll_dagger.DataCache,
                            "find",
                            side_effect=[
                                mock.MagicMock(),
                                mock.MagicMock(),
                                mock_marg_zerolag,
                                mock.MagicMock(),
                            ],
                        ):
                            with mock.patch.object(ll_dagger.DataCache, "generate"):
                                with mock.patch.object(
                                    ll_dagger, "inspiral_bank_splitter"
                                ):
                                    with mock.patch.object(
                                        ll_dagger, "inspiral_set_svdbin_option"
                                    ):
                                        ll_dagger.main()

        # With injections, should have more attach calls
        assert mock_dag.attach.call_count >= 3

    def test_main_inspiral_workflow_with_kafka(self, mock_dependencies):
        """Test main function with inspiral workflow including kafka services."""
        from sgnl.bin import ll_dagger

        mock_config = MockConfig()
        mock_config.filter.injections = False
        mock_config.services.kafka_server = "kafka:9092"

        mock_dag = mock.MagicMock()
        mock_svd_bins = ["bin0"]
        mock_svd_stats = {"bin0": {}}
        mock_marg_zerolag = [mock.MagicMock()]

        with mock.patch.object(ll_dagger, "parse_command_line") as mock_parse:
            mock_parse.return_value = mock.MagicMock(
                config="config.yaml",
                workflow="inspiral",
                dag_dir="/output",
                dag_name=None,
                init=False,
            )
            with mock.patch.object(ll_dagger, "build_config", return_value=mock_config):
                with mock.patch.object(ll_dagger, "DAG", return_value=mock_dag):
                    with mock.patch.object(
                        ll_dagger,
                        "load_svd_options",
                        return_value=(mock_svd_bins, mock_svd_stats),
                    ):
                        with mock.patch.object(
                            ll_dagger.DataCache,
                            "find",
                            side_effect=[
                                mock.MagicMock(),
                                mock.MagicMock(),
                                mock_marg_zerolag,
                                mock.MagicMock(),
                            ],
                        ):
                            with mock.patch.object(ll_dagger.DataCache, "generate"):
                                with mock.patch.object(
                                    ll_dagger, "inspiral_bank_splitter"
                                ):
                                    with mock.patch.object(
                                        ll_dagger, "inspiral_set_svdbin_option"
                                    ):
                                        ll_dagger.main()

        # With kafka, should have many more attach calls for event handling
        assert mock_dag.attach.call_count >= 6

    def test_main_inspiral_workflow_with_injections_and_kafka(self, mock_dependencies):
        """Test inspiral workflow with both injections and kafka."""
        from sgnl.bin import ll_dagger

        mock_config = MockConfig()
        mock_config.filter.injections = True
        mock_config.services.kafka_server = "kafka:9092"

        mock_dag = mock.MagicMock()
        mock_svd_bins = ["bin0"]
        mock_svd_stats = {"bin0": {}}
        mock_marg_zerolag = [mock.MagicMock()]

        with mock.patch.object(ll_dagger, "parse_command_line") as mock_parse:
            mock_parse.return_value = mock.MagicMock(
                config="config.yaml",
                workflow="inspiral",
                dag_dir="/output",
                dag_name=None,
                init=False,
            )
            with mock.patch.object(ll_dagger, "build_config", return_value=mock_config):
                with mock.patch.object(ll_dagger, "DAG", return_value=mock_dag):
                    with mock.patch.object(
                        ll_dagger,
                        "load_svd_options",
                        return_value=(mock_svd_bins, mock_svd_stats),
                    ):
                        with mock.patch.object(
                            ll_dagger.DataCache,
                            "find",
                            side_effect=[
                                mock.MagicMock(),
                                mock.MagicMock(),
                                mock_marg_zerolag,
                                mock.MagicMock(),
                            ],
                        ):
                            with mock.patch.object(ll_dagger.DataCache, "generate"):
                                with mock.patch.object(
                                    ll_dagger, "inspiral_bank_splitter"
                                ):
                                    with mock.patch.object(
                                        ll_dagger, "inspiral_set_svdbin_option"
                                    ):
                                        ll_dagger.main()

        # With both injections and kafka, should have the most attach calls
        assert mock_dag.attach.call_count >= 8

    def test_main_unrecognized_workflow_raises(self, mock_dependencies):
        """Test that unrecognized workflow raises ValueError."""
        from sgnl.bin import ll_dagger

        mock_config = MockConfig()

        with mock.patch.object(ll_dagger, "parse_command_line") as mock_parse:
            mock_parse.return_value = mock.MagicMock(
                config="config.yaml",
                workflow="invalid_workflow",
                dag_dir="/output",
                dag_name=None,
                init=False,
            )
            with mock.patch.object(ll_dagger, "build_config", return_value=mock_config):
                with mock.patch.object(ll_dagger, "DAG"):
                    with mock.patch.object(ll_dagger, "inspiral_bank_splitter"):
                        with mock.patch.object(ll_dagger, "inspiral_set_svdbin_option"):
                            with pytest.raises(
                                ValueError, match="Unrecognized workflow"
                            ):
                                ll_dagger.main()

    def test_main_init_workflow(self, mock_dependencies):
        """Test main function with --init flag."""
        from sgnl.bin import ll_dagger

        mock_config = MockConfig()
        # Set up SVD config attributes needed for init workflow
        mock_config.svd.max_f_final = 1024
        mock_config.svd.f_low = 15
        mock_config.svd.num_split_templates = 100
        mock_config.svd.num_banks = 4
        mock_config.svd.sort_by = "chi"
        mock_config.svd.num_chi_bins = 10
        mock_config.svd.num_mu_bins = 5
        mock_config.svd.overlap = 0.1
        mock_config.svd.approximant = "IMRPhenomD"
        mock_config.paths.template_bank = "/path/to/bank.xml"

        with mock.patch.object(ll_dagger, "parse_command_line") as mock_parse:
            mock_parse.return_value = mock.MagicMock(
                config="config.yaml",
                workflow=None,
                dag_dir="/output",
                dag_name=None,
                init=True,
            )
            with mock.patch.object(ll_dagger, "build_config", return_value=mock_config):
                with mock.patch.object(
                    ll_dagger, "inspiral_bank_splitter"
                ) as mock_splitter:
                    with mock.patch.object(
                        ll_dagger, "inspiral_set_svdbin_option"
                    ) as mock_svdbin:
                        ll_dagger.main()

        # Verify the init workflow called the right functions
        mock_splitter.split_bank.assert_called_once()
        mock_svdbin.set_svdbin_option.assert_called_once_with(mock_config)

    def test_main_init_workflow_with_mu_sort(self, mock_dependencies):
        """Test main function with --init flag and mu sorting."""
        from sgnl.bin import ll_dagger

        mock_config = MockConfig()
        mock_config.svd.max_f_final = 1024
        mock_config.svd.f_low = 15
        mock_config.svd.num_split_templates = 100
        mock_config.svd.num_banks = [2, 4]
        mock_config.svd.sort_by = "mu"
        mock_config.svd.num_chi_bins = 10
        mock_config.svd.num_mu_bins = 5
        mock_config.svd.overlap = None
        mock_config.svd.approximant = "IMRPhenomD"
        mock_config.paths.template_bank = "/path/to/bank.xml"

        with mock.patch.object(ll_dagger, "parse_command_line") as mock_parse:
            mock_parse.return_value = mock.MagicMock(
                config="config.yaml",
                workflow=None,
                dag_dir="/output",
                dag_name=None,
                init=True,
            )
            with mock.patch.object(ll_dagger, "build_config", return_value=mock_config):
                with mock.patch.object(
                    ll_dagger, "inspiral_bank_splitter"
                ) as mock_splitter:
                    with mock.patch.object(ll_dagger, "inspiral_set_svdbin_option"):
                        ll_dagger.main()

        mock_splitter.split_bank.assert_called_once()

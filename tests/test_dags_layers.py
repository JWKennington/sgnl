"""Tests for sgnl.dags.layers"""

import sys
from unittest import mock

import pytest

# Mock ezdag before importing layers
mock_ezdag = mock.MagicMock()
sys.modules["ezdag"] = mock_ezdag

from sgnl.dags import layers  # noqa: E402


class MockCondorConfig:
    """Mock condor configuration."""

    def __init__(self):
        self.accounting_group = "ligo.dev.o4.test"
        self.accounting_group_user = "testuser"
        self.transfer_files = None
        self.getenv = None
        self.use_scitokens = False
        self.container = None

    def __contains__(self, key):
        return hasattr(self, key)

    def __getitem__(self, key):
        return getattr(self, key)


class MockSvdStats(dict):
    """Mock SVD stats that supports both attribute and dict access."""

    def __init__(self):
        super().__init__()
        self["bins"] = {
            "0000": {
                "ht_gate_threshold": 50.0,
                "mean_mchirp": 10.0,
                "ac_length": 701,
            }
        }
        self.bins = self["bins"]


@pytest.fixture
def mock_condor_config():
    return MockCondorConfig()


@pytest.fixture
def mock_svd_stats():
    return MockSvdStats()


class TestCreateSubmitDescription:
    """Tests for create_submit_description function."""

    def test_basic_submit_description(self, mock_condor_config):
        """Test basic submit description creation."""
        result = layers.create_submit_description(mock_condor_config)

        assert "want_graceful_removal" in result
        assert result["want_graceful_removal"] == "True"
        assert result["kill_sig"] == "15"
        assert result["accounting_group"] == "ligo.dev.o4.test"
        assert result["accounting_group_user"] == "testuser"

    def test_submit_description_with_getenv(self, mock_condor_config):
        """Test submit description with getenv option."""
        mock_condor_config.getenv = "True"
        result = layers.create_submit_description(mock_condor_config)
        assert result["getenv"] == "True"

    def test_submit_description_with_container(self):
        """Test submit description with container option."""
        config = MockCondorConfig()
        config.container = "/path/to/container.sif"
        result = layers.create_submit_description(config)
        assert result["MY.SingularityImage"] == '"/path/to/container.sif"'
        assert result["transfer_executable"] is False

    def test_submit_description_with_scitokens(self):
        """Test submit description with scitokens option."""
        config = MockCondorConfig()
        config.use_scitokens = True
        result = layers.create_submit_description(config)
        assert result["use_oauth_services"] == "scitokens"
        assert "BEARER_TOKEN_FILE" in result["environment"]

    def test_submit_description_with_directives(self):
        """Test submit description with directives."""
        config = MockCondorConfig()
        config.directives = {"custom_directive": "value"}
        result = layers.create_submit_description(config)
        assert result["custom_directive"] == "value"

    def test_submit_description_with_requirements(self):
        """Test submit description with requirements."""
        config = MockCondorConfig()
        config.requirements = ["req1", "req2"]
        result = layers.create_submit_description(config)
        assert result["requirements"] == "req1 && req2"

    def test_submit_description_with_environment(self):
        """Test submit description with environment variables."""
        config = MockCondorConfig()
        config.environment = {"MY_VAR": "myvalue"}
        result = layers.create_submit_description(config)
        assert "MY_VAR=myvalue" in result["environment"]


class TestCreateLayer:
    """Tests for create_layer function."""

    def test_create_layer_basic(self, mock_condor_config):
        """Test basic layer creation."""
        resource_requests = {
            "request_cpus": 1,
            "request_memory": "2GB",
            "request_disk": "1GB",
        }
        layer = layers.create_layer(
            "test-executable", mock_condor_config, resource_requests
        )
        assert layer is not None

    def test_create_layer_with_name(self, mock_condor_config):
        """Test layer creation with custom name."""
        resource_requests = {
            "request_cpus": 1,
            "request_memory": "2GB",
            "request_disk": "1GB",
        }
        layer = layers.create_layer(
            "test-executable",
            mock_condor_config,
            resource_requests,
            name="custom-name",
        )
        assert layer is not None

    def test_create_layer_with_retries(self, mock_condor_config):
        """Test layer creation with custom retries."""
        resource_requests = {
            "request_cpus": 1,
            "request_memory": "2GB",
            "request_disk": "1GB",
        }
        layer = layers.create_layer(
            "test-executable", mock_condor_config, resource_requests, retries=10
        )
        assert layer is not None

    def test_create_layer_with_transfer_files(self):
        """Test layer creation with transfer_files option."""
        config = MockCondorConfig()
        config.transfer_files = False
        resource_requests = {
            "request_cpus": 1,
            "request_memory": "2GB",
            "request_disk": "1GB",
        }
        layer = layers.create_layer("test-executable", config, resource_requests)
        assert layer is not None

    def test_create_layer_with_executable_config(self):
        """Test layer creation with executable-specific config."""
        config = MockCondorConfig()
        setattr(config, "test-executable", {"request_memory": "4GB"})
        resource_requests = {
            "request_cpus": 1,
            "request_memory": "2GB",
            "request_disk": "1GB",
        }
        layer = layers.create_layer("test-executable", config, resource_requests)
        assert layer is not None


class TestTestLayer:
    """Tests for test function."""

    def test_test_layer_creation(self, mock_condor_config):
        """Test test layer creation."""
        echo_config = mock.MagicMock()
        echo_config.jobs = 2

        layer = layers.test(echo_config, mock_condor_config)

        assert layer is not None


class TestReferencePsd:
    """Tests for reference_psd function."""

    def test_reference_psd_basic(self, mock_condor_config):
        """Test reference_psd layer creation."""
        filter_config = mock.MagicMock()
        filter_config.search = None

        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        source_config = mock.MagicMock()
        source_config.frame_segments_name = "datasegments"
        source_config.frame_segments_file = "/path/to/segments.xml"
        source_config.frame_cache = "/path/to/cache.lcf"
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}

        ref_psd_cache = mock.MagicMock()
        ref_psd_cache.groupby.return_value = {
            ("H1L1", (1000, 2000)): mock.MagicMock(files=["psd1.xml.gz"])
        }

        layer = layers.reference_psd(
            filter_config, psd_config, source_config, mock_condor_config, ref_psd_cache
        )

        assert layer is not None

    def test_reference_psd_with_search(self, mock_condor_config):
        """Test reference_psd with search option."""
        filter_config = mock.MagicMock()
        filter_config.search = "ew"

        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        source_config = mock.MagicMock()
        source_config.frame_segments_name = "datasegments"
        source_config.frame_segments_file = "/path/to/segments.xml"
        source_config.frame_cache = None
        source_config.frame_type = {"H1": "H1_HOFT_C00", "L1": "L1_HOFT_C00"}
        source_config.data_find_server = "datafind.ligo.org"
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}

        ref_psd_cache = mock.MagicMock()
        ref_psd_cache.groupby.return_value = {
            ("H1L1", (1000, 2000)): mock.MagicMock(files=["psd1.xml.gz"])
        }

        layer = layers.reference_psd(
            filter_config, psd_config, source_config, mock_condor_config, ref_psd_cache
        )

        assert layer is not None


class TestMedianPsd:
    """Tests for median_psd function."""

    def test_median_psd_basic(self, mock_condor_config):
        """Test median_psd layer creation."""
        psd_config = mock.MagicMock()

        ref_psd_cache = mock.MagicMock()
        ref_psd_cache.files = ["psd1.xml.gz", "psd2.xml.gz"]

        median_psd_cache = mock.MagicMock()
        median_psd_cache.files = ["median_psd.xml.gz"]

        layer = layers.median_psd(
            psd_config, mock_condor_config, ref_psd_cache, median_psd_cache
        )

        assert layer is not None


class TestSvdBank:
    """Tests for svd_bank function."""

    def test_svd_bank_basic(self, mock_condor_config, mock_svd_stats):
        """Test svd_bank layer creation."""
        svd_config = mock.MagicMock()
        svd_config.f_low = 15.0
        svd_config.max_f_final = 1024.0
        svd_config.approximant = "TaylorF2"
        svd_config.overlap = 0.01
        svd_config.num_split_templates = 100
        svd_config.num_banks = 10
        svd_config.sort_by = "mchirp"
        svd_config.autocorrelation_length = None
        svd_config.samples_min = 32
        svd_config.samples_max_64 = 64
        svd_config.samples_max_256 = 256
        svd_config.samples_max = 1024
        svd_config.tolerance = 0.99

        all_ifos = "H1L1"

        split_bank_cache = mock.MagicMock()
        split_bank_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["split_bank.xml.gz"])
        }

        median_psd_cache = mock.MagicMock()
        median_psd_cache.files = ["median_psd.xml.gz"]

        svd_cache = mock.MagicMock()
        svd_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_bank.xml.gz"])
        }

        svd_bins = ["0000"]

        layer = layers.svd_bank(
            svd_config,
            mock_condor_config,
            all_ifos,
            split_bank_cache,
            median_psd_cache,
            svd_cache,
            svd_bins,
            mock_svd_stats,
        )

        assert layer is not None


class TestFilter:
    """Tests for filter function."""

    def test_filter_basic(self, mock_condor_config, mock_svd_stats):
        """Test filter layer creation."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        svd_config = mock.MagicMock()

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snr_min = 4.0
        filter_config.all_triggers_to_background = False
        filter_config.search = None
        filter_config.torch_dtype = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = None
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.idq_gate_threshold = None

        source_config = mock.MagicMock()
        source_config.frame_segments_name = "datasegments"
        source_config.frame_segments_file = "/path/to/segments.xml"
        source_config.frame_cache = "/path/to/cache.lcf"
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.idq_channel_name = None
        source_config.idq_state_channel_name = None

        ref_psd_cache = mock.MagicMock()
        ref_psd_cache.groupby.return_value = {
            ("H1L1", (1000, 2000)): mock.MagicMock(files=["psd.xml.gz"])
        }

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_h1.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd_l1.xml.gz"]),
        }

        lr_cache = mock.MagicMock()
        lr_cache.groupby.return_value = {
            ("H1L1", (1000, 2000), "0000"): mock.MagicMock(files=["lr.xml.gz"])
        }

        trigger_cache = mock.MagicMock()
        trigger_group = mock.MagicMock()
        trigger_group.groupby.return_value = {"0000": mock.MagicMock()}
        trigger_group.files = ["triggers.sqlite.gz"]

        def mock_chunked(n):
            yield trigger_group

        trigger_inner = mock.MagicMock(files=["triggers.sqlite.gz"])
        trigger_inner.chunked = mock_chunked
        trigger_cache.groupby.return_value = {("H1L1", (1000, 2000)): trigger_inner}

        layer = layers.filter(
            psd_config,
            svd_config,
            filter_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            lr_cache,
            trigger_cache,
            mock_svd_stats,
            min_instruments=2,
        )

        assert layer is not None

    def test_filter_with_cuda(self, mock_condor_config, mock_svd_stats):
        """Test filter layer with CUDA device."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        svd_config = mock.MagicMock()

        filter_config = mock.MagicMock()
        filter_config.torch_device = "cuda:0"
        filter_config.coincidence_threshold = 0.005
        filter_config.snr_min = 4.0
        filter_config.all_triggers_to_background = False
        filter_config.search = None
        filter_config.torch_dtype = "float32"
        filter_config.trigger_finding_duration = 1
        filter_config.group_svd_num = 1
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.idq_gate_threshold = None

        source_config = mock.MagicMock()
        source_config.frame_segments_name = "datasegments"
        source_config.frame_segments_file = "/path/to/segments.xml"
        source_config.frame_cache = "/path/to/cache.lcf"
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.idq_channel_name = None
        source_config.idq_state_channel_name = None

        ref_psd_cache = mock.MagicMock()
        ref_psd_cache.groupby.return_value = {}

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {}

        lr_cache = mock.MagicMock()
        lr_cache.groupby.return_value = {}

        trigger_cache = mock.MagicMock()
        trigger_cache.groupby.return_value = {}

        layer = layers.filter(
            psd_config,
            svd_config,
            filter_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            lr_cache,
            trigger_cache,
            mock_svd_stats,
            min_instruments=2,
        )

        assert layer is not None


class TestInjectionFilter:
    """Tests for injection_filter function."""

    def test_injection_filter_basic(self, mock_condor_config, mock_svd_stats):
        """Test injection_filter layer creation."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        svd_config = mock.MagicMock()

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snr_min = 4.0
        filter_config.search = None
        filter_config.torch_dtype = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = None
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.idq_gate_threshold = None

        injection_config = mock.MagicMock()
        injection_config.filter = {
            "bbh": {"file": "/path/to/inj.xml", "noiseless_inj_frames": False}
        }

        source_config = mock.MagicMock()
        source_config.frame_segments_name = "datasegments"
        source_config.frame_segments_file = "/path/to/segments.xml"
        source_config.frame_cache = "/path/to/cache.lcf"
        source_config.inj_frame_cache = None
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.inj_channel_name = {"H1": "H1:INJ", "L1": "L1:INJ"}
        source_config.idq_channel_name = None
        source_config.idq_state_channel_name = None

        ref_psd_cache = mock.MagicMock()
        ref_psd_cache.groupby.return_value = {
            ("H1L1", (1000, 2000)): mock.MagicMock(files=["psd.xml.gz"])
        }

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_h1.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd_l1.xml.gz"]),
        }

        trigger_cache = mock.MagicMock()
        trigger_group = mock.MagicMock()
        trigger_group.groupby.return_value = {"0000": mock.MagicMock()}
        trigger_group.files = ["triggers.sqlite.gz"]

        def mock_chunked(n):
            yield trigger_group

        trigger_inner = mock.MagicMock(files=["triggers.sqlite.gz"])
        trigger_inner.chunked = mock_chunked
        trigger_cache.groupby.return_value = {
            ("H1L1", (1000, 2000), "bbh"): trigger_inner
        }

        layer = layers.injection_filter(
            psd_config,
            svd_config,
            filter_config,
            injection_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            trigger_cache,
            mock_svd_stats,
            min_instruments=2,
        )

        assert layer is not None


class TestAggregate:
    """Tests for aggregate function."""

    def test_aggregate_basic(self, mock_condor_config):
        """Test aggregate layer creation."""
        filter_config = mock.MagicMock()
        filter_config.event_config_file = "/path/to/event_config.yaml"

        trigger_cache = mock.MagicMock()
        trigger_cache.groupby.return_value = {
            ("H1L1", (1000, 2000), "0000"): mock.MagicMock(files=["triggers.sqlite.gz"])
        }

        clustered_triggers_cache = mock.MagicMock()
        clustered_triggers_cache.groupby.return_value = {
            ("H1L1", (1000, 2000), "0000"): mock.MagicMock(
                files=["clustered.sqlite.gz"]
            )
        }

        layer = layers.aggregate(
            filter_config, mock_condor_config, trigger_cache, clustered_triggers_cache
        )

        assert layer is not None


class TestMarginalizeLikelihoodRatio:
    """Tests for marginalize_likelihood_ratio function."""

    def test_marginalize_likelihood_ratio_basic(self, mock_condor_config):
        """Test marginalize_likelihood_ratio layer creation."""
        lr_cache = mock.MagicMock()
        lr_cache.groupby.return_value = {"0000": mock.MagicMock(files=["lr.xml.gz"])}

        marg_lr_cache = mock.MagicMock()
        marg_lr_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["marg_lr.xml.gz"])
        }

        layer = layers.marginalize_likelihood_ratio(
            mock_condor_config, lr_cache, marg_lr_cache
        )

        assert layer is not None

    def test_marginalize_likelihood_ratio_with_prior(self, mock_condor_config):
        """Test marginalize_likelihood_ratio with prior cache."""
        lr_cache = mock.MagicMock()
        lr_cache.groupby.return_value = {"0000": mock.MagicMock(files=["lr.xml.gz"])}

        marg_lr_cache = mock.MagicMock()
        marg_lr_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["marg_lr.xml.gz"])
        }

        prior_cache = mock.MagicMock()
        prior_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["prior.xml.gz"])
        }

        layer = layers.marginalize_likelihood_ratio(
            mock_condor_config, lr_cache, marg_lr_cache, prior_cache=prior_cache
        )

        assert layer is not None


class TestCreatePrior:
    """Tests for create_prior function."""

    def test_create_prior_basic(self, mock_condor_config, mock_svd_stats):
        """Test create_prior layer creation."""
        filter_config = mock.MagicMock()
        filter_config.search = None

        prior_config = mock.MagicMock()
        prior_config.mass_model = "/path/to/mass_model.h5"
        prior_config.idq_timeseries = None
        prior_config.dtdphi = None

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["svd.xml.gz"])
        }

        prior_cache = mock.MagicMock()
        prior_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["prior.xml.gz"])
        }

        layer = layers.create_prior(
            filter_config,
            mock_condor_config,
            prior_config,
            coincidence_threshold=0.005,
            svd_bank_cache=svd_bank_cache,
            prior_cache=prior_cache,
            ifos=["H1", "L1"],
            min_instruments=2,
            svd_stats=mock_svd_stats,
        )

        assert layer is not None

    def test_create_prior_with_search(self, mock_condor_config, mock_svd_stats):
        """Test create_prior with search option (line 761)."""
        filter_config = mock.MagicMock()
        filter_config.search = "AllSky"  # Enable search option

        prior_config = mock.MagicMock()
        prior_config.mass_model = "/path/to/mass_model.h5"
        prior_config.idq_timeseries = None
        prior_config.dtdphi = None

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["svd.xml.gz"])
        }

        prior_cache = mock.MagicMock()
        prior_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["prior.xml.gz"])
        }

        layer = layers.create_prior(
            filter_config,
            mock_condor_config,
            prior_config,
            coincidence_threshold=0.005,
            svd_bank_cache=svd_bank_cache,
            prior_cache=prior_cache,
            ifos=["H1", "L1"],
            min_instruments=2,
            svd_stats=mock_svd_stats,
        )

        assert layer is not None

    def test_create_prior_with_write_empty_zerolag(
        self, mock_condor_config, mock_svd_stats
    ):
        """Test create_prior with write_empty_zerolag options (lines 762-790)."""
        filter_config = mock.MagicMock()
        filter_config.search = None

        prior_config = mock.MagicMock()
        prior_config.mass_model = "/path/to/mass_model.h5"
        prior_config.idq_timeseries = None
        prior_config.dtdphi = None

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["svd.xml.gz"]),
            "0001": mock.MagicMock(files=["svd2.xml.gz"]),  # Add second bin
        }

        prior_cache = mock.MagicMock()
        prior_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["prior.xml.gz"]),
            "0001": mock.MagicMock(files=["prior2.xml.gz"]),  # Add second bin
        }

        # Mock write_empty_zerolag cache
        write_empty_zerolag = mock.MagicMock()
        write_empty_zerolag.groupby.return_value = {
            "0000": mock.MagicMock(files=["zerolag_0000.xml.gz"]),
            "0001": mock.MagicMock(files=["zerolag_0001.xml.gz"]),
        }

        # Mock write_empty_marg_zerolag cache
        write_empty_marg_zerolag = mock.MagicMock()
        write_empty_marg_zerolag.files = ["marg_zerolag.xml.gz"]

        # Need svd_stats with both bins
        svd_stats_multi = MockSvdStats()
        svd_stats_multi.bins["0001"] = {
            "mean_mchirp": 15.0,
            "ht_gate_threshold": 50.0,
            "ac_length": 701,
        }

        layer = layers.create_prior(
            filter_config,
            mock_condor_config,
            prior_config,
            coincidence_threshold=0.005,
            svd_bank_cache=svd_bank_cache,
            prior_cache=prior_cache,
            ifos=["H1", "L1"],
            min_instruments=2,
            svd_stats=svd_stats_multi,
            write_empty_zerolag=write_empty_zerolag,
            write_empty_marg_zerolag=write_empty_marg_zerolag,
        )

        assert layer is not None


class TestAddTriggerDbs:
    """Tests for add_trigger_dbs function."""

    def test_add_trigger_dbs_basic(self, mock_condor_config):
        """Test add_trigger_dbs layer creation."""
        filter_config = mock.MagicMock()
        filter_config.event_config_file = "/path/to/event_config.yaml"

        trigger_cache = mock.MagicMock()
        trigger_cache.groupby.return_value = {
            ("0000", ""): mock.MagicMock(files=["triggers.sqlite.gz"])
        }

        clustered_trigger_cache = mock.MagicMock()
        clustered_trigger_cache.groupby.return_value = {
            ("0000", ""): mock.MagicMock(files=["clustered.sqlite.gz"])
        }

        layer = layers.add_trigger_dbs(
            mock_condor_config,
            filter_config,
            trigger_cache,
            clustered_trigger_cache,
            column="network_snr",
            window=0.1,
        )

        assert layer is not None


class TestMergeAndReduce:
    """Tests for merge_and_reduce function."""

    def test_merge_and_reduce_basic(self, mock_condor_config):
        """Test merge_and_reduce layer creation."""
        filter_config = mock.MagicMock()
        filter_config.event_config_file = "/path/to/event_config.yaml"

        trigger_cache = mock.MagicMock()
        trigger_cache.groupby.return_value = {
            "": mock.MagicMock(files=["triggers.sqlite.gz"])
        }

        clustered_trigger_cache = mock.MagicMock()
        clustered_trigger_cache.groupby.return_value = {
            "": mock.MagicMock(files=["clustered.sqlite.gz"])
        }

        layer = layers.merge_and_reduce(
            mock_condor_config,
            filter_config,
            trigger_cache,
            clustered_trigger_cache,
            column="network_snr",
            window=0.1,
        )

        assert layer is not None


class TestAssignLikelihood:
    """Tests for assign_likelihood function."""

    def test_assign_likelihood_basic(self, mock_condor_config, mock_svd_stats):
        """Test assign_likelihood layer creation."""
        filter_config = mock.MagicMock()
        filter_config.event_config_file = "/path/to/event_config.yaml"

        prior_config = mock.MagicMock()
        prior_config.mass_model = "/path/to/mass_model.h5"
        prior_config.idq_timeseries = None
        prior_config.dtdphi = None

        trigger_cache = mock.MagicMock()
        trigger_cache.groupby.return_value = {
            ("0000", ""): mock.MagicMock(files=["triggers.sqlite.gz"])
        }

        lr_cache = mock.MagicMock()
        lr_cache.groupby.return_value = {"0000": mock.MagicMock(files=["lr.xml.gz"])}

        lr_trigger_cache = mock.MagicMock()
        lr_trigger_cache.groupby.return_value = {
            ("0000", ""): mock.MagicMock(files=["lr_triggers.sqlite.gz"])
        }

        layer = layers.assign_likelihood(
            mock_condor_config,
            filter_config,
            prior_config,
            trigger_cache,
            lr_cache,
            lr_trigger_cache,
            mock_svd_stats,
        )

        assert layer is not None


class TestCalcPdf:
    """Tests for calc_pdf function."""

    def test_calc_pdf_basic(self, mock_condor_config, mock_svd_stats):
        """Test calc_pdf layer creation."""
        prior_config = mock.MagicMock()
        prior_config.mass_model = "/path/to/mass_model.h5"
        prior_config.idq_timeseries = None
        prior_config.dtdphi = None

        rank_config = mock.MagicMock()
        rank_config.calc_pdf_cores = 1
        rank_config.ranking_stat_samples = 1000000

        lr_cache = mock.MagicMock()
        lr_cache.groupby.return_value = {"0000": mock.MagicMock(files=["lr.xml.gz"])}

        pdf_cache = mock.MagicMock()
        pdf_cache.groupby.return_value = {"0000": mock.MagicMock(files=["pdf.xml.gz"])}

        layer = layers.calc_pdf(
            mock_condor_config,
            prior_config,
            rank_config,
            config_svd_bins=["0000"],
            lr_cache=lr_cache,
            pdf_cache=pdf_cache,
            svd_stats=mock_svd_stats,
        )

        assert layer is not None


class TestExtinctBin:
    """Tests for extinct_bin function."""

    def test_extinct_bin_basic(self, mock_condor_config):
        """Test extinct_bin layer creation."""
        event_config_file = "/path/to/event_config.yaml"

        pdf_cache = mock.MagicMock()
        pdf_cache.groupby.return_value = {"0000": mock.MagicMock(files=["pdf.xml.gz"])}

        trigger_cache = mock.MagicMock()
        trigger_by_subtype = mock.MagicMock()
        trigger_by_subtype.groupby.return_value = {
            "0000": mock.MagicMock(files=["triggers.sqlite.gz"])
        }
        trigger_cache.groupby.return_value = {"": trigger_by_subtype}

        extinct_cache = mock.MagicMock()
        extinct_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["extinct.xml.gz"])
        }

        layer = layers.extinct_bin(
            mock_condor_config,
            event_config_file,
            pdf_cache,
            trigger_cache,
            extinct_cache,
        )

        assert layer is not None


class TestMarginalizePdf:
    """Tests for marginalize_pdf function."""

    def test_marginalize_pdf_single_round(self, mock_condor_config):
        """Test marginalize_pdf with single round."""
        rank_config = mock.MagicMock()
        rank_config.marg_pdf_files = 1000

        pdf_cache = mock.MagicMock()
        pdf_cache.files = ["pdf1.xml.gz", "pdf2.xml.gz"]

        def mock_chunked(n):
            yield pdf_cache

        pdf_cache.chunked = mock_chunked

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        result = layers.marginalize_pdf(
            mock_condor_config,
            rank_config,
            rank_dir="/path/to/rank",
            all_ifos="H1L1",
            span=(1000, 2000),
            pdf_cache=pdf_cache,
            marg_pdf_cache=marg_pdf_cache,
        )

        assert len(result) == 1
        assert result[0] is not None

    def test_marginalize_pdf_two_rounds(self, mock_condor_config):
        """Test marginalize_pdf with two rounds."""
        rank_config = mock.MagicMock()
        rank_config.marg_pdf_files = 2

        pdf_cache = mock.MagicMock()
        pdf_cache.files = ["pdf1.xml.gz", "pdf2.xml.gz", "pdf3.xml.gz"]

        chunk1 = mock.MagicMock()
        chunk1.name = mock.MagicMock()
        chunk1.name.filename.return_value = "partial.xml.gz"
        chunk1.name.directory.return_value = "/path/to/rank/pdfs"
        chunk1.groupby.return_value = {
            "0000": mock.MagicMock(),
            "0001": mock.MagicMock(),
        }
        chunk1.files = ["pdf1.xml.gz", "pdf2.xml.gz"]

        chunk2 = mock.MagicMock()
        chunk2.name = mock.MagicMock()
        chunk2.name.filename.return_value = "partial2.xml.gz"
        chunk2.name.directory.return_value = "/path/to/rank/pdfs"
        chunk2.groupby.return_value = {"0002": mock.MagicMock()}
        chunk2.files = ["pdf3.xml.gz"]

        def mock_chunked(n):
            yield chunk1
            yield chunk2

        pdf_cache.chunked = mock_chunked

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        result = layers.marginalize_pdf(
            mock_condor_config,
            rank_config,
            rank_dir="/path/to/rank",
            all_ifos="H1L1",
            span=(1000, 2000),
            pdf_cache=pdf_cache,
            marg_pdf_cache=marg_pdf_cache,
        )

        assert len(result) == 2


class TestAssignFar:
    """Tests for assign_far function."""

    def test_assign_far_basic(self, mock_condor_config):
        """Test assign_far layer creation."""
        event_config_file = "/path/to/event_config.yaml"

        trigger_cache = mock.MagicMock()
        trigger_cache.groupby.return_value = {
            "": mock.MagicMock(files=["triggers.sqlite.gz"]),
            "bbh": mock.MagicMock(files=["inj_triggers.sqlite.gz"]),
        }

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        post_pdf_cache = mock.MagicMock()
        post_pdf_cache.files = ["post_pdf.xml.gz"]

        far_trigger_cache = mock.MagicMock()
        far_trigger_cache.groupby.return_value = {
            "": mock.MagicMock(files=["far_triggers.sqlite.gz"]),
            "bbh": mock.MagicMock(files=["far_inj_triggers.sqlite.gz"]),
        }

        result = layers.assign_far(
            mock_condor_config,
            event_config_file,
            trigger_cache,
            marg_pdf_cache,
            post_pdf_cache,
            far_trigger_cache,
        )

        assert len(result) == 2


class TestSummaryPage:
    """Tests for summary_page function."""

    def test_summary_page_without_injections(self, mock_condor_config):
        """Test summary_page without injections."""
        event_config_file = "/path/to/event_config.yaml"
        segments_file = "/path/to/segments.xml"
        segments_name = "datasegments"
        webdir = "/path/to/web"

        far_trigger_cache = mock.MagicMock()
        far_trigger_cache.groupby.return_value = {
            "": mock.MagicMock(files=["far_triggers.sqlite.gz"])
        }

        seg_far_trigger_cache = mock.MagicMock()
        seg_far_trigger_cache.groupby.return_value = {
            "": mock.MagicMock(files=["seg_far_triggers.sqlite.gz"])
        }

        post_pdf_cache = mock.MagicMock()
        post_pdf_cache.files = ["post_pdf.xml.gz"]

        marg_lr_prior_cache = mock.MagicMock()
        marg_lr_prior_cache.files = ["marg_lr_prior.xml.gz"]

        result = layers.summary_page(
            mock_condor_config,
            event_config_file,
            segments_file,
            segments_name,
            webdir,
            far_trigger_cache,
            seg_far_trigger_cache,
            post_pdf_cache,
            marg_lr_prior_cache,
            mass_model_file="/path/to/mass_model.h5",
            injections=False,
        )

        assert len(result) == 2

    def test_summary_page_with_injections(self, mock_condor_config):
        """Test summary_page with injections."""
        event_config_file = "/path/to/event_config.yaml"
        segments_file = "/path/to/segments.xml"
        segments_name = "datasegments"
        webdir = "/path/to/web"

        far_trigger_cache = mock.MagicMock()
        far_trigger_cache.groupby.return_value = {
            "": mock.MagicMock(files=["far_triggers.sqlite.gz"]),
            "bbh": mock.MagicMock(files=["inj_triggers.sqlite.gz"]),
        }

        seg_far_trigger_cache = mock.MagicMock()
        seg_far_trigger_cache.groupby.return_value = {
            "": mock.MagicMock(files=["seg_far_triggers.sqlite.gz"]),
            "bbh": mock.MagicMock(files=["seg_inj_triggers.sqlite.gz"]),
        }

        post_pdf_cache = mock.MagicMock()
        post_pdf_cache.files = ["post_pdf.xml.gz"]

        marg_lr_prior_cache = mock.MagicMock()
        marg_lr_prior_cache.files = ["marg_lr_prior.xml.gz"]

        result = layers.summary_page(
            mock_condor_config,
            event_config_file,
            segments_file,
            segments_name,
            webdir,
            far_trigger_cache,
            seg_far_trigger_cache,
            post_pdf_cache,
            marg_lr_prior_cache,
            mass_model_file="/path/to/mass_model.h5",
            injections=True,
        )

        assert len(result) == 3


class TestFilterOnline:
    """Tests for filter_online function."""

    def test_filter_online_basic(self, mock_condor_config, mock_svd_stats):
        """Test filter_online layer creation."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snapshot_multiprocess = False
        filter_config.snapshot_interval = None
        filter_config.all_triggers_to_background = False
        filter_config.search = None
        filter_config.torch_dtype = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = 1
        filter_config.dynamic_group = None
        filter_config.cap_singles = False
        filter_config.verbose = False
        filter_config.compress_likelihood_ratio = False
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.ht_gate_threshold = 50.0

        upload_config = mock.MagicMock()
        upload_config.far_trials_factor = 1
        upload_config.gracedb_far_threshold = 1e-6
        upload_config.aggregator_far_threshold = 1e-6
        upload_config.aggregator_far_trials_factor = 1
        upload_config.gracedb_group = "Test"
        upload_config.gracedb_search = "AllSky"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        source_config = mock.MagicMock()
        source_config.data_source = "devshm"
        source_config.source_queue_timeout = 300
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.state_channel_name = {"H1": "H1:STATE", "L1": "L1:STATE"}
        source_config.state_vector_on_bits = {"H1": "0x1", "L1": "0x1"}
        source_config.shared_memory_dir = {"H1": "/dev/shm/H1", "L1": "/dev/shm/L1"}

        ref_psd_cache = "/path/to/ref_psd.xml.gz"

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
        }

        lr_cache = mock.MagicMock()
        lr_cache.groupby.return_value = {"0000": mock.MagicMock()}

        lr_group = mock.MagicMock()
        lr_group.groupby.return_value = {"0000": mock.MagicMock()}
        lr_group.files = ["lr.xml.gz"]

        def mock_chunked(n):
            yield lr_group

        lr_cache.chunked = mock_chunked

        zerolag_pdf_cache = mock.MagicMock()
        zerolag_pdf_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["zerolag.xml.gz"])
        }

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        layer = layers.filter_online(
            psd_config,
            filter_config,
            upload_config,
            services_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            lr_cache,
            mock_svd_stats,
            zerolag_pdf_cache,
            marg_pdf_cache,
            ifos=["H1", "L1"],
            tag="test_tag",
            min_instruments=2,
        )

        assert layer is not None

    def test_filter_online_with_cuda_and_options(
        self, mock_condor_config, mock_svd_stats
    ):
        """Test filter_online with CUDA and various options."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        filter_config = mock.MagicMock()
        filter_config.torch_device = "cuda:0"  # Enable CUDA (lines 1257, 1262)
        filter_config.coincidence_threshold = 0.005
        filter_config.snapshot_multiprocess = True  # Enable (line 1304)
        filter_config.snapshot_interval = 300
        filter_config.all_triggers_to_background = True  # Enable (line 1322)
        filter_config.search = "AllSky"  # Enable (line 1325)
        filter_config.torch_dtype = "float64"  # Enable (line 1329)
        filter_config.trigger_finding_duration = 2  # Enable (line 1338)
        filter_config.group_svd_num = 1
        filter_config.dynamic_group = None
        filter_config.cap_singles = True
        filter_config.verbose = True
        filter_config.compress_likelihood_ratio = True
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.ht_gate_threshold = 50.0

        upload_config = mock.MagicMock()
        upload_config.far_trials_factor = 1
        upload_config.gracedb_far_threshold = 1e-6
        upload_config.aggregator_far_threshold = 1e-6
        upload_config.aggregator_far_trials_factor = 1
        upload_config.gracedb_group = "Test"
        upload_config.gracedb_search = "AllSky"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        source_config = mock.MagicMock()
        source_config.data_source = "devshm"
        source_config.source_queue_timeout = 300
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.state_channel_name = {"H1": "H1:STATE", "L1": "L1:STATE"}
        source_config.state_vector_on_bits = {"H1": "0x1", "L1": "0x1"}
        source_config.shared_memory_dir = {"H1": "/dev/shm/H1", "L1": "/dev/shm/L1"}

        ref_psd_cache = "/path/to/ref_psd.xml.gz"

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
        }

        lr_cache = mock.MagicMock()
        lr_cache.groupby.return_value = {"0000": mock.MagicMock()}

        lr_group = mock.MagicMock()
        lr_group.groupby.return_value = {"0000": mock.MagicMock()}
        lr_group.files = ["lr.xml.gz"]

        def mock_chunked(n):
            yield lr_group

        lr_cache.chunked = mock_chunked

        zerolag_pdf_cache = mock.MagicMock()
        zerolag_pdf_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["zerolag.xml.gz"])
        }

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        layer = layers.filter_online(
            psd_config,
            filter_config,
            upload_config,
            services_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            lr_cache,
            mock_svd_stats,
            zerolag_pdf_cache,
            marg_pdf_cache,
            ifos=["H1", "L1"],
            tag="test_tag",
            min_instruments=2,
        )

        assert layer is not None


class TestInjectionFilterOnline:
    """Tests for injection_filter_online function."""

    def test_injection_filter_online_basic(self, mock_condor_config, mock_svd_stats):
        """Test injection_filter_online layer creation."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snapshot_multiprocess = False
        filter_config.snapshot_interval = None
        filter_config.search = None
        filter_config.torch_dtype = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = 1
        filter_config.dynamic_group = None
        filter_config.cap_singles = False
        filter_config.verbose = False
        filter_config.compress_likelihood_ratio = False
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.ht_gate_threshold = 50.0

        upload_config = mock.MagicMock()
        upload_config.far_trials_factor = 1
        upload_config.gracedb_far_threshold = 1e-6
        upload_config.aggregator_far_threshold = 1e-6
        upload_config.aggregator_far_trials_factor = 1
        upload_config.gracedb_group = "Test"
        upload_config.inj_gracedb_search = "MDC"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        source_config = mock.MagicMock()
        source_config.data_source = "devshm"
        source_config.source_queue_timeout = 300
        source_config.inj_channel_name = {"H1": "H1:INJ", "L1": "L1:INJ"}
        source_config.state_channel_name = {"H1": "H1:STATE", "L1": "L1:STATE"}
        source_config.state_vector_on_bits = {"H1": "0x1", "L1": "0x1"}
        source_config.shared_memory_dir = {"H1": "/dev/shm/H1", "L1": "/dev/shm/L1"}

        ref_psd_cache = "/path/to/ref_psd.xml.gz"

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
        }

        lr_cache = mock.MagicMock()
        lr_group = mock.MagicMock()
        lr_group.groupby.return_value = {"0000": mock.MagicMock()}
        lr_group.files = ["lr.xml.gz"]

        def mock_chunked(n):
            yield lr_group

        lr_cache.chunked = mock_chunked

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        layer = layers.injection_filter_online(
            psd_config,
            filter_config,
            upload_config,
            services_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            lr_cache,
            mock_svd_stats,
            marg_pdf_cache,
            ifos=["H1", "L1"],
            tag="test_tag",
            min_instruments=2,
        )

        assert layer is not None

    def test_injection_filter_online_with_cuda_and_options(
        self, mock_condor_config, mock_svd_stats
    ):
        """Test injection_filter_online with CUDA and options."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        filter_config = mock.MagicMock()
        filter_config.torch_device = "cuda:0"  # Enable CUDA (lines 1452, 1457)
        filter_config.coincidence_threshold = 0.005
        filter_config.snapshot_multiprocess = True  # Enable (line 1506)
        filter_config.snapshot_interval = 300  # Enable (line 1508)
        filter_config.search = "MDC"  # Enable (line 1524)
        filter_config.torch_dtype = "float64"  # Enable (line 1528)
        filter_config.trigger_finding_duration = 2  # Enable (line 1537)
        filter_config.group_svd_num = 1
        filter_config.dynamic_group = None
        filter_config.cap_singles = True  # Enable (line 1546)
        filter_config.verbose = True  # Enable (line 1549)
        filter_config.compress_likelihood_ratio = True  # Enable (line 1553)
        filter_config.compress_likelihood_ratio_threshold = 1e-10
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.ht_gate_threshold = 50.0

        upload_config = mock.MagicMock()
        upload_config.far_trials_factor = 1
        upload_config.gracedb_far_threshold = 1e-6
        upload_config.aggregator_far_threshold = 1e-6
        upload_config.aggregator_far_trials_factor = 1
        upload_config.gracedb_group = "Test"
        upload_config.inj_gracedb_search = "MDC"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        source_config = mock.MagicMock()
        source_config.data_source = "devshm"
        source_config.source_queue_timeout = 300
        source_config.inj_channel_name = {"H1": "H1:INJ", "L1": "L1:INJ"}
        source_config.state_channel_name = {"H1": "H1:STATE", "L1": "L1:STATE"}
        source_config.state_vector_on_bits = {"H1": "0x1", "L1": "0x1"}
        source_config.shared_memory_dir = {"H1": "/dev/shm/H1", "L1": "/dev/shm/L1"}

        ref_psd_cache = "/path/to/ref_psd.xml.gz"

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
        }

        lr_cache = mock.MagicMock()
        lr_group = mock.MagicMock()
        lr_group.groupby.return_value = {"0000": mock.MagicMock()}
        lr_group.files = ["lr.xml.gz"]

        def mock_chunked(n):
            yield lr_group

        lr_cache.chunked = mock_chunked

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        layer = layers.injection_filter_online(
            psd_config,
            filter_config,
            upload_config,
            services_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            lr_cache,
            mock_svd_stats,
            marg_pdf_cache,
            ifos=["H1", "L1"],
            tag="test_tag",
            min_instruments=2,
        )

        assert layer is not None

    def test_injection_filter_online_with_grouped_bins(
        self, mock_condor_config, mock_svd_stats
    ):
        """Test injection_filter_online with group_svd_num > 1 (line 1584)."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snapshot_multiprocess = False
        filter_config.snapshot_interval = None
        filter_config.search = None
        filter_config.torch_dtype = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = 2  # Enable grouping (line 1584)
        filter_config.dynamic_group = None
        filter_config.cap_singles = False
        filter_config.verbose = False
        filter_config.compress_likelihood_ratio = False
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.ht_gate_threshold = 50.0

        upload_config = mock.MagicMock()
        upload_config.far_trials_factor = 1
        upload_config.gracedb_far_threshold = 1e-6
        upload_config.aggregator_far_threshold = 1e-6
        upload_config.aggregator_far_trials_factor = 1
        upload_config.gracedb_group = "Test"
        upload_config.inj_gracedb_search = "MDC"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        source_config = mock.MagicMock()
        source_config.data_source = "devshm"
        source_config.source_queue_timeout = 300
        source_config.inj_channel_name = {"H1": "H1:INJ", "L1": "L1:INJ"}
        source_config.state_channel_name = {"H1": "H1:STATE", "L1": "L1:STATE"}
        source_config.state_vector_on_bits = {"H1": "0x1", "L1": "0x1"}
        source_config.shared_memory_dir = {"H1": "/dev/shm/H1", "L1": "/dev/shm/L1"}

        ref_psd_cache = "/path/to/ref_psd.xml.gz"

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
            ("H1", "0001"): mock.MagicMock(files=["svd.xml.gz"]),
            ("L1", "0001"): mock.MagicMock(files=["svd.xml.gz"]),
        }

        lr_cache = mock.MagicMock()
        lr_group = mock.MagicMock()
        lr_group.groupby.return_value = {
            "0000": mock.MagicMock(),
            "0001": mock.MagicMock(),
        }
        lr_group.files = ["lr.xml.gz"]

        def mock_chunked(n):
            yield lr_group

        lr_cache.chunked = mock_chunked

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        # Add second bin to svd_stats
        svd_stats_multi = MockSvdStats()
        svd_stats_multi.bins["0001"] = {
            "mean_mchirp": 15.0,
            "ht_gate_threshold": 50.0,
            "ac_length": 701,
        }

        layer = layers.injection_filter_online(
            psd_config,
            filter_config,
            upload_config,
            services_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            lr_cache,
            svd_stats_multi,
            marg_pdf_cache,
            ifos=["H1", "L1"],
            tag="test_tag",
            min_instruments=2,
        )

        assert layer is not None

    def test_injection_filter_online_with_dynamic_group(
        self, mock_condor_config, mock_svd_stats
    ):
        """Test injection_filter_online with dynamic_group option (lines 1567-1576)."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snapshot_multiprocess = False
        filter_config.snapshot_interval = None
        filter_config.search = None
        filter_config.torch_dtype = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = None  # Must be None to hit dynamic_group branch
        filter_config.dynamic_group = "1,1"  # Comma-separated string
        filter_config.cap_singles = False
        filter_config.verbose = False
        filter_config.compress_likelihood_ratio = False
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.ht_gate_threshold = 50.0

        upload_config = mock.MagicMock()
        upload_config.far_trials_factor = 1
        upload_config.gracedb_far_threshold = 1e-6
        upload_config.aggregator_far_threshold = 1e-6
        upload_config.aggregator_far_trials_factor = 1
        upload_config.gracedb_group = "Test"
        upload_config.inj_gracedb_search = "MDC"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        source_config = mock.MagicMock()
        source_config.data_source = "devshm"
        source_config.source_queue_timeout = 300
        source_config.inj_channel_name = {"H1": "H1:INJ", "L1": "L1:INJ"}
        source_config.state_channel_name = {"H1": "H1:STATE", "L1": "L1:STATE"}
        source_config.state_vector_on_bits = {"H1": "0x1", "L1": "0x1"}
        source_config.shared_memory_dir = {"H1": "/dev/shm/H1", "L1": "/dev/shm/L1"}

        ref_psd_cache = "/path/to/ref_psd.xml.gz"

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
            ("H1", "0001"): mock.MagicMock(files=["svd.xml.gz"]),
            ("L1", "0001"): mock.MagicMock(files=["svd.xml.gz"]),
        }

        # Mock lr_cache for dynamic_group path
        lr_item_0 = mock.MagicMock()
        lr_item_0.bin = "0000"
        lr_item_1 = mock.MagicMock()
        lr_item_1.bin = "0001"
        lr_cache = mock.MagicMock()
        lr_cache.name = "lr_cache"
        lr_cache.cache = [lr_item_0, lr_item_1]  # List that can be sliced

        # Mock the DataCache class to return properly configured groups
        lr_group_0 = mock.MagicMock()
        lr_group_0.groupby.return_value = {"0000": mock.MagicMock()}
        lr_group_0.files = ["lr_0000.xml.gz"]

        lr_group_1 = mock.MagicMock()
        lr_group_1.groupby.return_value = {"0001": mock.MagicMock()}
        lr_group_1.files = ["lr_0001.xml.gz"]

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        # Add second bin to svd_stats
        svd_stats_multi = MockSvdStats()
        svd_stats_multi.bins["0001"] = {
            "mean_mchirp": 15.0,
            "ht_gate_threshold": 50.0,
            "ac_length": 701,
        }

        with mock.patch.object(
            layers.util, "DataCache", side_effect=[lr_group_0, lr_group_1]
        ):
            layer = layers.injection_filter_online(
                psd_config,
                filter_config,
                upload_config,
                services_config,
                source_config,
                mock_condor_config,
                ref_psd_cache,
                svd_bank_cache,
                lr_cache,
                svd_stats_multi,
                marg_pdf_cache,
                ifos=["H1", "L1"],
                tag="test_tag",
                min_instruments=2,
            )

        assert layer is not None


class TestMarginalizeOnline:
    """Tests for marginalize_online function."""

    def test_marginalize_online_basic(self, mock_condor_config):
        """Test marginalize_online layer creation."""
        filter_config = mock.MagicMock()
        filter_config.group_svd_num = 1
        filter_config.dynamic_group = None

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        lr_cache = mock.MagicMock()
        lr_group = mock.MagicMock()
        lr_group.groupby.return_value = {"0000": mock.MagicMock()}

        def mock_chunked(n):
            yield lr_group

        lr_cache.chunked = mock_chunked

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        layer = layers.marginalize_online(
            mock_condor_config,
            filter_config,
            services_config,
            lr_cache,
            tag="test_tag",
            marg_pdf_cache=marg_pdf_cache,
        )

        assert layer is not None

    def test_marginalize_online_with_options(self, mock_condor_config):
        """Test marginalize_online with additional options."""
        filter_config = mock.MagicMock()
        filter_config.group_svd_num = 2
        filter_config.dynamic_group = None

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        lr_cache = mock.MagicMock()
        lr_group = mock.MagicMock()
        lr_group.groupby.return_value = {
            "0000": mock.MagicMock(),
            "0001": mock.MagicMock(),
        }

        def mock_chunked(n):
            yield lr_group

        lr_cache.chunked = mock_chunked

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        layer = layers.marginalize_online(
            mock_condor_config,
            filter_config,
            services_config,
            lr_cache,
            tag="test_tag",
            marg_pdf_cache=marg_pdf_cache,
            extinct_percent=10,
            fast_burnin=True,
            calc_pdf_cores=4,
        )

        assert layer is not None


class TestTrackNoise:
    """Tests for track_noise function."""

    def test_track_noise_basic(self, mock_condor_config):
        """Test track_noise layer creation."""
        source_config = mock.MagicMock()
        source_config.data_source = "devshm"
        source_config.source_queue_timeout = 300
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.state_channel_name = {"H1": "H1:STATE", "L1": "L1:STATE"}
        source_config.state_vector_on_bits = {"H1": "0x1", "L1": "0x1"}
        source_config.shared_memory_dir = {"H1": "/dev/shm/H1", "L1": "/dev/shm/L1"}

        filter_config = mock.MagicMock()

        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        metrics_config = mock.MagicMock()

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        layer = layers.track_noise(
            mock_condor_config,
            source_config,
            filter_config,
            psd_config,
            metrics_config,
            services_config,
            ifos=["H1", "L1"],
            tag="test_tag",
            ref_psd="/path/to/ref_psd.xml.gz",
        )

        assert layer is not None

    def test_track_noise_with_injection(self, mock_condor_config):
        """Test track_noise with injection."""
        source_config = mock.MagicMock()
        source_config.data_source = "devshm"
        source_config.source_queue_timeout = 300
        source_config.inj_channel_name = {"H1": "H1:INJ", "L1": "L1:INJ"}
        source_config.state_channel_name = {"H1": "H1:STATE", "L1": "L1:STATE"}
        source_config.state_vector_on_bits = {"H1": "0x1", "L1": "0x1"}
        source_config.inj_shared_memory_dir = {
            "H1": "/dev/shm/H1_inj",
            "L1": "/dev/shm/L1_inj",
        }

        filter_config = mock.MagicMock()

        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        metrics_config = mock.MagicMock()

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        layer = layers.track_noise(
            mock_condor_config,
            source_config,
            filter_config,
            psd_config,
            metrics_config,
            services_config,
            ifos=["H1", "L1"],
            tag="test_tag",
            ref_psd="/path/to/ref_psd.xml.gz",
            injection=True,
        )

        assert layer is not None


class TestCountEvents:
    """Tests for count_events function."""

    def test_count_events_basic(self, mock_condor_config):
        """Test count_events layer creation."""
        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        upload_config = mock.MagicMock()

        zerolag_pdf = mock.MagicMock()
        zerolag_pdf.files = ["zerolag.xml.gz"]

        layer = layers.count_events(
            mock_condor_config,
            services_config,
            upload_config,
            tag="test_tag",
            zerolag_pdf=zerolag_pdf,
        )

        assert layer is not None


class TestUploadEvents:
    """Tests for upload_events function."""

    def test_upload_events_basic(self, mock_condor_config):
        """Test upload_events layer creation."""
        upload_config = mock.MagicMock()
        upload_config.enable_injection_uploads = False
        upload_config.gracedb_group = "Test"
        upload_config.gracedb_pipeline = "gstlal"
        upload_config.aggregator_far_threshold = 1e-6
        upload_config.aggregator_far_trials_factor = 1
        upload_config.aggregator_cadence_type = "linear"
        upload_config.aggregator_cadence_factor = 1
        upload_config.gracedb_service_url = "https://gracedb.ligo.org"
        upload_config.gracedb_search = "AllSky"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        metrics_config = mock.MagicMock()
        metrics_config.scald_config = "/path/to/scald.yaml"

        layer = layers.upload_events(
            mock_condor_config,
            upload_config,
            services_config,
            metrics_config,
            svd_bins=["0000", "0001"],
            tag="test_tag",
        )

        assert layer is not None

    def test_upload_events_with_injections(self, mock_condor_config):
        """Test upload_events with injection uploads enabled."""
        upload_config = mock.MagicMock()
        upload_config.enable_injection_uploads = True
        upload_config.gracedb_group = "Test"
        upload_config.gracedb_pipeline = "gstlal"
        upload_config.aggregator_far_threshold = 1e-6
        upload_config.aggregator_far_trials_factor = 1
        upload_config.aggregator_cadence_type = "linear"
        upload_config.aggregator_cadence_factor = 1
        upload_config.gracedb_service_url = "https://gracedb.ligo.org"
        upload_config.inj_gracedb_service_url = "https://gracedb-test.ligo.org"
        upload_config.gracedb_search = "AllSky"
        upload_config.inj_gracedb_search = "MDC"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        metrics_config = mock.MagicMock()
        metrics_config.scald_config = "/path/to/scald.yaml"

        layer = layers.upload_events(
            mock_condor_config,
            upload_config,
            services_config,
            metrics_config,
            svd_bins=["0000"],
            tag="test_tag",
        )

        assert layer is not None


class TestPlotEvents:
    """Tests for plot_events function."""

    def test_plot_events_basic(self, mock_condor_config):
        """Test plot_events layer creation."""
        upload_config = mock.MagicMock()
        upload_config.enable_injection_uploads = False
        upload_config.gracedb_service_url = "https://gracedb.ligo.org"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        layer = layers.plot_events(
            mock_condor_config, upload_config, services_config, tag="test_tag"
        )

        assert layer is not None

    def test_plot_events_with_injection_uploads(self, mock_condor_config):
        """Test plot_events with injection uploads enabled (line 1880)."""
        upload_config = mock.MagicMock()
        upload_config.enable_injection_uploads = True
        upload_config.gracedb_service_url = "https://gracedb.ligo.org"
        upload_config.inj_gracedb_service_url = "https://gracedb-test.ligo.org"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        layer = layers.plot_events(
            mock_condor_config, upload_config, services_config, tag="test_tag"
        )

        assert layer is not None


class TestCollectMetrics:
    """Tests for collect_metrics function."""

    def test_collect_metrics_basic(self, mock_condor_config):
        """Test collect_metrics layer creation."""
        metrics_config = mock.MagicMock()
        metrics_config.scald_config = "/path/to/scald.yaml"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        filter_config = mock.MagicMock()
        filter_config.injections = False
        filter_config.group_svd_num = 1
        filter_config.dynamic_group = None

        layer = layers.collect_metrics(
            mock_condor_config,
            metrics_config,
            services_config,
            filter_config,
            tag="test_tag",
            ifos=["H1", "L1"],
            svd_bins=["0000"],
        )

        assert layer is not None

    def test_collect_metrics_with_dynamic_group(self, mock_condor_config):
        """Test collect_metrics with dynamic_group option (lines 2004-2007)."""
        metrics_config = mock.MagicMock()
        metrics_config.scald_config = "/path/to/scald.yaml"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        filter_config = mock.MagicMock()
        filter_config.injections = False
        filter_config.group_svd_num = None  # Must be None
        filter_config.dynamic_group = "1,1"  # Comma-separated string

        layer = layers.collect_metrics(
            mock_condor_config,
            metrics_config,
            services_config,
            filter_config,
            tag="test_tag",
            ifos=["H1", "L1"],
            svd_bins=["0000", "0001"],  # Two bins matching "1,1"
        )

        assert layer is not None


class TestCollectMetricsEvent:
    """Tests for collect_metrics_event function."""

    def test_collect_metrics_event_basic(self, mock_condor_config):
        """Test collect_metrics_event layer creation."""
        metrics_config = mock.MagicMock()
        metrics_config.scald_config = "/path/to/scald.yaml"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        filter_config = mock.MagicMock()
        filter_config.injections = False

        layer = layers.collect_metrics_event(
            mock_condor_config,
            metrics_config,
            services_config,
            filter_config,
            tag="test_tag",
        )

        assert layer is not None


class TestUploadPastro:
    """Tests for upload_pastro function."""

    def test_upload_pastro_basic(self, mock_condor_config, tmp_path):
        """Test upload_pastro layer creation."""
        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        upload_config = mock.MagicMock()
        upload_config.enable_injection_uploads = False
        upload_config.gracedb_service_url = "https://gracedb.ligo.org"

        # Create a temporary mass model file
        mass_model_file = tmp_path / "mass_model.h5"
        mass_model_file.write_text("")

        pastro_config = {
            "default": mock.MagicMock(
                mass_model=str(mass_model_file), upload_file="pastro.json"
            )
        }

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        layer = layers.upload_pastro(
            mock_condor_config,
            services_config,
            upload_config,
            pastro_config,
            tag="test_tag",
            marg_pdf_cache=marg_pdf_cache,
        )

        assert layer is not None


class TestAddLikelihoodRatioFileOptions:
    """Tests for add_likelihood_ratio_file_options function."""

    def test_basic_options(self, mock_svd_stats):
        """Test basic likelihood ratio file options."""
        prior_config = mock.MagicMock()
        prior_config.mass_model = "/path/to/mass_model.h5"
        prior_config.idq_timeseries = None
        prior_config.dtdphi = None

        result = layers.add_likelihood_ratio_file_options(
            "0000", mock_svd_stats, prior_config
        )

        assert len(result) == 1

    def test_options_with_idq(self, mock_svd_stats):
        """Test options with IDQ timeseries."""
        prior_config = mock.MagicMock()
        prior_config.mass_model = "/path/to/mass_model.h5"
        prior_config.idq_timeseries = "/path/to/idq.h5"
        prior_config.dtdphi = None

        result = layers.add_likelihood_ratio_file_options(
            "0000", mock_svd_stats, prior_config
        )

        assert len(result) == 2

    def test_options_with_dtdphi(self, mock_svd_stats):
        """Test options with dtdphi file."""
        prior_config = mock.MagicMock()
        prior_config.mass_model = "/path/to/mass_model.h5"
        prior_config.idq_timeseries = None
        prior_config.dtdphi = "/path/to/dtdphi.h5"

        result = layers.add_likelihood_ratio_file_options(
            "0000", mock_svd_stats, prior_config
        )

        assert len(result) == 2

    def test_options_with_dtdphi_mapping(self):
        """Test options with dtdphi as a mapping."""
        svd_stats = MockSvdStats()
        svd_stats.bins["0000"]["bank_name"] = "bns"

        prior_config = mock.MagicMock()
        prior_config.mass_model = "/path/to/mass_model.h5"
        prior_config.idq_timeseries = None
        prior_config.dtdphi = {"bns": "/path/to/dtdphi_bns.h5"}

        result = layers.add_likelihood_ratio_file_options(
            "0000", svd_stats, prior_config
        )

        assert len(result) == 2

    def test_transfer_only_mode(self, mock_svd_stats):
        """Test transfer_only mode."""
        prior_config = mock.MagicMock()
        prior_config.mass_model = "/path/to/mass_model.h5"
        prior_config.idq_timeseries = "/path/to/idq.h5"
        prior_config.dtdphi = "/path/to/dtdphi.h5"

        result = layers.add_likelihood_ratio_file_options(
            "0000", mock_svd_stats, prior_config, transfer_only=True
        )

        assert len(result) == 3


class TestAutocorrelationLengthMap:
    """Tests for autocorrelation_length_map function."""

    def test_single_range(self):
        """Test with single AC length range."""
        ac_map = layers.autocorrelation_length_map("0:15:701")

        assert ac_map(5.0) == 701
        assert ac_map(10.0) == 701

    def test_multiple_ranges(self):
        """Test with multiple AC length ranges."""
        ac_map = layers.autocorrelation_length_map(
            ["0:5:201", "5:15:401", "15:100:701"]
        )

        assert ac_map(2.0) == 201
        assert ac_map(10.0) == 401
        assert ac_map(50.0) == 701

    def test_single_value(self):
        """Test with single AC length value (not a string range)."""
        ac_map = layers.autocorrelation_length_map(501)

        assert ac_map(10.0) == 501

    def test_invalid_ranges_with_gaps(self):
        """Test that gaps in ranges raise assertion error."""
        with pytest.raises(AssertionError):
            layers.autocorrelation_length_map(["0:5:201", "6:15:401"])


class TestSubmitDescriptionEdgeCases:
    """Additional tests for edge cases in create_submit_description."""

    def test_submit_description_with_existing_environment(self):
        """Test when submit_description already has environment."""
        config = MockCondorConfig()
        config.directives = {"environment": '"EXISTING=value"'}
        config.environment = {"NEW_VAR": "new_value"}
        result = layers.create_submit_description(config)
        assert "NEW_VAR=new_value" in result["environment"]
        assert "EXISTING=value" in result["environment"]


class TestSvdBankEdgeCases:
    """Additional tests for svd_bank edge cases."""

    def test_svd_bank_with_autocorrelation_length(self, mock_condor_config):
        """Test svd_bank with autocorrelation_length in config."""
        svd_stats = MockSvdStats()

        svd_config = mock.MagicMock()
        svd_config.f_low = 15.0
        svd_config.max_f_final = 1024.0
        svd_config.approximant = "TaylorF2"
        svd_config.overlap = 0.01
        svd_config.num_split_templates = 100
        svd_config.num_banks = 10
        svd_config.sort_by = "mchirp"
        svd_config.autocorrelation_length = "0:15:701"
        svd_config.samples_min = 32
        svd_config.samples_max_64 = 64
        svd_config.samples_max_256 = 256
        svd_config.samples_max = 1024
        svd_config.tolerance = 0.99

        all_ifos = "H1L1"

        split_bank_cache = mock.MagicMock()
        split_bank_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["split_bank.xml.gz"])
        }

        median_psd_cache = mock.MagicMock()
        median_psd_cache.files = ["median_psd.xml.gz"]

        svd_cache = mock.MagicMock()
        svd_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_bank.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd_bank.xml.gz"]),
        }

        svd_bins = ["0000"]

        layer = layers.svd_bank(
            svd_config,
            mock_condor_config,
            all_ifos,
            split_bank_cache,
            median_psd_cache,
            svd_cache,
            svd_bins,
            svd_stats,
        )

        assert layer is not None

    def test_svd_bank_with_bank_name(self, mock_condor_config):
        """Test svd_bank with bank_name sub-banks."""
        svd_stats = MockSvdStats()
        svd_stats["bins"]["0000"]["bank_name"] = "bns"

        svd_config = mock.MagicMock()
        svd_config.f_low = 15.0
        svd_config.max_f_final = 1024.0
        svd_config.approximant = "TaylorF2"
        svd_config.overlap = 0.01
        svd_config.num_split_templates = 100
        svd_config.num_banks = 10
        svd_config.sort_by = "mchirp"
        svd_config.autocorrelation_length = None
        svd_config.samples_min = 32
        svd_config.samples_max_64 = 64
        svd_config.samples_max_256 = 256
        svd_config.samples_max = 1024
        svd_config.tolerance = 0.99
        svd_config.sub_banks = {
            "bns": mock.MagicMock(
                f_low=20.0,
                samples_min=32,
                samples_max_64=64,
                samples_max_256=256,
                samples_max=1024,
                tolerance=0.99,
                autocorrelation_length=None,
            )
        }

        all_ifos = "H1L1"

        split_bank_cache = mock.MagicMock()
        split_bank_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["split_bank.xml.gz"])
        }

        median_psd_cache = mock.MagicMock()
        median_psd_cache.files = ["median_psd.xml.gz"]

        svd_cache = mock.MagicMock()
        svd_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_bank.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd_bank.xml.gz"]),
        }

        svd_bins = ["0000"]

        layer = layers.svd_bank(
            svd_config,
            mock_condor_config,
            all_ifos,
            split_bank_cache,
            median_psd_cache,
            svd_cache,
            svd_bins,
            svd_stats,
        )

        assert layer is not None

    def test_svd_bank_no_ac_length_raises_error(self, mock_condor_config):
        """Test svd_bank raises ValueError when no AC length found."""
        svd_stats = MockSvdStats()
        # Remove ac_length from the mock stats
        del svd_stats["bins"]["0000"]["ac_length"]

        svd_config = mock.MagicMock()
        svd_config.f_low = 15.0
        svd_config.max_f_final = 1024.0
        svd_config.approximant = "TaylorF2"
        svd_config.overlap = 0.01
        svd_config.num_split_templates = 100
        svd_config.num_banks = 10
        svd_config.sort_by = "mchirp"
        svd_config.autocorrelation_length = None
        svd_config.samples_min = 32
        svd_config.samples_max_64 = 64
        svd_config.samples_max_256 = 256
        svd_config.samples_max = 1024
        svd_config.tolerance = 0.99

        all_ifos = "H1L1"

        split_bank_cache = mock.MagicMock()
        split_bank_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["split_bank.xml.gz"])
        }

        median_psd_cache = mock.MagicMock()
        median_psd_cache.files = ["median_psd.xml.gz"]

        svd_cache = mock.MagicMock()
        svd_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_bank.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd_bank.xml.gz"]),
        }

        svd_bins = ["0000"]

        with pytest.raises(ValueError) as exc_info:
            layers.svd_bank(
                svd_config,
                mock_condor_config,
                all_ifos,
                split_bank_cache,
                median_psd_cache,
                svd_cache,
                svd_bins,
                svd_stats,
            )
        assert "Unknown autocorrelation length" in str(exc_info.value)


class TestFilterEdgeCases:
    """Additional tests for filter edge cases."""

    def test_filter_with_idq_gate(self, mock_condor_config, mock_svd_stats):
        """Test filter with IDQ gate threshold."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        svd_config = mock.MagicMock()

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snr_min = 4.0
        filter_config.all_triggers_to_background = True
        filter_config.search = "ew"
        filter_config.torch_dtype = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = None
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.idq_gate_threshold = 0.5

        source_config = mock.MagicMock()
        source_config.frame_segments_name = "datasegments"
        source_config.frame_segments_file = "/path/to/segments.xml"
        source_config.frame_cache = None
        source_config.frame_type = {"H1": "H1_HOFT", "L1": "L1_HOFT"}
        source_config.data_find_server = "datafind.ligo.org"
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.idq_channel_name = {"H1": "H1:IDQ", "L1": "L1:IDQ"}
        source_config.idq_state_channel_name = {
            "H1": "H1:IDQ_STATE",
            "L1": "L1:IDQ_STATE",
        }

        ref_psd_cache = mock.MagicMock()
        ref_psd_cache.groupby.return_value = {
            ("H1L1", (1000, 2000)): mock.MagicMock(files=["psd.xml.gz"])
        }

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_h1.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd_l1.xml.gz"]),
        }

        lr_cache = mock.MagicMock()
        lr_cache.groupby.return_value = {
            ("H1L1", (1000, 2000), "0000"): mock.MagicMock(files=["lr.xml.gz"])
        }

        trigger_cache = mock.MagicMock()
        trigger_group = mock.MagicMock()
        trigger_group.groupby.return_value = {"0000": mock.MagicMock()}
        trigger_group.files = ["triggers.sqlite.gz"]

        def mock_chunked(n):
            yield trigger_group

        trigger_inner = mock.MagicMock(files=["triggers.sqlite.gz"])
        trigger_inner.chunked = mock_chunked
        trigger_cache.groupby.return_value = {("H1L1", (1000, 2000)): trigger_inner}

        layer = layers.filter(
            psd_config,
            svd_config,
            filter_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            lr_cache,
            trigger_cache,
            mock_svd_stats,
            min_instruments=2,
        )

        assert layer is not None


class TestInjectionFilterEdgeCases:
    """Additional tests for injection_filter edge cases."""

    def test_injection_filter_with_noiseless_frames(
        self, mock_condor_config, mock_svd_stats
    ):
        """Test injection_filter with noiseless injection frames."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        svd_config = mock.MagicMock()

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snr_min = 4.0
        filter_config.search = None
        filter_config.torch_dtype = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = None
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.idq_gate_threshold = None

        injection_config = mock.MagicMock()
        injection_config.filter = {
            "bbh": {"file": "/path/to/inj.xml", "noiseless_inj_frames": True}
        }

        source_config = mock.MagicMock()
        source_config.frame_segments_name = "datasegments"
        source_config.frame_segments_file = "/path/to/segments.xml"
        source_config.frame_cache = "/path/to/cache.lcf"
        source_config.inj_frame_cache = "/path/to/inj_cache.lcf"
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.inj_channel_name = {"H1": "H1:INJ", "L1": "L1:INJ"}
        source_config.idq_channel_name = None
        source_config.idq_state_channel_name = None

        ref_psd_cache = mock.MagicMock()
        ref_psd_cache.groupby.return_value = {
            ("H1L1", (1000, 2000)): mock.MagicMock(files=["psd.xml.gz"])
        }

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_h1.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd_l1.xml.gz"]),
        }

        trigger_cache = mock.MagicMock()
        trigger_group = mock.MagicMock()
        trigger_group.groupby.return_value = {"0000": mock.MagicMock()}
        trigger_group.files = ["triggers.sqlite.gz"]

        def mock_chunked(n):
            yield trigger_group

        trigger_inner = mock.MagicMock(files=["triggers.sqlite.gz"])
        trigger_inner.chunked = mock_chunked
        trigger_cache.groupby.return_value = {
            ("H1L1", (1000, 2000), "bbh"): trigger_inner
        }

        layer = layers.injection_filter(
            psd_config,
            svd_config,
            filter_config,
            injection_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            trigger_cache,
            mock_svd_stats,
            min_instruments=2,
        )

        assert layer is not None


class TestOnlineEdgeCases:
    """Additional tests for online function edge cases."""

    def test_filter_online_with_dynamic_group(self, mock_condor_config, mock_svd_stats):
        """Test filter_online with dynamic_group option (lines 1370-1379)."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snapshot_multiprocess = False
        filter_config.snapshot_interval = 100
        filter_config.all_triggers_to_background = False
        filter_config.search = None
        filter_config.torch_dtype = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = None  # Must be None to hit dynamic_group branch
        filter_config.dynamic_group = "1,1"  # Comma-separated string
        filter_config.cap_singles = True
        filter_config.verbose = True
        filter_config.compress_likelihood_ratio = True
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.ht_gate_threshold = 50.0

        upload_config = mock.MagicMock()
        upload_config.far_trials_factor = 1
        upload_config.gracedb_far_threshold = 1e-6
        upload_config.aggregator_far_threshold = 1e-6
        upload_config.aggregator_far_trials_factor = 1
        upload_config.gracedb_group = "Test"
        upload_config.gracedb_search = "AllSky"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        source_config = mock.MagicMock()
        source_config.data_source = "devshm"
        source_config.source_queue_timeout = 300
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.state_channel_name = {"H1": "H1:STATE", "L1": "L1:STATE"}
        source_config.state_vector_on_bits = {"H1": "0x1", "L1": "0x1"}
        source_config.shared_memory_dir = {"H1": "/dev/shm/H1", "L1": "/dev/shm/L1"}

        ref_psd_cache = "/path/to/ref_psd.xml.gz"

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
            ("H1", "0001"): mock.MagicMock(files=["svd.xml.gz"]),
            ("L1", "0001"): mock.MagicMock(files=["svd.xml.gz"]),
        }

        # Mock lr_cache for dynamic_group path
        lr_item_0 = mock.MagicMock()
        lr_item_0.bin = "0000"
        lr_item_1 = mock.MagicMock()
        lr_item_1.bin = "0001"
        lr_cache = mock.MagicMock()
        lr_cache.name = "lr_cache"
        lr_cache.cache = [lr_item_0, lr_item_1]  # List that can be sliced

        # Mock the DataCache class to return properly configured groups
        lr_group_0 = mock.MagicMock()
        lr_group_0.groupby.return_value = {"0000": mock.MagicMock()}
        lr_group_0.files = ["lr_0000.xml.gz"]

        lr_group_1 = mock.MagicMock()
        lr_group_1.groupby.return_value = {"0001": mock.MagicMock()}
        lr_group_1.files = ["lr_0001.xml.gz"]

        zerolag_pdf_cache = mock.MagicMock()
        zerolag_pdf_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["zerolag_0000.xml.gz"]),
            "0001": mock.MagicMock(files=["zerolag_0001.xml.gz"]),
        }

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        with mock.patch.object(
            layers.util, "DataCache", side_effect=[lr_group_0, lr_group_1]
        ):
            layer = layers.filter_online(
                psd_config,
                filter_config,
                upload_config,
                services_config,
                source_config,
                mock_condor_config,
                ref_psd_cache,
                svd_bank_cache,
                lr_cache,
                mock_svd_stats,
                zerolag_pdf_cache,
                marg_pdf_cache,
                ifos=["H1", "L1"],
                tag="test_tag",
                min_instruments=2,
            )

        assert layer is not None

    def test_marginalize_online_with_dynamic_group(self, mock_condor_config):
        """Test marginalize_online with dynamic_group option (lines 1640-1649)."""
        filter_config = mock.MagicMock()
        filter_config.group_svd_num = None  # Must be None to hit dynamic_group branch
        filter_config.dynamic_group = "1,1"  # Comma-separated string

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        # Mock lr_cache for dynamic_group path
        lr_item_0 = mock.MagicMock()
        lr_item_0.bin = "0000"
        lr_item_1 = mock.MagicMock()
        lr_item_1.bin = "0001"
        lr_cache = mock.MagicMock()
        lr_cache.name = "lr_cache"
        lr_cache.cache = [lr_item_0, lr_item_1]  # List that can be sliced

        # Mock the DataCache class to return properly configured groups
        lr_group_0 = mock.MagicMock()
        lr_group_0.groupby.return_value = {"0000": mock.MagicMock()}
        lr_group_0.files = ["lr_0000.xml.gz"]

        lr_group_1 = mock.MagicMock()
        lr_group_1.groupby.return_value = {"0001": mock.MagicMock()}
        lr_group_1.files = ["lr_0001.xml.gz"]

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        with mock.patch.object(
            layers.util, "DataCache", side_effect=[lr_group_0, lr_group_1]
        ):
            layer = layers.marginalize_online(
                mock_condor_config,
                filter_config,
                services_config,
                lr_cache,
                tag="test_tag",
                marg_pdf_cache=marg_pdf_cache,
            )

        assert layer is not None


class TestSvdBankAdditionalOptions:
    """Tests for svd_bank with additional options coverage."""

    def test_svd_bank_with_max_duration(self, mock_condor_config, mock_svd_stats):
        """Test svd_bank with max_duration option (line 266)."""
        svd_config = mock.MagicMock()
        svd_config.f_low = 15.0
        svd_config.max_f_final = 1024.0
        svd_config.approximant = "TaylorF2"
        svd_config.overlap = 0.01
        svd_config.num_split_templates = 100
        svd_config.num_banks = 10
        svd_config.sort_by = "mchirp"
        svd_config.autocorrelation_length = None
        svd_config.samples_min = 32
        svd_config.samples_max_64 = 64
        svd_config.samples_max_256 = 256
        svd_config.samples_max = 1024
        svd_config.tolerance = 0.99
        svd_config.max_duration = 128.0  # Enable line 266
        svd_config.__contains__ = lambda self, key: key in ["max_duration"]

        all_ifos = "H1L1"

        split_bank_cache = mock.MagicMock()
        split_bank_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["split_bank.xml.gz"])
        }

        median_psd_cache = mock.MagicMock()
        median_psd_cache.files = ["median_psd.xml.gz"]

        svd_cache = mock.MagicMock()
        svd_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_bank.xml.gz"])
        }

        svd_bins = ["0000"]

        layer = layers.svd_bank(
            svd_config,
            mock_condor_config,
            all_ifos,
            split_bank_cache,
            median_psd_cache,
            svd_cache,
            svd_bins,
            mock_svd_stats,
        )

        assert layer is not None

    def test_svd_bank_with_sample_rate(self, mock_condor_config, mock_svd_stats):
        """Test svd_bank with sample_rate option (line 268)."""
        svd_config = mock.MagicMock()
        svd_config.f_low = 15.0
        svd_config.max_f_final = 1024.0
        svd_config.approximant = "TaylorF2"
        svd_config.overlap = 0.01
        svd_config.num_split_templates = 100
        svd_config.num_banks = 10
        svd_config.sort_by = "mchirp"
        svd_config.autocorrelation_length = None
        svd_config.samples_min = 32
        svd_config.samples_max_64 = 64
        svd_config.samples_max_256 = 256
        svd_config.samples_max = 1024
        svd_config.tolerance = 0.99
        svd_config.sample_rate = 4096  # Enable line 268
        svd_config.__contains__ = lambda self, key: key in ["sample_rate"]

        all_ifos = "H1L1"

        split_bank_cache = mock.MagicMock()
        split_bank_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["split_bank.xml.gz"])
        }

        median_psd_cache = mock.MagicMock()
        median_psd_cache.files = ["median_psd.xml.gz"]

        svd_cache = mock.MagicMock()
        svd_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_bank.xml.gz"])
        }

        svd_bins = ["0000"]

        layer = layers.svd_bank(
            svd_config,
            mock_condor_config,
            all_ifos,
            split_bank_cache,
            median_psd_cache,
            svd_cache,
            svd_bins,
            mock_svd_stats,
        )

        assert layer is not None


class TestInjectionFilterAdditionalOptions:
    """Tests for injection_filter with additional options."""

    def test_injection_filter_with_cuda(self, mock_condor_config, mock_svd_stats):
        """Test injection_filter with CUDA options (lines 470, 475-476)."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        svd_config = mock.MagicMock()

        filter_config = mock.MagicMock()
        filter_config.torch_device = "cuda:0"  # Enable CUDA path
        filter_config.torch_dtype = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snr_min = 4.0
        filter_config.search = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = None
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.idq_gate_threshold = None

        injection_config = mock.MagicMock()
        injection_config.filter = {
            "bbh": {"file": "/path/to/inj.xml", "noiseless_inj_frames": False}
        }

        source_config = mock.MagicMock()
        source_config.frame_segments_name = "datasegments"
        source_config.frame_segments_file = "/path/to/segments.xml"
        source_config.frame_cache = "/path/to/cache.lcf"
        source_config.inj_frame_cache = None
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.inj_channel_name = {"H1": "H1:INJ", "L1": "L1:INJ"}
        source_config.idq_channel_name = None
        source_config.idq_state_channel_name = None

        ref_psd_cache = mock.MagicMock()
        ref_psd_cache.groupby.return_value = {
            ("H1L1", (1000, 2000)): mock.MagicMock(files=["psd.xml.gz"])
        }

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_h1.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd_l1.xml.gz"]),
        }

        trigger_cache = mock.MagicMock()
        trigger_group = mock.MagicMock()
        trigger_group.groupby.return_value = {"0000": mock.MagicMock()}
        trigger_group.files = ["triggers.sqlite.gz"]

        def mock_chunked(n):
            yield trigger_group

        trigger_inner = mock.MagicMock(files=["triggers.sqlite.gz"])
        trigger_inner.chunked = mock_chunked
        trigger_cache.groupby.return_value = {
            ("H1L1", (1000, 2000), "bbh"): trigger_inner
        }

        layer = layers.injection_filter(
            psd_config,
            svd_config,
            filter_config,
            injection_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            trigger_cache,
            mock_svd_stats,
            min_instruments=2,
        )

        assert layer is not None

    def test_injection_filter_with_no_frame_cache(
        self, mock_condor_config, mock_svd_stats
    ):
        """Test injection_filter when no frame cache (line 587 else branch)."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        svd_config = mock.MagicMock()

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.torch_dtype = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snr_min = 4.0
        filter_config.search = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = None
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.idq_gate_threshold = None

        injection_config = mock.MagicMock()
        injection_config.filter = {
            "bbh": {"file": "/path/to/inj.xml", "noiseless_inj_frames": False}
        }

        source_config = mock.MagicMock()
        source_config.frame_segments_name = "datasegments"
        source_config.frame_segments_file = "/path/to/segments.xml"
        source_config.frame_cache = None  # No frame cache
        source_config.inj_frame_cache = None  # No inj frame cache either
        source_config.frame_type = {"H1": "H1_HOFT_C00", "L1": "L1_HOFT_C00"}
        source_config.data_find_server = "datafind.ligo.org"
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.inj_channel_name = {"H1": "H1:INJ", "L1": "L1:INJ"}
        source_config.idq_channel_name = None
        source_config.idq_state_channel_name = None

        ref_psd_cache = mock.MagicMock()
        ref_psd_cache.groupby.return_value = {
            ("H1L1", (1000, 2000)): mock.MagicMock(files=["psd.xml.gz"])
        }

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_h1.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd_l1.xml.gz"]),
        }

        trigger_cache = mock.MagicMock()
        trigger_group = mock.MagicMock()
        trigger_group.groupby.return_value = {"0000": mock.MagicMock()}
        trigger_group.files = ["triggers.sqlite.gz"]

        def mock_chunked(n):
            yield trigger_group

        trigger_inner = mock.MagicMock(files=["triggers.sqlite.gz"])
        trigger_inner.chunked = mock_chunked
        trigger_cache.groupby.return_value = {
            ("H1L1", (1000, 2000), "bbh"): trigger_inner
        }

        layer = layers.injection_filter(
            psd_config,
            svd_config,
            filter_config,
            injection_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            trigger_cache,
            mock_svd_stats,
            min_instruments=2,
        )

        assert layer is not None

    def test_injection_filter_noiseless_false(self, mock_condor_config, mock_svd_stats):
        """Test injection_filter with noiseless_inj_frames=False (lines 570-573)."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        svd_config = mock.MagicMock()

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.torch_dtype = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snr_min = 4.0
        filter_config.search = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = None
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.idq_gate_threshold = None

        injection_config = mock.MagicMock()
        injection_config.filter = {
            "bbh": {"file": "/path/to/inj.xml", "noiseless_inj_frames": False}
        }

        source_config = mock.MagicMock()
        source_config.frame_segments_name = "datasegments"
        source_config.frame_segments_file = "/path/to/segments.xml"
        source_config.frame_cache = "/path/to/cache.lcf"
        source_config.inj_frame_cache = "/path/to/inj_cache.lcf"  # Has inj frame cache
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.inj_channel_name = {"H1": "H1:INJ", "L1": "L1:INJ"}
        source_config.idq_channel_name = None
        source_config.idq_state_channel_name = None

        ref_psd_cache = mock.MagicMock()
        ref_psd_cache.groupby.return_value = {
            ("H1L1", (1000, 2000)): mock.MagicMock(files=["psd.xml.gz"])
        }

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_h1.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd_l1.xml.gz"]),
        }

        trigger_cache = mock.MagicMock()
        trigger_group = mock.MagicMock()
        trigger_group.groupby.return_value = {"0000": mock.MagicMock()}
        trigger_group.files = ["triggers.sqlite.gz"]

        def mock_chunked(n):
            yield trigger_group

        trigger_inner = mock.MagicMock(files=["triggers.sqlite.gz"])
        trigger_inner.chunked = mock_chunked
        trigger_cache.groupby.return_value = {
            ("H1L1", (1000, 2000), "bbh"): trigger_inner
        }

        layer = layers.injection_filter(
            psd_config,
            svd_config,
            filter_config,
            injection_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            trigger_cache,
            mock_svd_stats,
            min_instruments=2,
        )

        assert layer is not None

    def test_injection_filter_with_idq(self, mock_condor_config, mock_svd_stats):
        """Test injection_filter with IDQ options (lines 600-601, 610-616)."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        svd_config = mock.MagicMock()

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.torch_dtype = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snr_min = 4.0
        filter_config.search = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = None
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.idq_gate_threshold = 0.5  # Enable IDQ gate

        injection_config = mock.MagicMock()
        injection_config.filter = {
            "bbh": {"file": "/path/to/inj.xml", "noiseless_inj_frames": False}
        }

        source_config = mock.MagicMock()
        source_config.frame_segments_name = "datasegments"
        source_config.frame_segments_file = "/path/to/segments.xml"
        source_config.frame_cache = "/path/to/cache.lcf"
        source_config.inj_frame_cache = None
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.inj_channel_name = {"H1": "H1:INJ", "L1": "L1:INJ"}
        source_config.idq_channel_name = {
            "H1": "H1:IDQ",
            "L1": "L1:IDQ",
        }  # IDQ channels
        source_config.idq_state_channel_name = {
            "H1": "H1:IDQ_STATE",
            "L1": "L1:IDQ_STATE",
        }

        ref_psd_cache = mock.MagicMock()
        ref_psd_cache.groupby.return_value = {
            ("H1L1", (1000, 2000)): mock.MagicMock(files=["psd.xml.gz"])
        }

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_h1.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd_l1.xml.gz"]),
        }

        trigger_cache = mock.MagicMock()
        trigger_group = mock.MagicMock()
        trigger_group.groupby.return_value = {"0000": mock.MagicMock()}
        trigger_group.files = ["triggers.sqlite.gz"]

        def mock_chunked(n):
            yield trigger_group

        trigger_inner = mock.MagicMock(files=["triggers.sqlite.gz"])
        trigger_inner.chunked = mock_chunked
        trigger_cache.groupby.return_value = {
            ("H1L1", (1000, 2000), "bbh"): trigger_inner
        }

        layer = layers.injection_filter(
            psd_config,
            svd_config,
            filter_config,
            injection_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            trigger_cache,
            mock_svd_stats,
            min_instruments=2,
        )

        assert layer is not None

    def test_injection_filter_with_search_option(
        self, mock_condor_config, mock_svd_stats
    ):
        """Test injection_filter with search option (line 499)."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        svd_config = mock.MagicMock()

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.torch_dtype = "float64"  # Also test torch_dtype (line 503)
        filter_config.coincidence_threshold = 0.005
        filter_config.snr_min = 4.0
        filter_config.search = "AllSky"  # Enable search option
        filter_config.trigger_finding_duration = 2  # Also test this (line 512)
        filter_config.group_svd_num = 2  # Also test this (line 519)
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.idq_gate_threshold = None

        injection_config = mock.MagicMock()
        injection_config.filter = {
            "bbh": {"file": "/path/to/inj.xml", "noiseless_inj_frames": False}
        }

        source_config = mock.MagicMock()
        source_config.frame_segments_name = "datasegments"
        source_config.frame_segments_file = "/path/to/segments.xml"
        source_config.frame_cache = "/path/to/cache.lcf"
        source_config.inj_frame_cache = None
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.inj_channel_name = {"H1": "H1:INJ", "L1": "L1:INJ"}
        source_config.idq_channel_name = None
        source_config.idq_state_channel_name = None

        ref_psd_cache = mock.MagicMock()
        ref_psd_cache.groupby.return_value = {
            ("H1L1", (1000, 2000)): mock.MagicMock(files=["psd.xml.gz"])
        }

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd_h1.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd_l1.xml.gz"]),
        }

        trigger_cache = mock.MagicMock()
        trigger_group = mock.MagicMock()
        trigger_group.groupby.return_value = {"0000": mock.MagicMock()}
        trigger_group.files = ["triggers.sqlite.gz"]

        def mock_chunked(n):
            yield trigger_group

        trigger_inner = mock.MagicMock(files=["triggers.sqlite.gz"])
        trigger_inner.chunked = mock_chunked
        trigger_cache.groupby.return_value = {
            ("H1L1", (1000, 2000), "bbh"): trigger_inner
        }

        layer = layers.injection_filter(
            psd_config,
            svd_config,
            filter_config,
            injection_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            trigger_cache,
            mock_svd_stats,
            min_instruments=2,
        )

        assert layer is not None


class TestOnlineDefaultGrouping:
    """Tests for online functions with default grouping."""

    def test_filter_online_default_grouping(self, mock_condor_config, mock_svd_stats):
        """Test filter_online with neither group_svd_num nor dynamic_group."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snapshot_multiprocess = False
        filter_config.snapshot_interval = 100
        filter_config.all_triggers_to_background = False
        filter_config.search = None
        filter_config.torch_dtype = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = None  # Neither this
        filter_config.dynamic_group = None  # Nor this - triggers else branch
        filter_config.cap_singles = False
        filter_config.verbose = False
        filter_config.compress_likelihood_ratio = False
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.ht_gate_threshold = 50.0

        upload_config = mock.MagicMock()
        upload_config.far_trials_factor = 1
        upload_config.gracedb_far_threshold = 1e-6
        upload_config.aggregator_far_threshold = 1e-6
        upload_config.aggregator_far_trials_factor = 1
        upload_config.gracedb_group = "Test"
        upload_config.gracedb_search = "AllSky"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        source_config = mock.MagicMock()
        source_config.data_source = "devshm"
        source_config.source_queue_timeout = 300
        source_config.channel_name = {"H1": "H1:STRAIN", "L1": "L1:STRAIN"}
        source_config.state_channel_name = {"H1": "H1:STATE", "L1": "L1:STATE"}
        source_config.state_vector_on_bits = {"H1": "0x1", "L1": "0x1"}
        source_config.shared_memory_dir = {"H1": "/dev/shm/H1", "L1": "/dev/shm/L1"}

        ref_psd_cache = "/path/to/ref_psd.xml.gz"

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
        }

        lr_cache = mock.MagicMock()
        lr_group = mock.MagicMock()
        lr_group.groupby.return_value = {"0000": mock.MagicMock()}
        lr_group.files = ["lr.xml.gz"]

        def mock_chunked(n):
            yield lr_group

        lr_cache.chunked = mock_chunked

        zerolag_pdf_cache = mock.MagicMock()
        zerolag_pdf_cache.groupby.return_value = {
            "0000": mock.MagicMock(files=["zerolag.xml.gz"])
        }

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        layer = layers.filter_online(
            psd_config,
            filter_config,
            upload_config,
            services_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            lr_cache,
            mock_svd_stats,
            zerolag_pdf_cache,
            marg_pdf_cache,
            ifos=["H1", "L1"],
            tag="test_tag",
            min_instruments=2,
        )

        assert layer is not None

    def test_injection_filter_online_default_grouping(
        self, mock_condor_config, mock_svd_stats
    ):
        """Test injection_filter_online with default grouping."""
        psd_config = mock.MagicMock()
        psd_config.fft_length = 8

        filter_config = mock.MagicMock()
        filter_config.torch_device = None
        filter_config.coincidence_threshold = 0.005
        filter_config.snapshot_multiprocess = False
        filter_config.snapshot_interval = None
        filter_config.search = None
        filter_config.torch_dtype = None
        filter_config.trigger_finding_duration = None
        filter_config.group_svd_num = None  # Neither this
        filter_config.dynamic_group = None  # Nor this - triggers else branch
        filter_config.cap_singles = False
        filter_config.verbose = False
        filter_config.compress_likelihood_ratio = False
        filter_config.event_config_file = "/path/to/event_config.yaml"
        filter_config.ht_gate_threshold = 50.0

        upload_config = mock.MagicMock()
        upload_config.far_trials_factor = 1
        upload_config.gracedb_far_threshold = 1e-6
        upload_config.aggregator_far_threshold = 1e-6
        upload_config.aggregator_far_trials_factor = 1
        upload_config.gracedb_group = "Test"
        upload_config.inj_gracedb_search = "MDC"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        source_config = mock.MagicMock()
        source_config.data_source = "devshm"
        source_config.source_queue_timeout = 300
        source_config.inj_channel_name = {"H1": "H1:INJ", "L1": "L1:INJ"}
        source_config.state_channel_name = {"H1": "H1:STATE", "L1": "L1:STATE"}
        source_config.state_vector_on_bits = {"H1": "0x1", "L1": "0x1"}
        source_config.shared_memory_dir = {"H1": "/dev/shm/H1", "L1": "/dev/shm/L1"}

        ref_psd_cache = "/path/to/ref_psd.xml.gz"

        svd_bank_cache = mock.MagicMock()
        svd_bank_cache.groupby.return_value = {
            ("H1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
            ("L1", "0000"): mock.MagicMock(files=["svd.xml.gz"]),
        }

        lr_cache = mock.MagicMock()
        lr_group = mock.MagicMock()
        lr_group.groupby.return_value = {"0000": mock.MagicMock()}
        lr_group.files = ["lr.xml.gz"]

        def mock_chunked(n):
            yield lr_group

        lr_cache.chunked = mock_chunked

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        layer = layers.injection_filter_online(
            psd_config,
            filter_config,
            upload_config,
            services_config,
            source_config,
            mock_condor_config,
            ref_psd_cache,
            svd_bank_cache,
            lr_cache,
            mock_svd_stats,
            marg_pdf_cache,
            ifos=["H1", "L1"],
            tag="test_tag",
            min_instruments=2,
        )

        assert layer is not None

    def test_marginalize_online_default_grouping(self, mock_condor_config):
        """Test marginalize_online with default grouping."""
        filter_config = mock.MagicMock()
        filter_config.group_svd_num = None  # Neither this
        filter_config.dynamic_group = None  # Nor this - triggers else branch

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        lr_cache = mock.MagicMock()
        lr_group = mock.MagicMock()
        lr_group.groupby.return_value = {"0000": mock.MagicMock()}

        def mock_chunked(n):
            yield lr_group

        lr_cache.chunked = mock_chunked

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        layer = layers.marginalize_online(
            mock_condor_config,
            filter_config,
            services_config,
            lr_cache,
            tag="test_tag",
            marg_pdf_cache=marg_pdf_cache,
        )

        assert layer is not None


class TestRemainingCoverage:
    """Tests for remaining coverage of edge cases."""

    def test_collect_metrics_default_grouping(self, mock_condor_config):
        """Test collect_metrics with default grouping."""
        metrics_config = mock.MagicMock()
        metrics_config.scald_config = "/path/to/scald.yaml"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        filter_config = mock.MagicMock()
        filter_config.injections = False
        filter_config.group_svd_num = None  # Neither this
        filter_config.dynamic_group = None  # Nor this - triggers else branch

        layer = layers.collect_metrics(
            mock_condor_config,
            metrics_config,
            services_config,
            filter_config,
            tag="test_tag",
            ifos=["H1", "L1"],
            svd_bins=["0000"],
        )

        assert layer is not None

    def test_upload_pastro_with_injection_uploads(self, mock_condor_config):
        """Test upload_pastro with enable_injection_uploads (lines 2130-2132, 2150)."""
        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        upload_config = mock.MagicMock()
        upload_config.enable_injection_uploads = True
        upload_config.gracedb_service_url = "https://gracedb.ligo.org"
        upload_config.inj_gracedb_service_url = "https://gracedb-test.ligo.org"

        pastro_config = {
            "test_model": mock.MagicMock(
                # Use simple path format: "path/filename" for split("/")
                mass_model="path/mass_model.h5",
                upload_file="path/pastro.xml",
            )
        }

        marg_pdf_cache = mock.MagicMock()
        marg_pdf_cache.files = ["marg_pdf.xml.gz"]

        # Mock shutil.copy to avoid file operations
        with mock.patch("sgnl.dags.layers.shutil.copy"):
            layer = layers.upload_pastro(
                mock_condor_config,
                services_config,
                upload_config,
                pastro_config,
                tag="test_tag",
                marg_pdf_cache=marg_pdf_cache,
            )

        assert layer is not None

    def test_collect_metrics_over_1000_jobs(self, mock_condor_config):
        """Test collect_metrics raises ValueError when num_jobs > 1000 (line 2051)."""
        metrics_config = mock.MagicMock()
        metrics_config.scald_config = "/path/to/scald.yaml"

        services_config = mock.MagicMock()
        services_config.kafka_server = "kafka.example.com:9092"

        filter_config = mock.MagicMock()
        filter_config.injections = False
        filter_config.group_svd_num = None
        # Set dynamic_group to >1000 comma-separated values to trigger line 2051
        filter_config.dynamic_group = ",".join(["1"] * 1001)

        with pytest.raises(ValueError, match="not implemented"):
            layers.collect_metrics(
                mock_condor_config,
                metrics_config,
                services_config,
                filter_config,
                tag="test_tag",
                ifos=["H1", "L1"],
                svd_bins=["0000"],
            )

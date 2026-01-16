"""Tests for sgnl.bin.ll_marginalize_likelihoods_online"""

import sys
from unittest import mock
from urllib.error import HTTPError, URLError

import pytest


@pytest.fixture(autouse=True)
def mock_dependencies():
    """Mock external dependencies and clean up after tests."""
    sys.modules.pop("sgnl.bin.ll_marginalize_likelihoods_online", None)

    original_modules = {}

    modules_to_mock = [
        "lal",
        "lal.utils",
        "sgnligo",
        "sgnligo.base",
        "strike",
        "strike.stats",
        "strike.stats.far",
        "sgnl.events",
        "sgnl.dags.util",
    ]

    # Create fresh mocks for each test
    for mod in modules_to_mock:
        original_modules[mod] = sys.modules.get(mod)
        sys.modules[mod] = mock.MagicMock()

    # If sgnl package exists, update its events attribute to use our mock
    # This is needed to ensure test isolation when other tests have modified it
    sgnl_orig_events = None
    if "sgnl" in sys.modules:
        sgnl_orig_events = getattr(sys.modules["sgnl"], "events", None)
        sys.modules["sgnl"].events = sys.modules["sgnl.events"]

    # Set up DEFAULT_BACKUP_DIR
    sys.modules["sgnl.dags.util"].DEFAULT_BACKUP_DIR = "/tmp/backup"
    sys.modules["sgnl.dags.util"].DataType = mock.MagicMock()
    sys.modules["sgnl.dags.util"].DataCache = mock.MagicMock()
    sys.modules["sgnl.dags.util"].T050017_filename = mock.MagicMock(
        return_value="backup_file.xml.gz"
    )

    # Set up sgnligo.base.now
    sys.modules["sgnligo.base"].now = mock.MagicMock(return_value=1000000000)

    yield

    # Restore sgnl.events attribute if we modified it
    if "sgnl" in sys.modules:
        if sgnl_orig_events is not None:
            sys.modules["sgnl"].events = sgnl_orig_events
        elif hasattr(sys.modules["sgnl"], "events"):
            del sys.modules["sgnl"].events

    # Restore originals
    for mod in modules_to_mock:
        if original_modules[mod] is None:
            sys.modules.pop(mod, None)
        else:
            sys.modules[mod] = original_modules[mod]

    # Always clear the module cache to ensure fresh import next test
    sys.modules.pop("sgnl.bin.ll_marginalize_likelihoods_online", None)


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_default_values(self):
        """Test parsing with default values and required args."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        with mock.patch(
            "sys.argv",
            [
                "ll_marginalize_likelihoods_online",
                "--output",
                "output.xml",
                "--registry",
                "0000_0010_registry.txt",
            ],
        ):
            options = ll_marginalize_likelihoods_online.parse_command_line()
            assert options.output == "output.xml"
            assert options.registry == ["0000_0010_registry.txt"]
            assert options.num_cores == 1
            assert options.tag == "test"
            assert options.verbose is False
            assert options.fast_burnin is False
            assert options.extinct_percent == 0.99

    def test_missing_output_raises(self):
        """Test that missing --output raises ValueError."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        with mock.patch(
            "sys.argv",
            [
                "ll_marginalize_likelihoods_online",
                "--registry",
                "0000_0010_registry.txt",
            ],
        ):
            with pytest.raises(ValueError, match="must set --output"):
                ll_marginalize_likelihoods_online.parse_command_line()

    def test_missing_registry_raises(self):
        """Test that missing --registry raises ValueError."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        with mock.patch(
            "sys.argv",
            ["ll_marginalize_likelihoods_online", "--output", "output.xml"],
        ):
            with pytest.raises(ValueError, match="must provide at least one registry"):
                ll_marginalize_likelihoods_online.parse_command_line()

    def test_num_cores_option(self):
        """Test --num-cores option."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        with mock.patch(
            "sys.argv",
            [
                "ll_marginalize_likelihoods_online",
                "--output",
                "output.xml",
                "--registry",
                "0000_0010_registry.txt",
                "-j",
                "4",
            ],
        ):
            options = ll_marginalize_likelihoods_online.parse_command_line()
            assert options.num_cores == 4

    def test_output_kafka_server_option(self):
        """Test --output-kafka-server option."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        with mock.patch(
            "sys.argv",
            [
                "ll_marginalize_likelihoods_online",
                "--output",
                "output.xml",
                "--registry",
                "0000_0010_registry.txt",
                "--output-kafka-server",
                "kafka:9092",
            ],
        ):
            options = ll_marginalize_likelihoods_online.parse_command_line()
            assert options.output_kafka_server == "kafka:9092"

    def test_tag_option(self):
        """Test --tag option."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        with mock.patch(
            "sys.argv",
            [
                "ll_marginalize_likelihoods_online",
                "--output",
                "output.xml",
                "--registry",
                "0000_0010_registry.txt",
                "--tag",
                "production",
            ],
        ):
            options = ll_marginalize_likelihoods_online.parse_command_line()
            assert options.tag == "production"

    def test_ifo_option(self):
        """Test --ifo option."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        with mock.patch(
            "sys.argv",
            [
                "ll_marginalize_likelihoods_online",
                "--output",
                "output.xml",
                "--registry",
                "0000_0010_registry.txt",
                "--ifo",
                "H1",
                "--ifo",
                "L1",
            ],
        ):
            options = ll_marginalize_likelihoods_online.parse_command_line()
            assert options.ifo == ["H1", "L1"]

    def test_fast_burnin_option(self):
        """Test --fast-burnin option."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        with mock.patch(
            "sys.argv",
            [
                "ll_marginalize_likelihoods_online",
                "--output",
                "output.xml",
                "--registry",
                "0000_0010_registry.txt",
                "--fast-burnin",
            ],
        ):
            options = ll_marginalize_likelihoods_online.parse_command_line()
            assert options.fast_burnin is True

    def test_extinct_percent_option(self):
        """Test --extinct-percent option."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        with mock.patch(
            "sys.argv",
            [
                "ll_marginalize_likelihoods_online",
                "--output",
                "output.xml",
                "--registry",
                "0000_0010_registry.txt",
                "--extinct-percent",
                "0.95",
            ],
        ):
            options = ll_marginalize_likelihoods_online.parse_command_line()
            assert options.extinct_percent == 0.95

    def test_verbose_option(self):
        """Test --verbose option."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        with mock.patch(
            "sys.argv",
            [
                "ll_marginalize_likelihoods_online",
                "--output",
                "output.xml",
                "--registry",
                "0000_0010_registry.txt",
                "--verbose",
            ],
        ):
            options = ll_marginalize_likelihoods_online.parse_command_line()
            assert options.verbose is True

    def test_multiple_registry_files(self):
        """Test multiple --registry options."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        with mock.patch(
            "sys.argv",
            [
                "ll_marginalize_likelihoods_online",
                "--output",
                "output.xml",
                "--registry",
                "0000_0010_registry.txt",
                "--registry",
                "0011_0020_registry.txt",
            ],
        ):
            options = ll_marginalize_likelihoods_online.parse_command_line()
            assert options.registry == [
                "0000_0010_registry.txt",
                "0011_0020_registry.txt",
            ]


class TestUrlFromRegistry:
    """Tests for url_from_registry function."""

    def test_url_from_registry(self):
        """Test url_from_registry returns correct URL."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        mock_open = mock.mock_open(read_data="http://node001.ligo.caltech.edu\n")
        with mock.patch("builtins.open", mock_open):
            url = ll_marginalize_likelihoods_online.url_from_registry(
                "registry.txt", "test/path"
            )

        assert url == "http://node001.ligo.caltech.edu/test/path"
        mock_open.assert_called_once_with("registry.txt", "r")

    def test_url_from_registry_strips_newlines(self):
        """Test url_from_registry strips newlines from server."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        mock_open = mock.mock_open(read_data="http://server.example.com\n\n")
        with mock.patch("builtins.open", mock_open):
            url = ll_marginalize_likelihoods_online.url_from_registry(
                "registry.txt", "data/path"
            )

        assert "http://server.example.com" in url
        assert "\n" not in url


class TestCalcRankPdfs:
    """Tests for calc_rank_pdfs function."""

    def test_calc_rank_pdfs_success_healthy(self):
        """Test calc_rank_pdfs with healthy ranking stat."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        mock_rankingstat = mock.MagicMock()
        mock_rankingstat.is_healthy.return_value = True
        mock_copy = mock.MagicMock()
        mock_copy.is_healthy.return_value = True
        mock_rankingstat.copy.return_value = mock_copy

        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.return_value = (
            mock_rankingstat
        )
        mock_pdf = mock.MagicMock()
        ll_marginalize_likelihoods_online.far.RankingStatPDF.return_value = mock_pdf

        status, pdf = ll_marginalize_likelihoods_online.calc_rank_pdfs(
            "http://test.url", 1000, 1, verbose=True
        )

        assert status == 1
        assert pdf == mock_pdf
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.assert_called_once()
        mock_copy.finish.assert_called_once()

    def test_calc_rank_pdfs_success_unhealthy(self):
        """Test calc_rank_pdfs with unhealthy ranking stat."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        mock_rankingstat = mock.MagicMock()
        mock_copy = mock.MagicMock()
        mock_copy.is_healthy.return_value = False
        mock_rankingstat.copy.return_value = mock_copy

        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.return_value = (
            mock_rankingstat
        )
        mock_pdf = mock.MagicMock()
        ll_marginalize_likelihoods_online.far.RankingStatPDF.return_value = mock_pdf

        with mock.patch("time.sleep"):
            status, pdf = ll_marginalize_likelihoods_online.calc_rank_pdfs(
                "http://test.url", 1000, 1
            )

        assert status == 1
        assert pdf == mock_pdf
        # Should create PDF with nsamples=0 for unhealthy stat
        call_args = ll_marginalize_likelihoods_online.far.RankingStatPDF.call_args
        assert call_args[1]["nsamples"] == 0

    def test_calc_rank_pdfs_url_error(self):
        """Test calc_rank_pdfs handles URLError."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = (
            URLError("Connection refused")
        )

        status, pdf = ll_marginalize_likelihoods_online.calc_rank_pdfs(
            "http://test.url", 1000, 1
        )

        assert status == 0
        assert pdf is None

    def test_calc_rank_pdfs_http_error(self):
        """Test calc_rank_pdfs handles HTTPError."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = (
            HTTPError("http://test", 500, "Server Error", {}, None)
        )

        status, pdf = ll_marginalize_likelihoods_online.calc_rank_pdfs(
            "http://test.url", 1000, 1
        )

        assert status == 0
        assert pdf is None


class TestProcessSvdBin:
    """Tests for process_svd_bin function."""

    def _setup_mocks(self, ll_marginalize_likelihoods_online):
        """Helper to set up common mocks."""
        # Reset side_effect from any previous tests
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = None
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.side_effect = None

        mock_pdf = mock.MagicMock()
        mock_pdf.ready_for_extinction.return_value = True
        # Make __iadd__ return self so pdf += old_pdf keeps same mock
        mock_pdf.__iadd__ = mock.MagicMock(return_value=mock_pdf)

        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.return_value = (
            mock_pdf
        )
        ll_marginalize_likelihoods_online.far.RankingStatPDF.return_value = mock_pdf

        mock_rankingstat = mock.MagicMock()
        mock_copy = mock.MagicMock()
        mock_copy.is_healthy.return_value = True
        mock_rankingstat.copy.return_value = mock_copy
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.return_value = (
            mock_rankingstat
        )

        return mock_pdf

    def test_process_svd_bin_new_and_old_pdf(self):
        """Test process_svd_bin with both new and old PDF."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        mock_pdf = self._setup_mocks(ll_marginalize_likelihoods_online)

        pdfs = {"0001": mock.MagicMock()}
        pdfs["0001"].files = ["/tmp/pdf_0001.xml"]

        # Mock url_from_registry instead of builtins.open
        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                status, extinction_status, pdf = (
                    ll_marginalize_likelihoods_online.process_svd_bin(
                        "registry.txt",
                        "0001",
                        "test/likelihood",
                        "test/zerolag",
                        pdfs,
                        1000,
                        1,
                        verbose=True,
                    )
                )

        assert status == 1
        assert extinction_status == 1
        mock_pdf.save.assert_called_once()

    def test_process_svd_bin_no_old_pdf(self):
        """Test process_svd_bin without existing old PDF."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        self._setup_mocks(ll_marginalize_likelihoods_online)

        pdfs = {"0001": mock.MagicMock()}
        pdfs["0001"].files = ["/tmp/pdf_0001.xml"]

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=False):
                status, extinction_status, pdf = (
                    ll_marginalize_likelihoods_online.process_svd_bin(
                        "registry.txt",
                        "0001",
                        "test/likelihood",
                        "test/zerolag",
                        pdfs,
                        1000,
                        1,
                    )
                )

        assert status == 1
        assert extinction_status == 1

    def test_process_svd_bin_calc_fails_with_old_pdf(self):
        """Test process_svd_bin when calc fails but old PDF exists."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        # Reset side_effect from any previous tests
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = None
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.side_effect = None

        mock_old_pdf = mock.MagicMock()
        mock_old_pdf.ready_for_extinction.return_value = True
        mock_old_pdf.__iadd__ = mock.MagicMock(return_value=mock_old_pdf)

        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.return_value = (
            mock_old_pdf
        )
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = (
            URLError("Failed")
        )

        pdfs = {"0001": mock.MagicMock()}
        pdfs["0001"].files = ["/tmp/pdf_0001.xml"]

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                status, extinction_status, pdf = (
                    ll_marginalize_likelihoods_online.process_svd_bin(
                        "registry.txt",
                        "0001",
                        "test/likelihood",
                        "test/zerolag",
                        pdfs,
                        1000,
                        1,
                    )
                )

        # new_pdf_status is 0 so status should be 0
        assert status == 0
        # but extinction should still work on old pdf
        assert extinction_status == 1
        # pdf is the result of new_with_extinction() on the old pdf
        mock_old_pdf.new_with_extinction.assert_called_once()

    def test_process_svd_bin_not_ready_for_extinction(self):
        """Test process_svd_bin when PDF not ready for extinction."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        # Reset side_effect from any previous tests
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = None
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.side_effect = None

        mock_pdf = mock.MagicMock()
        mock_pdf.ready_for_extinction.return_value = False
        mock_pdf.__iadd__ = mock.MagicMock(return_value=mock_pdf)

        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.return_value = (
            mock_pdf
        )
        ll_marginalize_likelihoods_online.far.RankingStatPDF.return_value = mock_pdf

        mock_rankingstat = mock.MagicMock()
        mock_copy = mock.MagicMock()
        mock_copy.is_healthy.return_value = True
        mock_rankingstat.copy.return_value = mock_copy
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.return_value = (
            mock_rankingstat
        )

        pdfs = {"0001": mock.MagicMock()}
        pdfs["0001"].files = ["/tmp/pdf_0001.xml"]

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                status, extinction_status, pdf = (
                    ll_marginalize_likelihoods_online.process_svd_bin(
                        "registry.txt",
                        "0001",
                        "test/likelihood",
                        "test/zerolag",
                        pdfs,
                        1000,
                        1,
                    )
                )

        assert status == 1
        assert extinction_status == 0  # Not ready for extinction

    def test_process_svd_bin_zerolag_url_error(self):
        """Test process_svd_bin handles URLError when getting zerolag."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        # Reset side_effect from any previous tests
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = None
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.side_effect = None

        mock_pdf = mock.MagicMock()
        mock_pdf.ready_for_extinction.return_value = True
        mock_pdf.__iadd__ = mock.MagicMock(return_value=mock_pdf)

        # First call loads old PDF, second call for zerolag fails
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.side_effect = [
            mock_pdf,
            URLError("Failed"),
        ]
        ll_marginalize_likelihoods_online.far.RankingStatPDF.return_value = mock_pdf

        mock_rankingstat = mock.MagicMock()
        mock_copy = mock.MagicMock()
        mock_copy.is_healthy.return_value = True
        mock_rankingstat.copy.return_value = mock_copy
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.return_value = (
            mock_rankingstat
        )

        pdfs = {"0001": mock.MagicMock()}
        pdfs["0001"].files = ["/tmp/pdf_0001.xml"]

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                status, extinction_status, pdf = (
                    ll_marginalize_likelihoods_online.process_svd_bin(
                        "registry.txt",
                        "0001",
                        "test/likelihood",
                        "test/zerolag",
                        pdfs,
                        1000,
                        1,
                    )
                )

        assert status == 1
        # Still ready for extinction even though zerolag fetch failed
        assert extinction_status == 1

    def test_process_svd_bin_with_process_params(self):
        """Test process_svd_bin with process_params."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        mock_pdf = self._setup_mocks(ll_marginalize_likelihoods_online)

        pdfs = {"0001": mock.MagicMock()}
        pdfs["0001"].files = ["/tmp/pdf_0001.xml"]

        process_params = {"param1": "value1"}

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                status, extinction_status, pdf = (
                    ll_marginalize_likelihoods_online.process_svd_bin(
                        "registry.txt",
                        "0001",
                        "test/likelihood",
                        "test/zerolag",
                        pdfs,
                        1000,
                        1,
                        process_params=process_params,
                    )
                )

        assert status == 1
        # Check process_params was passed to save
        call_kwargs = mock_pdf.save.call_args
        assert call_kwargs[1]["process_params"] == process_params


class TestMain:
    """Tests for main function."""

    def _setup_main_mocks(self, ll_marginalize_likelihoods_online, break_after=1):
        """Helper to set up mocks for main function tests.

        Args:
            ll_marginalize_likelihoods_online: The module being tested
            break_after: Number of loop iterations before raising SystemExit
        """
        # Reset side_effect from any previous tests
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = None
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.side_effect = None

        # Mock CacheEntry
        mock_cache_entry = mock.MagicMock()
        mock_cache_entry.observatory = "H1L1"
        ll_marginalize_likelihoods_online.CacheEntry.from_T050017.return_value = (
            mock_cache_entry
        )

        # Mock DataCache
        mock_data_cache = mock.MagicMock()
        mock_grouped = {"0000": mock.MagicMock()}
        mock_grouped["0000"].files = ["/tmp/pdf_0000.xml"]
        mock_data_cache.groupby.return_value = mock_grouped
        ll_marginalize_likelihoods_online.DataCache.generate.return_value = (
            mock_data_cache
        )

        # Create the mock PDF - will be used as data in main()
        iteration_count = [0]

        mock_pdf = mock.MagicMock()
        mock_pdf.ready_for_extinction.return_value = True

        # Make density_estimate_zero_lag_rates raise SystemExit after break_after calls
        # This is called once per iteration at line 493 of main()
        def mock_density_estimate():
            iteration_count[0] += 1
            if iteration_count[0] > break_after:
                raise SystemExit(0)

        mock_pdf.density_estimate_zero_lag_rates = mock_density_estimate

        # Make __iadd__ return the same mock so data accumulation works
        mock_pdf.__iadd__ = mock.MagicMock(return_value=mock_pdf)

        # Make new_with_extinction return the same mock
        mock_pdf.new_with_extinction.return_value = mock_pdf

        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.return_value = (
            mock_pdf
        )
        ll_marginalize_likelihoods_online.far.RankingStatPDF.return_value = mock_pdf

        # Mock marginalize_pdf_urls
        mock_rankingstat = mock.MagicMock()
        mock_copy = mock.MagicMock()
        mock_copy.is_healthy.return_value = True
        mock_rankingstat.copy.return_value = mock_copy
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.return_value = (
            mock_rankingstat
        )

        return mock_pdf

    def test_main_single_bin_registry(self):
        """Test main with single bin registry format."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        self._setup_main_mocks(ll_marginalize_likelihoods_online)

        # Update grouped to match single bin
        mock_grouped = {"0000": mock.MagicMock()}
        mock_grouped["0000"].files = ["/tmp/pdf_0000.xml"]
        data_cache = ll_marginalize_likelihoods_online.DataCache.generate.return_value
        data_cache.groupby.return_value = mock_grouped

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                with mock.patch("os.path.exists", return_value=True):
                    with mock.patch("time.sleep"):  # Mock to skip initial 600s sleep
                        with mock.patch(
                            "sys.argv",
                            [
                                "ll_marginalize_likelihoods_online",
                                "--output",
                                "H1L1-OUTPUT-0-100.xml",
                                "--registry",
                                "0000_test_registry.txt",
                            ],
                        ):
                            with pytest.raises(SystemExit):
                                ll_marginalize_likelihoods_online.main()

    def test_main_range_registry(self):
        """Test main with range registry format (0000_0010_registry.txt)."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        self._setup_main_mocks(ll_marginalize_likelihoods_online)

        # Setup for range of bins
        mock_grouped = {}
        for i in range(11):
            bin_name = "%04d" % i
            mock_grouped[bin_name] = mock.MagicMock()
            mock_grouped[bin_name].files = [f"/tmp/pdf_{bin_name}.xml"]

        data_cache = ll_marginalize_likelihoods_online.DataCache.generate.return_value
        data_cache.groupby.return_value = mock_grouped

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                with mock.patch("os.path.exists", return_value=True):
                    with mock.patch("time.sleep"):
                        with mock.patch(
                            "sys.argv",
                            [
                                "ll_marginalize_likelihoods_online",
                                "--output",
                                "H1L1-OUTPUT-0-100.xml",
                                "--registry",
                                "0000_0010_test_registry.txt",
                            ],
                        ):
                            with pytest.raises(SystemExit):
                                ll_marginalize_likelihoods_online.main()

    def test_main_invalid_registry_format(self):
        """Test main with invalid registry format."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        with mock.patch(
            "sys.argv",
            [
                "ll_marginalize_likelihoods_online",
                "--output",
                "H1L1-OUTPUT-0-100.xml",
                "--registry",
                "invalid.txt",
            ],
        ):
            with pytest.raises(ValueError, match="Wrong name for registry file"):
                ll_marginalize_likelihoods_online.main()

    def test_main_with_kafka(self):
        """Test main with Kafka processor."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        self._setup_main_mocks(ll_marginalize_likelihoods_online)

        mock_grouped = {"0000": mock.MagicMock()}
        mock_grouped["0000"].files = ["/tmp/pdf_0000.xml"]
        data_cache = ll_marginalize_likelihoods_online.DataCache.generate.return_value
        data_cache.groupby.return_value = mock_grouped

        mock_kafka = mock.MagicMock()
        ll_marginalize_likelihoods_online.events.EventProcessor.return_value = (
            mock_kafka
        )

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                with mock.patch("os.path.exists", return_value=True):
                    with mock.patch("time.sleep"):
                        with mock.patch(
                            "sys.argv",
                            [
                                "ll_marginalize_likelihoods_online",
                                "--output",
                                "H1L1-OUTPUT-0-100.xml",
                                "--registry",
                                "0000_test_registry.txt",
                                "--output-kafka-server",
                                "kafka:9092",
                            ],
                        ):
                            with pytest.raises(SystemExit):
                                ll_marginalize_likelihoods_online.main()

        # Verify heartbeat was called
        mock_kafka.heartbeat.assert_called()

    def test_main_verbose(self):
        """Test main with verbose flag."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        self._setup_main_mocks(ll_marginalize_likelihoods_online)

        mock_grouped = {"0000": mock.MagicMock()}
        mock_grouped["0000"].files = ["/tmp/pdf_0000.xml"]
        data_cache = ll_marginalize_likelihoods_online.DataCache.generate.return_value
        data_cache.groupby.return_value = mock_grouped

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                with mock.patch("os.path.exists", return_value=True):
                    with mock.patch("time.sleep"):
                        with mock.patch(
                            "sys.argv",
                            [
                                "ll_marginalize_likelihoods_online",
                                "--output",
                                "H1L1-OUTPUT-0-100.xml",
                                "--registry",
                                "0000_test_registry.txt",
                                "--verbose",
                            ],
                        ):
                            with pytest.raises(SystemExit):
                                ll_marginalize_likelihoods_online.main()

    def test_main_too_many_failures_exits(self):
        """Test main exits when too many bins fail."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        # Reset side_effect from any previous tests
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = None
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.side_effect = None

        # Mock to simulate failures
        mock_cache_entry = mock.MagicMock()
        mock_cache_entry.observatory = "H1L1"
        ll_marginalize_likelihoods_online.CacheEntry.from_T050017.return_value = (
            mock_cache_entry
        )

        # Setup for 100 bins, we need >1% to fail
        mock_grouped = {}
        for i in range(100):
            bin_name = "%04d" % i
            mock_grouped[bin_name] = mock.MagicMock()
            mock_grouped[bin_name].files = [f"/tmp/pdf_{bin_name}.xml"]

        data_cache = ll_marginalize_likelihoods_online.DataCache.generate.return_value
        data_cache.groupby.return_value = mock_grouped

        # Always fail marginalize_pdf_urls and RankingStatPDF.load
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = (
            URLError("Failed")
        )
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.side_effect = (
            URLError("Failed")
        )

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=False):
                with mock.patch("time.sleep"):
                    with mock.patch(
                        "sys.argv",
                        [
                            "ll_marginalize_likelihoods_online",
                            "--output",
                            "H1L1-OUTPUT-0-100.xml",
                            "--registry",
                            "0000_0099_test_registry.txt",
                        ],
                    ):
                        with pytest.raises(SystemExit) as exc_info:
                            ll_marginalize_likelihoods_online.main()

        assert exc_info.value.code == 1

    def test_main_saves_backup(self):
        """Test main saves backup file when extinction threshold met."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        self._setup_main_mocks(ll_marginalize_likelihoods_online)

        mock_grouped = {"0000": mock.MagicMock()}
        mock_grouped["0000"].files = ["/tmp/pdf_0000.xml"]
        data_cache = ll_marginalize_likelihoods_online.DataCache.generate.return_value
        data_cache.groupby.return_value = mock_grouped

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                with mock.patch("os.path.exists", return_value=False):
                    with mock.patch("os.makedirs") as mock_makedirs:
                        with mock.patch("time.sleep"):
                            with mock.patch(
                                "sys.argv",
                                [
                                    "ll_marginalize_likelihoods_online",
                                    "--output",
                                    "H1L1-OUTPUT-0-100.xml",
                                    "--registry",
                                    "0000_test_registry.txt",
                                ],
                            ):
                                with pytest.raises(SystemExit):
                                    ll_marginalize_likelihoods_online.main()

        # Verify backup dir creation was attempted
        mock_makedirs.assert_called()

    def test_main_fast_burnin(self):
        """Test main with fast burnin option."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        self._setup_main_mocks(ll_marginalize_likelihoods_online)

        mock_grouped = {"0000": mock.MagicMock()}
        mock_grouped["0000"].files = ["/tmp/pdf_0000.xml"]
        data_cache = ll_marginalize_likelihoods_online.DataCache.generate.return_value
        data_cache.groupby.return_value = mock_grouped

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                with mock.patch("os.path.exists", return_value=True):
                    with mock.patch("time.sleep"):
                        with mock.patch(
                            "sys.argv",
                            [
                                "ll_marginalize_likelihoods_online",
                                "--output",
                                "H1L1-OUTPUT-0-100.xml",
                                "--registry",
                                "0000_test_registry.txt",
                                "--fast-burnin",
                            ],
                        ):
                            with pytest.raises(SystemExit):
                                ll_marginalize_likelihoods_online.main()

    def test_main_retry_failed_bins(self):
        """Test main retries failed bins."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        # Reset side_effect from any previous tests
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = None
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.side_effect = None

        iteration_count = [0]

        mock_pdf = mock.MagicMock()
        mock_pdf.ready_for_extinction.return_value = True
        mock_pdf.__iadd__ = mock.MagicMock(return_value=mock_pdf)
        mock_pdf.new_with_extinction.return_value = mock_pdf

        # Break the loop after first iteration
        def mock_density_estimate():
            iteration_count[0] += 1
            if iteration_count[0] > 1:
                raise SystemExit(0)

        mock_pdf.density_estimate_zero_lag_rates = mock_density_estimate

        mock_cache_entry = mock.MagicMock()
        mock_cache_entry.observatory = "H1L1"
        ll_marginalize_likelihoods_online.CacheEntry.from_T050017.return_value = (
            mock_cache_entry
        )

        mock_grouped = {"0000": mock.MagicMock(), "0001": mock.MagicMock()}
        mock_grouped["0000"].files = ["/tmp/pdf_0000.xml"]
        mock_grouped["0001"].files = ["/tmp/pdf_0001.xml"]
        data_cache = ll_marginalize_likelihoods_online.DataCache.generate.return_value
        data_cache.groupby.return_value = mock_grouped

        # Fail first, succeed on retry
        call_count = [0]

        def mock_marginalize(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 6:  # First 6 calls fail (2 bins * 3 retries)
                raise URLError("Failed")
            mock_stat = mock.MagicMock()
            mock_copy = mock.MagicMock()
            mock_copy.is_healthy.return_value = True
            mock_stat.copy.return_value = mock_copy
            return mock_stat

        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = (
            mock_marginalize
        )
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.return_value = (
            mock_pdf
        )
        ll_marginalize_likelihoods_online.far.RankingStatPDF.return_value = mock_pdf

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                with mock.patch("os.path.exists", return_value=True):
                    with mock.patch("time.sleep"):
                        with mock.patch(
                            "sys.argv",
                            [
                                "ll_marginalize_likelihoods_online",
                                "--output",
                                "H1L1-OUTPUT-0-100.xml",
                                "--registry",
                                "0000_0001_test_registry.txt",
                            ],
                        ):
                            with pytest.raises(SystemExit):
                                ll_marginalize_likelihoods_online.main()

    def test_main_final_retry_fails_with_old_pdf(self):
        """Test main handles final retry failure but uses old PDF."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        # Reset side_effect from any previous tests
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = None
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.side_effect = None

        iteration_count = [0]

        mock_old_pdf = mock.MagicMock()
        mock_old_pdf.ready_for_extinction.return_value = True
        mock_old_pdf.__iadd__ = mock.MagicMock(return_value=mock_old_pdf)
        mock_old_pdf.new_with_extinction.return_value = mock_old_pdf

        # Break the loop after first iteration
        def mock_density_estimate():
            iteration_count[0] += 1
            if iteration_count[0] > 1:
                raise SystemExit(0)

        mock_old_pdf.density_estimate_zero_lag_rates = mock_density_estimate

        mock_cache_entry = mock.MagicMock()
        mock_cache_entry.observatory = "H1L1"
        ll_marginalize_likelihoods_online.CacheEntry.from_T050017.return_value = (
            mock_cache_entry
        )

        mock_grouped = {"0000": mock.MagicMock()}
        mock_grouped["0000"].files = ["/tmp/pdf_0000.xml"]
        data_cache = ll_marginalize_likelihoods_online.DataCache.generate.return_value
        data_cache.groupby.return_value = mock_grouped

        # Always fail marginalization
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = (
            URLError("Failed")
        )
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.return_value = (
            mock_old_pdf
        )

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                with mock.patch("os.path.exists", return_value=True):
                    with mock.patch("time.sleep"):
                        with mock.patch(
                            "sys.argv",
                            [
                                "ll_marginalize_likelihoods_online",
                                "--output",
                                "H1L1-OUTPUT-0-100.xml",
                                "--registry",
                                "0000_test_registry.txt",
                            ],
                        ):
                            with pytest.raises(SystemExit):
                                ll_marginalize_likelihoods_online.main()

    def test_main_no_data_loads_from_zerolag(self):
        """Test main loads from zerolag when no data available."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        # Reset side_effect from any previous tests
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = None
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.side_effect = None

        iteration_count = [0]

        mock_cache_entry = mock.MagicMock()
        mock_cache_entry.observatory = "H1L1"
        ll_marginalize_likelihoods_online.CacheEntry.from_T050017.return_value = (
            mock_cache_entry
        )

        mock_grouped = {"0000": mock.MagicMock()}
        mock_grouped["0000"].files = ["/tmp/pdf_0000.xml"]
        data_cache = ll_marginalize_likelihoods_online.DataCache.generate.return_value
        data_cache.groupby.return_value = mock_grouped

        # No old PDF, marginalization fails
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = (
            URLError("Failed")
        )

        mock_pdf = mock.MagicMock()
        mock_pdf.ready_for_extinction.return_value = False
        mock_pdf.__iadd__ = mock.MagicMock(return_value=mock_pdf)

        # Break the loop after first iteration
        def mock_density_estimate():
            iteration_count[0] += 1
            if iteration_count[0] > 1:
                raise SystemExit(0)

        mock_pdf.density_estimate_zero_lag_rates = mock_density_estimate

        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.return_value = (
            mock_pdf
        )

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=False):
                with mock.patch("os.path.exists", return_value=True):
                    with mock.patch("time.sleep"):
                        with mock.patch(
                            "sys.argv",
                            [
                                "ll_marginalize_likelihoods_online",
                                "--output",
                                "H1L1-OUTPUT-0-100.xml",
                                "--registry",
                                "0000_test_registry.txt",
                            ],
                        ):
                            with pytest.raises(SystemExit):
                                ll_marginalize_likelihoods_online.main()

    def test_main_final_retry_with_kafka_and_existing_data(self):
        """Test main handles final retry with kafka and adds pdf to existing data."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        # Reset side_effect from any previous tests
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = None
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.side_effect = None

        iteration_count = [0]

        mock_pdf = mock.MagicMock()
        mock_pdf.ready_for_extinction.return_value = True
        mock_pdf.__iadd__ = mock.MagicMock(return_value=mock_pdf)
        mock_pdf.new_with_extinction.return_value = mock_pdf

        # Break the loop after first iteration
        def mock_density_estimate():
            iteration_count[0] += 1
            if iteration_count[0] > 1:
                raise SystemExit(0)

        mock_pdf.density_estimate_zero_lag_rates = mock_density_estimate

        mock_cache_entry = mock.MagicMock()
        mock_cache_entry.observatory = "H1L1"
        ll_marginalize_likelihoods_online.CacheEntry.from_T050017.return_value = (
            mock_cache_entry
        )

        # Two bins - first will succeed, second will fail all attempts
        mock_grouped = {"0000": mock.MagicMock(), "0001": mock.MagicMock()}
        mock_grouped["0000"].files = ["/tmp/pdf_0000.xml"]
        mock_grouped["0001"].files = ["/tmp/pdf_0001.xml"]
        data_cache = ll_marginalize_likelihoods_online.DataCache.generate.return_value
        data_cache.groupby.return_value = mock_grouped

        # Mock kafka
        mock_kafka = mock.MagicMock()
        ll_marginalize_likelihoods_online.events.EventProcessor.return_value = (
            mock_kafka
        )

        call_count = [0]

        def mock_marginalize(*args, **kwargs):
            call_count[0] += 1
            # Bin 0000: succeed on calls 1-3 (normal processing)
            # Bin 0001: fail on calls 4-6 (3 retries), succeed on call 7 (final retry)
            if call_count[0] <= 3:
                # First bin succeeds
                mock_stat = mock.MagicMock()
                mock_copy = mock.MagicMock()
                mock_copy.is_healthy.return_value = True
                mock_stat.copy.return_value = mock_copy
                return mock_stat
            elif call_count[0] <= 6:
                # Second bin fails in main loop
                raise URLError("Failed")
            else:
                # Second bin fails on final retry too
                raise URLError("Failed")

        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = (
            mock_marginalize
        )
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.return_value = (
            mock_pdf
        )
        ll_marginalize_likelihoods_online.far.RankingStatPDF.return_value = mock_pdf

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=True):
                with mock.patch("os.path.exists", return_value=True):
                    with mock.patch("time.sleep"):
                        with mock.patch(
                            "sys.argv",
                            [
                                "ll_marginalize_likelihoods_online",
                                "--output",
                                "H1L1-OUTPUT-0-100.xml",
                                "--registry",
                                "0000_0001_test_registry.txt",
                                "--output-kafka-server",
                                "kafka:9092",
                            ],
                        ):
                            with pytest.raises(SystemExit):
                                ll_marginalize_likelihoods_online.main()

        # Verify kafka heartbeat was called multiple times (including in retry block)
        assert mock_kafka.heartbeat.call_count >= 3

    def test_main_data_none_after_all_bins_fail(self):
        """Test main when data is None after all bins fail - loads from zerolag only."""
        from sgnl.bin import ll_marginalize_likelihoods_online

        # Reset side_effect from any previous tests
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = None
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.side_effect = None

        iteration_count = [0]

        mock_cache_entry = mock.MagicMock()
        mock_cache_entry.observatory = "H1L1"
        ll_marginalize_likelihoods_online.CacheEntry.from_T050017.return_value = (
            mock_cache_entry
        )

        # Single bin
        mock_grouped = {"0000": mock.MagicMock()}
        mock_grouped["0000"].files = ["/tmp/pdf_0000.xml"]
        data_cache = ll_marginalize_likelihoods_online.DataCache.generate.return_value
        data_cache.groupby.return_value = mock_grouped

        # All marginalize calls fail
        ll_marginalize_likelihoods_online.far.marginalize_pdf_urls.side_effect = (
            URLError("Failed")
        )

        # Create mock PDF for zerolag load
        mock_zerolag_pdf = mock.MagicMock()
        mock_zerolag_pdf.__iadd__ = mock.MagicMock(return_value=mock_zerolag_pdf)

        # Break the loop after first iteration
        def mock_density_estimate():
            iteration_count[0] += 1
            if iteration_count[0] > 1:
                raise SystemExit(0)

        mock_zerolag_pdf.density_estimate_zero_lag_rates = mock_density_estimate

        # Since os.path.isfile returns False, the old PDF load is skipped
        # in process_svd_bin. Marginalization fails, so pdf=None and no loads
        # happen in process_svd_bin. The only load call will be for zerolag.
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.return_value = (
            mock_zerolag_pdf
        )

        with mock.patch.object(
            ll_marginalize_likelihoods_online,
            "url_from_registry",
            return_value="http://server/test/path",
        ):
            with mock.patch("os.path.isfile", return_value=False):
                with mock.patch("os.path.exists", return_value=True):
                    with mock.patch("time.sleep"):
                        with mock.patch(
                            "sys.argv",
                            [
                                "ll_marginalize_likelihoods_online",
                                "--output",
                                "H1L1-OUTPUT-0-100.xml",
                                "--registry",
                                "0000_test_registry.txt",
                            ],
                        ):
                            with pytest.raises(SystemExit):
                                ll_marginalize_likelihoods_online.main()

        # Verify the load was called (this is for the zerolag at line 484)
        ll_marginalize_likelihoods_online.far.RankingStatPDF.load.assert_called()

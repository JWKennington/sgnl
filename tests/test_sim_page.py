"""Tests for sgnl.bin.sim_page"""

import tempfile
from unittest import mock

import numpy
import pytest

from sgnl.bin import sim_page


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_valid_config_schema(self):
        """Test parsing with valid config schema."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            f.write(b"config: test")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "plot-sim",
                    "-s",
                    f.name,
                ],
            ):
                args = sim_page.parse_command_line()
                assert args.config_schema == f.name

    def test_all_options(self):
        """Test parsing with all options."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            f.write(b"config: test")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "plot-sim",
                    "-s",
                    f.name,
                    "--input-db",
                    "test.db",
                    "--segments-name",
                    "custom_segments",
                    "--output-html",
                    "output.html",
                    "--far-threshold",
                    "1e-6",
                    "-v",
                ],
            ):
                args = sim_page.parse_command_line()
                assert args.config_schema == f.name
                assert args.input_db == "test.db"
                assert args.segments_name == "custom_segments"
                assert args.output_html == "output.html"
                assert args.far_threshold == 1e-6
                assert args.verbose is True

    def test_default_values(self):
        """Test default values."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            f.write(b"config: test")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "plot-sim",
                    "-s",
                    f.name,
                ],
            ):
                args = sim_page.parse_command_line()
                assert args.segments_name == "datasegments"
                assert args.output_html == "plot-sim.html"
                assert args.far_threshold == pytest.approx(1 / 86400 / 30.0)
                assert args.verbose is False

    def test_config_schema_not_provided(self):
        """Test that missing config-schema raises AssertionError."""
        with mock.patch(
            "sys.argv",
            [
                "plot-sim",
            ],
        ):
            with pytest.raises(AssertionError):
                sim_page.parse_command_line()

    def test_config_schema_file_not_exists(self):
        """Test that non-existent config-schema file raises AssertionError."""
        with mock.patch(
            "sys.argv",
            [
                "plot-sim",
                "-s",
                "/nonexistent/path/config.yaml",
            ],
        ):
            with pytest.raises(AssertionError):
                sim_page.parse_command_line()


class TestEffVsDist:
    """Tests for EffVsDist class."""

    def _create_mock_data(self, n_points, distances=None):
        """Helper to create mock missed/found data."""
        mock_data = mock.MagicMock()
        mock_data.simulation = mock.MagicMock()
        mock_data.simulation.mass1 = numpy.ones(n_points) * 10.0
        mock_data.simulation.mass2 = numpy.ones(n_points) * 10.0
        if distances is None:
            mock_data.simulation.distance = numpy.linspace(10, 100, n_points)
        else:
            mock_data.simulation.distance = numpy.array(distances)
        return mock_data

    def test_init_with_sufficient_data(self):
        """Test initialization with sufficient data points."""
        # order=6 needs at least 8 points (order + 1 + 1)
        missed = self._create_mock_data(20)
        found = self._create_mock_data(20)

        eff = sim_page.EffVsDist(missed, found)

        assert eff.order == 6
        assert len(eff.dm) == 20
        assert len(eff.df) == 20
        assert bool(eff) is True

    def test_init_with_insufficient_data(self):
        """Test initialization with insufficient data points."""
        # Need more than order + 1 points (7 for order=6)
        missed = self._create_mock_data(3)
        found = self._create_mock_data(3)

        eff = sim_page.EffVsDist(missed, found)

        assert bool(eff) is False
        assert eff.Nm is None
        assert eff.Nf is None
        assert eff.pm is None
        assert eff.pf is None
        assert eff.dint is None

    def test_init_with_mc_filter(self):
        """Test initialization with chirp mass filter."""
        missed = self._create_mock_data(20)
        found = self._create_mock_data(20)

        # Use mcstart and mcend to filter by chirp mass
        eff = sim_page.EffVsDist(missed, found, mcstart=0.0, mcend=100.0)

        assert bool(eff) is True

    def test_bool_false_when_missed_insufficient(self):
        """Test __bool__ returns False when missed data insufficient."""
        missed = self._create_mock_data(3)
        found = self._create_mock_data(20)

        eff = sim_page.EffVsDist(missed, found)

        assert bool(eff) is False

    def test_bool_false_when_found_insufficient(self):
        """Test __bool__ returns False when found data insufficient."""
        missed = self._create_mock_data(20)
        found = self._create_mock_data(3)

        eff = sim_page.EffVsDist(missed, found)

        assert bool(eff) is False

    def test_darr_with_valid_data(self):
        """Test darr method with valid data."""
        missed = self._create_mock_data(20)
        found = self._create_mock_data(20)

        eff = sim_page.EffVsDist(missed, found)
        centers, deltas = eff.darr(n=10)

        assert len(centers) == 10
        assert len(deltas) == 10

    def test_darr_with_invalid_data(self):
        """Test darr method returns None when data insufficient."""
        missed = self._create_mock_data(3)
        found = self._create_mock_data(3)

        eff = sim_page.EffVsDist(missed, found)
        centers, deltas = eff.darr()

        assert centers is None
        assert deltas is None

    def test_call_with_valid_data(self):
        """Test __call__ with valid data."""
        missed = self._create_mock_data(20)
        found = self._create_mock_data(20)

        eff = sim_page.EffVsDist(missed, found)
        result = eff(50.0)

        # Should return (num/den, low, high)
        assert len(result) == 3

    def test_call_with_invalid_data_raises_typeerror(self):
        """Test __call__ with invalid data triggers bug in source code.

        Note: The source code has a bug on line 112 where it uses
        isinstance(lnd, numpy.array) but numpy.array is a function, not a type.
        This test documents the bug and covers the else branch.
        """
        missed = self._create_mock_data(3)
        found = self._create_mock_data(3)

        eff = sim_page.EffVsDist(missed, found)
        # The else branch has a bug: isinstance(lnd, numpy.array) fails
        # because numpy.array is a function, not a type
        with pytest.raises(TypeError):
            eff(numpy.array([50.0, 60.0]))

    def test_vt_with_valid_data(self):
        """Test vt method with valid data."""
        missed = self._create_mock_data(20)
        found = self._create_mock_data(20)

        eff = sim_page.EffVsDist(missed, found)
        result = eff.vt(t=1.0)

        # Should return (middle, low, high)
        assert len(result) == 3

    def test_vt_with_invalid_data(self):
        """Test vt returns None when data insufficient."""
        missed = self._create_mock_data(3)
        found = self._create_mock_data(3)

        eff = sim_page.EffVsDist(missed, found)
        result = eff.vt(t=1.0)

        assert result is None


class TestVT:
    """Tests for VT function."""

    def test_vt_function(self):
        """Test VT function."""
        mock_missed = mock.MagicMock()
        mock_missed.simulation = mock.MagicMock()
        mock_missed.simulation.mass1 = numpy.ones(20) * 10.0
        mock_missed.simulation.mass2 = numpy.ones(20) * 10.0
        mock_missed.simulation.distance = numpy.linspace(10, 100, 20)
        mock_missed.segments = mock.MagicMock()
        mock_missed.segments.__abs__ = mock.MagicMock(return_value=1e17)

        mock_found = mock.MagicMock()
        mock_found.simulation = mock.MagicMock()
        mock_found.simulation.mass1 = numpy.ones(20) * 10.0
        mock_found.simulation.mass2 = numpy.ones(20) * 10.0
        mock_found.simulation.distance = numpy.linspace(10, 100, 20)
        mock_found.segments = mock.MagicMock()
        mock_found.segments.__abs__ = mock.MagicMock(return_value=1e17)

        mock_indb = mock.MagicMock()
        mock_indb.missed_found_by_on_ifos.return_value = (
            {frozenset(["H1", "L1"]): mock_missed},
            {frozenset(["H1", "L1"]): mock_found},
        )

        vts, vts_low, vts_high = sim_page.VT(
            mock_indb, "datasegments", ifars=numpy.array([1.0, 10.0])
        )

        assert frozenset(["H1", "L1"]) in vts
        assert len(vts[frozenset(["H1", "L1"])]) > 0

    def test_vt_function_with_insufficient_data(self):
        """Test VT function with insufficient data."""
        mock_missed = mock.MagicMock()
        mock_missed.simulation = mock.MagicMock()
        mock_missed.simulation.mass1 = numpy.ones(3) * 10.0
        mock_missed.simulation.mass2 = numpy.ones(3) * 10.0
        mock_missed.simulation.distance = numpy.array([10, 50, 100])
        mock_missed.segments = mock.MagicMock()
        mock_missed.segments.__abs__ = mock.MagicMock(return_value=1e17)

        mock_found = mock.MagicMock()
        mock_found.simulation = mock.MagicMock()
        mock_found.simulation.mass1 = numpy.ones(3) * 10.0
        mock_found.simulation.mass2 = numpy.ones(3) * 10.0
        mock_found.simulation.distance = numpy.array([10, 50, 100])
        mock_found.segments = mock.MagicMock()
        mock_found.segments.__abs__ = mock.MagicMock(return_value=1e17)

        mock_indb = mock.MagicMock()
        mock_indb.missed_found_by_on_ifos.return_value = (
            {frozenset(["H1", "L1"]): mock_missed},
            {frozenset(["H1", "L1"]): mock_found},
        )

        vts, vts_low, vts_high = sim_page.VT(
            mock_indb, "datasegments", ifars=numpy.array([1.0])
        )

        # With insufficient data, should append 0.0
        assert frozenset(["H1", "L1"]) in vts

    def test_vt_default_ifars(self):
        """Test VT function uses default ifars when None."""
        mock_missed = mock.MagicMock()
        mock_missed.simulation = mock.MagicMock()
        mock_missed.simulation.mass1 = numpy.ones(20) * 10.0
        mock_missed.simulation.mass2 = numpy.ones(20) * 10.0
        mock_missed.simulation.distance = numpy.linspace(10, 100, 20)
        mock_missed.segments = mock.MagicMock()
        mock_missed.segments.__abs__ = mock.MagicMock(return_value=1e17)

        mock_found = mock.MagicMock()
        mock_found.simulation = mock.MagicMock()
        mock_found.simulation.mass1 = numpy.ones(20) * 10.0
        mock_found.simulation.mass2 = numpy.ones(20) * 10.0
        mock_found.simulation.distance = numpy.linspace(10, 100, 20)
        mock_found.segments = mock.MagicMock()
        mock_found.segments.__abs__ = mock.MagicMock(return_value=1e17)

        mock_indb = mock.MagicMock()
        mock_indb.missed_found_by_on_ifos.return_value = (
            {frozenset(["H1"]): mock_missed},
            {frozenset(["H1"]): mock_found},
        )

        # Call with ifars=None to use default
        vts, vts_low, vts_high = sim_page.VT(mock_indb, "datasegments", ifars=None)

        # Should have called missed_found_by_on_ifos 13 times (for 10^0 to 10^12)
        assert mock_indb.missed_found_by_on_ifos.call_count == 13


class TestMain:
    """Tests for main function."""

    def test_main_function(self, tmp_path):
        """Test main function."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("config: test")
        output_html = tmp_path / "output.html"

        # Create mock data
        mock_missed = mock.MagicMock()
        mock_missed.simulation = mock.MagicMock()
        mock_missed.simulation.mass1 = numpy.ones(20) * 10.0
        mock_missed.simulation.mass2 = numpy.ones(20) * 10.0
        mock_missed.simulation.distance = numpy.linspace(10, 100, 20)
        mock_missed.simulation.time = numpy.arange(20)
        mock_missed.simulation.decisive_snr = numpy.ones(20) * 8.0
        mock_missed.simulation.network_snr = numpy.ones(20) * 10.0
        mock_missed.segments = mock.MagicMock()
        mock_missed.segments.__abs__ = mock.MagicMock(return_value=1e17)
        mock_missed.color = "red"
        mock_missed.marker = "x"

        mock_found = mock.MagicMock()
        mock_found.simulation = mock.MagicMock()
        mock_found.simulation.mass1 = numpy.ones(15) * 10.0
        mock_found.simulation.mass2 = numpy.ones(15) * 10.0
        mock_found.simulation.distance = numpy.linspace(10, 80, 15)
        mock_found.simulation.time = numpy.arange(15)
        mock_found.simulation.decisive_snr = numpy.ones(15) * 12.0
        mock_found.simulation.network_snr = numpy.ones(15) * 15.0
        mock_found.event = mock.MagicMock()
        mock_found.event.network_snr = numpy.ones(15) * 14.0
        mock_found.segments = mock.MagicMock()
        mock_found.segments.__abs__ = mock.MagicMock(return_value=1e17)
        mock_found.color = "blue"
        mock_found.marker = "o"
        mock_found.__iter__ = mock.MagicMock(
            return_value=iter([{"trigger": [{"ifo": "H1"}, {"ifo": "L1"}]}] * 15)
        )
        mock_found.__len__ = mock.MagicMock(return_value=15)

        mock_indb = mock.MagicMock()
        mock_indb.missed_found_by_on_ifos.return_value = (
            {frozenset(["H1", "L1"]): mock_missed},
            {frozenset(["H1", "L1"]): mock_found},
        )

        mock_section = mock.MagicMock()
        mock_fig = mock.MagicMock()
        mock_ax = mock.MagicMock()

        with (
            mock.patch(
                "sys.argv",
                [
                    "plot-sim",
                    "-s",
                    str(config_file),
                    "--input-db",
                    "test.db",
                    "--output-html",
                    str(output_html),
                ],
            ),
            mock.patch("sgnl.bin.sim_page.sgnlio.SgnlDB", return_value=mock_indb),
            mock.patch("sgnl.bin.sim_page.viz.Section", return_value=mock_section),
            mock.patch("sgnl.bin.sim_page.viz.plt.figure", return_value=mock_fig),
            mock.patch("sgnl.bin.sim_page.viz.plt.loglog"),
            mock.patch("sgnl.bin.sim_page.viz.plt.semilogy"),
            mock.patch("sgnl.bin.sim_page.viz.plt.fill_between"),
            mock.patch("sgnl.bin.sim_page.viz.plt.grid"),
            mock.patch("sgnl.bin.sim_page.viz.plt.legend"),
            mock.patch("sgnl.bin.sim_page.viz.plt.xlabel"),
            mock.patch("sgnl.bin.sim_page.viz.plt.ylabel"),
            mock.patch("sgnl.bin.sim_page.viz.plt.axis"),
            mock.patch("sgnl.bin.sim_page.viz.plt.gca", return_value=mock_ax),
            mock.patch("sgnl.bin.sim_page.viz.b64", return_value="base64_data"),
            mock.patch("sgnl.bin.sim_page.viz.page", return_value="<html></html>"),
        ):
            sim_page.main()

            assert output_html.exists()
            assert output_html.read_text() == "<html></html>"

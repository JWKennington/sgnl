"""Tests for sgnl.plots.psd module."""

from unittest import mock

import lal
import numpy
from lal import LIGOTimeGPS
from matplotlib import figure

from sgnl.plots import psd as plots_psd


def create_mock_psd(instrument="H1", f0=0.0, deltaF=0.125, length=16385):
    """Create a mock PSD for testing."""
    psd = lal.CreateREAL8FrequencySeries(
        name=f"{instrument} PSD",
        epoch=LIGOTimeGPS(0),
        f0=f0,
        deltaF=deltaF,
        sampleUnits=lal.Unit("strain^2 s"),
        length=length,
    )
    # Fill with realistic-ish PSD data (1e-46 is typical for LIGO)
    psd.data.data[:] = 1e-46 * numpy.ones(length)
    # Add some frequency dependence
    f = f0 + numpy.arange(length) * deltaF
    # Avoid division by zero
    f_safe = numpy.where(f > 0, f, 1.0)
    psd.data.data[:] = 1e-46 * (1 + (30.0 / f_safe) ** 2)
    return psd


def create_mock_coinc_xmldoc():
    """Create a mock coinc XML document for testing."""
    mock_doc = mock.MagicMock()

    # Mock CoincEvent
    mock_coinc_event = mock.MagicMock()

    # Mock CoincInspiral
    mock_coinc_inspiral = mock.MagicMock()
    mock_coinc_inspiral.end = LIGOTimeGPS(1000000000)
    mock_coinc_inspiral.ifos = {"H1", "L1"}

    # Mock SnglInspiral rows
    mock_sngl_h1 = mock.MagicMock()
    mock_sngl_h1.ifo = "H1"
    mock_sngl_h1.mass1 = 1.4
    mock_sngl_h1.mass2 = 1.4
    mock_sngl_h1.snr = 10.0

    mock_sngl_l1 = mock.MagicMock()
    mock_sngl_l1.ifo = "L1"
    mock_sngl_l1.mass1 = 1.4
    mock_sngl_l1.mass2 = 1.4
    mock_sngl_l1.snr = 8.0

    return mock_doc, mock_coinc_event, mock_coinc_inspiral, [mock_sngl_h1, mock_sngl_l1]


class TestSummarizCoincXmldoc:
    """Tests for summarize_coinc_xmldoc function."""

    def test_summarize_coinc_xmldoc_basic(self):
        """Test basic summarization of coinc xmldoc."""
        mock_doc, mock_coinc_event, mock_coinc_inspiral, mock_sngls = (
            create_mock_coinc_xmldoc()
        )

        with (
            mock.patch.object(
                plots_psd.lsctables.CoincTable,
                "get_table",
                return_value=[mock_coinc_event],
            ),
            mock.patch.object(
                plots_psd.lsctables.CoincInspiralTable,
                "get_table",
                return_value=[mock_coinc_inspiral],
            ),
            mock.patch.object(
                plots_psd.lsctables.SnglInspiralTable,
                "get_table",
                return_value=mock_sngls,
            ),
        ):
            sngl_inspirals, mass1, mass2, end_time, on_instruments = (
                plots_psd.summarize_coinc_xmldoc(mock_doc)
            )

            assert "H1" in sngl_inspirals
            assert "L1" in sngl_inspirals
            assert mass1 == 1.4
            assert mass2 == 1.4
            assert end_time == mock_coinc_inspiral.end
            assert on_instruments == {"H1", "L1"}

    def test_summarize_coinc_xmldoc_mass_swap(self):
        """Test that masses are swapped if mass1 < mass2."""
        mock_doc, mock_coinc_event, mock_coinc_inspiral, mock_sngls = (
            create_mock_coinc_xmldoc()
        )
        # Set mass1 < mass2
        mock_sngls[0].mass1 = 1.2
        mock_sngls[0].mass2 = 1.6

        with (
            mock.patch.object(
                plots_psd.lsctables.CoincTable,
                "get_table",
                return_value=[mock_coinc_event],
            ),
            mock.patch.object(
                plots_psd.lsctables.CoincInspiralTable,
                "get_table",
                return_value=[mock_coinc_inspiral],
            ),
            mock.patch.object(
                plots_psd.lsctables.SnglInspiralTable,
                "get_table",
                return_value=mock_sngls,
            ),
        ):
            _, mass1, mass2, _, _ = plots_psd.summarize_coinc_xmldoc(mock_doc)

            # masses should be swapped so mass1 >= mass2
            assert mass1 == 1.6
            assert mass2 == 1.2


class TestLatexHorizonDistance:
    """Tests for latex_horizon_distance function."""

    def test_latex_horizon_distance_gpc(self):
        """Test latex formatting for Gpc distances."""
        result = plots_psd.latex_horizon_distance(500.0)
        assert "Gpc" in result

    def test_latex_horizon_distance_mpc(self):
        """Test latex formatting for Mpc distances."""
        result = plots_psd.latex_horizon_distance(100.0)
        assert "Mpc" in result

    def test_latex_horizon_distance_kpc(self):
        """Test latex formatting for kpc distances."""
        result = plots_psd.latex_horizon_distance(0.1)
        assert "kpc" in result

    def test_latex_horizon_distance_pc(self):
        """Test latex formatting for pc distances."""
        result = plots_psd.latex_horizon_distance(0.0001)
        assert "pc" in result


class TestAxesPlotPsds:
    """Tests for axes_plot_psds function."""

    def test_axes_plot_psds_without_coinc(self):
        """Test PSD plotting without coinc document."""
        fig = figure.Figure()
        axes = fig.add_subplot(111)

        psds = {
            "H1": create_mock_psd("H1"),
            "L1": create_mock_psd("L1"),
        }

        with mock.patch.object(plots_psd, "HorizonDistance") as mock_hd:
            mock_hd_instance = mock.MagicMock()
            mock_hd_instance.return_value = (
                100.0,
                (numpy.array([10, 20]), numpy.array([1e-46, 1e-46])),
            )
            mock_hd.return_value = mock_hd_instance

            plots_psd.axes_plot_psds(axes, psds, coinc_xmldoc=None)

            # Check that horizon distance was computed
            assert mock_hd.call_count == 2

    def test_axes_plot_psds_with_coinc(self):
        """Test PSD plotting with coinc document."""
        fig = figure.Figure()
        axes = fig.add_subplot(111)

        psds = {
            "H1": create_mock_psd("H1"),
            "L1": create_mock_psd("L1"),
        }

        mock_doc, mock_coinc_event, mock_coinc_inspiral, mock_sngls = (
            create_mock_coinc_xmldoc()
        )

        with (
            mock.patch.object(
                plots_psd.lsctables.CoincTable,
                "get_table",
                return_value=[mock_coinc_event],
            ),
            mock.patch.object(
                plots_psd.lsctables.CoincInspiralTable,
                "get_table",
                return_value=[mock_coinc_inspiral],
            ),
            mock.patch.object(
                plots_psd.lsctables.SnglInspiralTable,
                "get_table",
                return_value=mock_sngls,
            ),
            mock.patch.object(plots_psd, "HorizonDistance") as mock_hd,
        ):
            mock_hd_instance = mock.MagicMock()
            mock_hd_instance.return_value = (
                100.0,
                (numpy.array([10, 20]), numpy.array([1e-46, 1e-46])),
            )
            mock_hd.return_value = mock_hd_instance

            plots_psd.axes_plot_psds(axes, psds, coinc_xmldoc=mock_doc)

    def test_axes_plot_psds_with_none_psd(self):
        """Test PSD plotting with a None PSD value."""
        fig = figure.Figure()
        axes = fig.add_subplot(111)

        psds = {
            "H1": create_mock_psd("H1"),
            "L1": None,  # None PSD should be skipped
        }

        with mock.patch.object(plots_psd, "HorizonDistance") as mock_hd:
            mock_hd_instance = mock.MagicMock()
            mock_hd_instance.return_value = (
                100.0,
                (numpy.array([10, 20]), numpy.array([1e-46, 1e-46])),
            )
            mock_hd.return_value = mock_hd_instance

            plots_psd.axes_plot_psds(axes, psds, coinc_xmldoc=None)

            # Only H1 should have horizon distance computed
            assert mock_hd.call_count == 1

    def test_axes_plot_psds_empty_psds(self):
        """Test PSD plotting with empty PSDs dict."""
        fig = figure.Figure()
        axes = fig.add_subplot(111)

        psds = {}

        plots_psd.axes_plot_psds(axes, psds, coinc_xmldoc=None)

        # Should set default xlim
        assert axes.get_xlim() == (6.0, 3000.0)

    def test_axes_plot_psds_off_instrument(self):
        """Test PSD plotting with an instrument that is off."""
        fig = figure.Figure()
        axes = fig.add_subplot(111)

        psds = {
            "H1": create_mock_psd("H1"),
            "L1": create_mock_psd("L1"),
            "V1": create_mock_psd("V1"),  # V1 will be "off"
        }

        mock_doc, mock_coinc_event, mock_coinc_inspiral, mock_sngls = (
            create_mock_coinc_xmldoc()
        )
        # Only H1 and L1 are on
        mock_coinc_inspiral.ifos = {"H1", "L1"}

        with (
            mock.patch.object(
                plots_psd.lsctables.CoincTable,
                "get_table",
                return_value=[mock_coinc_event],
            ),
            mock.patch.object(
                plots_psd.lsctables.CoincInspiralTable,
                "get_table",
                return_value=[mock_coinc_inspiral],
            ),
            mock.patch.object(
                plots_psd.lsctables.SnglInspiralTable,
                "get_table",
                return_value=mock_sngls,
            ),
            mock.patch.object(plots_psd, "HorizonDistance") as mock_hd,
        ):
            mock_hd_instance = mock.MagicMock()
            mock_hd_instance.return_value = (
                100.0,
                (numpy.array([10, 20]), numpy.array([1e-46, 1e-46])),
            )
            mock_hd.return_value = mock_hd_instance

            plots_psd.axes_plot_psds(axes, psds, coinc_xmldoc=mock_doc)

            # All 3 PSDs should be plotted
            assert mock_hd.call_count == 3


class TestPlotPsds:
    """Tests for plot_psds function."""

    def test_plot_psds_basic(self):
        """Test basic PSD plot generation."""
        psds = {
            "H1": create_mock_psd("H1"),
        }

        with mock.patch.object(plots_psd, "HorizonDistance") as mock_hd:
            mock_hd_instance = mock.MagicMock()
            mock_hd_instance.return_value = (
                100.0,
                (numpy.array([10, 20]), numpy.array([1e-46, 1e-46])),
            )
            mock_hd.return_value = mock_hd_instance

            fig = plots_psd.plot_psds(psds)

            assert isinstance(fig, figure.Figure)

    def test_plot_psds_with_coinc(self):
        """Test PSD plot generation with coinc document."""
        psds = {
            "H1": create_mock_psd("H1"),
        }

        mock_doc, mock_coinc_event, mock_coinc_inspiral, mock_sngls = (
            create_mock_coinc_xmldoc()
        )

        with (
            mock.patch.object(
                plots_psd.lsctables.CoincTable,
                "get_table",
                return_value=[mock_coinc_event],
            ),
            mock.patch.object(
                plots_psd.lsctables.CoincInspiralTable,
                "get_table",
                return_value=[mock_coinc_inspiral],
            ),
            mock.patch.object(
                plots_psd.lsctables.SnglInspiralTable,
                "get_table",
                return_value=mock_sngls,
            ),
            mock.patch.object(plots_psd, "HorizonDistance") as mock_hd,
        ):
            mock_hd_instance = mock.MagicMock()
            mock_hd_instance.return_value = (
                100.0,
                (numpy.array([10, 20]), numpy.array([1e-46, 1e-46])),
            )
            mock_hd.return_value = mock_hd_instance

            fig = plots_psd.plot_psds(psds, coinc_xmldoc=mock_doc)

            assert isinstance(fig, figure.Figure)

    def test_plot_psds_custom_width(self):
        """Test PSD plot generation with custom width."""
        psds = {
            "H1": create_mock_psd("H1"),
        }

        with mock.patch.object(plots_psd, "HorizonDistance") as mock_hd:
            mock_hd_instance = mock.MagicMock()
            mock_hd_instance.return_value = (
                100.0,
                (numpy.array([10, 20]), numpy.array([1e-46, 1e-46])),
            )
            mock_hd.return_value = mock_hd_instance

            fig = plots_psd.plot_psds(psds, plot_width=800)

            assert isinstance(fig, figure.Figure)


class TestAxesPlotCummulativeSnr:
    """Tests for axes_plot_cummulative_snr function."""

    def test_axes_plot_cummulative_snr_basic(self):
        """Test cumulative SNR plotting."""
        fig = figure.Figure()
        axes = fig.add_subplot(111)

        # Create PSD with specific parameters
        deltaF = 0.125
        length = 16385
        psds = {
            "H1": create_mock_psd("H1", deltaF=deltaF, length=length),
            "L1": create_mock_psd("L1", deltaF=deltaF, length=length),
        }

        mock_doc, mock_coinc_event, mock_coinc_inspiral, mock_sngls = (
            create_mock_coinc_xmldoc()
        )

        with (
            mock.patch.object(
                plots_psd.lsctables.CoincTable,
                "get_table",
                return_value=[mock_coinc_event],
            ),
            mock.patch.object(
                plots_psd.lsctables.CoincInspiralTable,
                "get_table",
                return_value=[mock_coinc_inspiral],
            ),
            mock.patch.object(
                plots_psd.lsctables.SnglInspiralTable,
                "get_table",
                return_value=mock_sngls,
            ),
            mock.patch.object(plots_psd, "HorizonDistance") as mock_hd,
        ):
            mock_hd_instance = mock.MagicMock()
            # Create spectrum with matching frequency bins
            # The function calculates lo/hi based on spectrum frequencies
            # lo = int(round((inspiral_spectrum_x[0] - psd.f0) / psd.deltaF))
            # For f0=0, deltaF=0.125, if spectrum starts at 10 Hz: lo=80
            # If spectrum ends at 1000 Hz: hi=8001
            # We need the spectrum to have (hi - lo) = 7921 elements
            lo_freq = 10.0
            hi_freq = 1000.0
            lo_idx = int(round(lo_freq / deltaF))
            hi_idx = int(round(hi_freq / deltaF)) + 1
            num_elements = hi_idx - lo_idx
            f = numpy.linspace(lo_freq, hi_freq, num_elements)
            spectrum = numpy.ones(num_elements) * 1e-46
            mock_hd_instance.return_value = (100.0, (f, spectrum))
            mock_hd.return_value = mock_hd_instance

            plots_psd.axes_plot_cummulative_snr(axes, psds, mock_doc)

    def test_axes_plot_cummulative_snr_missing_psd(self):
        """Test cumulative SNR plotting with missing PSD."""
        fig = figure.Figure()
        axes = fig.add_subplot(111)

        deltaF = 0.125
        length = 16385

        # Only H1 PSD, but sngl_inspirals has both H1 and L1
        psds = {
            "H1": create_mock_psd("H1", deltaF=deltaF, length=length),
        }

        mock_doc, mock_coinc_event, mock_coinc_inspiral, mock_sngls = (
            create_mock_coinc_xmldoc()
        )

        with (
            mock.patch.object(
                plots_psd.lsctables.CoincTable,
                "get_table",
                return_value=[mock_coinc_event],
            ),
            mock.patch.object(
                plots_psd.lsctables.CoincInspiralTable,
                "get_table",
                return_value=[mock_coinc_inspiral],
            ),
            mock.patch.object(
                plots_psd.lsctables.SnglInspiralTable,
                "get_table",
                return_value=mock_sngls,
            ),
            mock.patch.object(plots_psd, "HorizonDistance") as mock_hd,
        ):
            mock_hd_instance = mock.MagicMock()
            lo_freq = 10.0
            hi_freq = 1000.0
            lo_idx = int(round(lo_freq / deltaF))
            hi_idx = int(round(hi_freq / deltaF)) + 1
            num_elements = hi_idx - lo_idx
            f = numpy.linspace(lo_freq, hi_freq, num_elements)
            spectrum = numpy.ones(num_elements) * 1e-46
            mock_hd_instance.return_value = (100.0, (f, spectrum))
            mock_hd.return_value = mock_hd_instance

            # Should not raise, just skip L1
            plots_psd.axes_plot_cummulative_snr(axes, psds, mock_doc)

    def test_axes_plot_cummulative_snr_none_psd(self):
        """Test cumulative SNR plotting with None PSD value."""
        fig = figure.Figure()
        axes = fig.add_subplot(111)

        deltaF = 0.125
        length = 16385

        psds = {
            "H1": create_mock_psd("H1", deltaF=deltaF, length=length),
            "L1": None,
        }

        mock_doc, mock_coinc_event, mock_coinc_inspiral, mock_sngls = (
            create_mock_coinc_xmldoc()
        )

        with (
            mock.patch.object(
                plots_psd.lsctables.CoincTable,
                "get_table",
                return_value=[mock_coinc_event],
            ),
            mock.patch.object(
                plots_psd.lsctables.CoincInspiralTable,
                "get_table",
                return_value=[mock_coinc_inspiral],
            ),
            mock.patch.object(
                plots_psd.lsctables.SnglInspiralTable,
                "get_table",
                return_value=mock_sngls,
            ),
            mock.patch.object(plots_psd, "HorizonDistance") as mock_hd,
        ):
            mock_hd_instance = mock.MagicMock()
            lo_freq = 10.0
            hi_freq = 1000.0
            lo_idx = int(round(lo_freq / deltaF))
            hi_idx = int(round(hi_freq / deltaF)) + 1
            num_elements = hi_idx - lo_idx
            f = numpy.linspace(lo_freq, hi_freq, num_elements)
            spectrum = numpy.ones(num_elements) * 1e-46
            mock_hd_instance.return_value = (100.0, (f, spectrum))
            mock_hd.return_value = mock_hd_instance

            plots_psd.axes_plot_cummulative_snr(axes, psds, mock_doc)


class TestPlotCumulativeSnrs:
    """Tests for plot_cumulative_snrs function."""

    def test_plot_cumulative_snrs_basic(self):
        """Test cumulative SNR plot generation."""
        deltaF = 0.125
        length = 16385
        psds = {
            "H1": create_mock_psd("H1", deltaF=deltaF, length=length),
        }

        mock_doc, mock_coinc_event, mock_coinc_inspiral, mock_sngls = (
            create_mock_coinc_xmldoc()
        )

        with (
            mock.patch.object(
                plots_psd.lsctables.CoincTable,
                "get_table",
                return_value=[mock_coinc_event],
            ),
            mock.patch.object(
                plots_psd.lsctables.CoincInspiralTable,
                "get_table",
                return_value=[mock_coinc_inspiral],
            ),
            mock.patch.object(
                plots_psd.lsctables.SnglInspiralTable,
                "get_table",
                return_value=mock_sngls,
            ),
            mock.patch.object(plots_psd, "HorizonDistance") as mock_hd,
        ):
            mock_hd_instance = mock.MagicMock()
            lo_freq = 10.0
            hi_freq = 1000.0
            lo_idx = int(round(lo_freq / deltaF))
            hi_idx = int(round(hi_freq / deltaF)) + 1
            num_elements = hi_idx - lo_idx
            f = numpy.linspace(lo_freq, hi_freq, num_elements)
            spectrum = numpy.ones(num_elements) * 1e-46
            mock_hd_instance.return_value = (100.0, (f, spectrum))
            mock_hd.return_value = mock_hd_instance

            fig = plots_psd.plot_cumulative_snrs(psds, mock_doc)

            assert isinstance(fig, figure.Figure)

    def test_plot_cumulative_snrs_custom_width(self):
        """Test cumulative SNR plot generation with custom width."""
        deltaF = 0.125
        length = 16385
        psds = {
            "H1": create_mock_psd("H1", deltaF=deltaF, length=length),
        }

        mock_doc, mock_coinc_event, mock_coinc_inspiral, mock_sngls = (
            create_mock_coinc_xmldoc()
        )

        with (
            mock.patch.object(
                plots_psd.lsctables.CoincTable,
                "get_table",
                return_value=[mock_coinc_event],
            ),
            mock.patch.object(
                plots_psd.lsctables.CoincInspiralTable,
                "get_table",
                return_value=[mock_coinc_inspiral],
            ),
            mock.patch.object(
                plots_psd.lsctables.SnglInspiralTable,
                "get_table",
                return_value=mock_sngls,
            ),
            mock.patch.object(plots_psd, "HorizonDistance") as mock_hd,
        ):
            mock_hd_instance = mock.MagicMock()
            lo_freq = 10.0
            hi_freq = 1000.0
            lo_idx = int(round(lo_freq / deltaF))
            hi_idx = int(round(hi_freq / deltaF)) + 1
            num_elements = hi_idx - lo_idx
            f = numpy.linspace(lo_freq, hi_freq, num_elements)
            spectrum = numpy.ones(num_elements) * 1e-46
            mock_hd_instance.return_value = (100.0, (f, spectrum))
            mock_hd.return_value = mock_hd_instance

            fig = plots_psd.plot_cumulative_snrs(psds, mock_doc, plot_width=800)

            assert isinstance(fig, figure.Figure)

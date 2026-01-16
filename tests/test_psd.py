"""Tests for sgnl.psd module."""

import math

import lal
import lalsimulation
import numpy
import pytest
from lal import LIGOTimeGPS

from sgnl import psd as psd_module


def create_test_psd(f0=0.0, deltaF=1.0, length=2049, name="test_psd"):
    """Create a test PSD for testing."""
    test_psd = lal.CreateREAL8FrequencySeries(
        name=name,
        epoch=LIGOTimeGPS(0),
        f0=f0,
        deltaF=deltaF,
        sampleUnits=lal.Unit("strain^2 s"),
        length=length,
    )
    # Fill with realistic PSD data
    f = f0 + numpy.arange(length) * deltaF
    f_safe = numpy.where(f > 0, f, 1.0)
    test_psd.data.data[:] = 1e-46 * (1 + (30.0 / f_safe) ** 2)
    return test_psd


class TestReadPsd:
    """Tests for read_psd function."""

    def test_read_psd_basic(self, tmp_path):
        """Test reading a PSD from XML file."""
        # Create a test PSD and write it
        test_psd = create_test_psd()
        psd_dict = {"H1": test_psd}

        xml_file = tmp_path / "test_psd.xml"
        psd_module.write_psd(str(xml_file), psd_dict)

        # Read it back
        result = psd_module.read_psd(str(xml_file))

        assert "H1" in result
        assert result["H1"].data.length == test_psd.data.length

    def test_read_psd_verbose(self, tmp_path):
        """Test reading a PSD with verbose output."""
        test_psd = create_test_psd()
        psd_dict = {"H1": test_psd}

        xml_file = tmp_path / "test_psd.xml"
        psd_module.write_psd(str(xml_file), psd_dict)

        result = psd_module.read_psd(str(xml_file), verbose=True)
        assert "H1" in result


class TestWritePsd:
    """Tests for write_psd function."""

    def test_write_psd_basic(self, tmp_path):
        """Test writing a PSD to XML file."""
        test_psd = create_test_psd()
        psd_dict = {"H1": test_psd}

        xml_file = tmp_path / "test_psd.xml"
        psd_module.write_psd(str(xml_file), psd_dict)

        assert xml_file.exists()

    def test_write_psd_verbose(self, tmp_path):
        """Test writing a PSD with verbose output."""
        test_psd = create_test_psd()
        psd_dict = {"H1": test_psd}

        xml_file = tmp_path / "test_psd.xml"
        psd_module.write_psd(str(xml_file), psd_dict, verbose=True)

        assert xml_file.exists()

    def test_write_psd_multiple_instruments(self, tmp_path):
        """Test writing PSDs for multiple instruments."""
        psd_dict = {
            "H1": create_test_psd(name="H1_psd"),
            "L1": create_test_psd(name="L1_psd"),
        }

        xml_file = tmp_path / "test_psd.xml"
        psd_module.write_psd(str(xml_file), psd_dict)

        assert xml_file.exists()


class TestReadAsdTxt:
    """Tests for read_asd_txt function."""

    def test_read_asd_txt_basic(self, tmp_path):
        """Test reading ASD from text file."""
        # Create a test ASD file
        asd_file = tmp_path / "test_asd.txt"
        freqs = numpy.arange(10, 1000, 1.0)
        asd_values = 1e-23 * numpy.ones_like(freqs)

        with open(asd_file, "w") as f:
            for freq, asd in zip(freqs, asd_values):
                f.write(f"{freq} {asd}\n")

        result = psd_module.read_asd_txt(str(asd_file))

        assert isinstance(result, lal.REAL8FrequencySeries)
        # ASD should be squared to get PSD
        assert result.data.data[0] == pytest.approx(1e-46, rel=1e-3)

    def test_read_asd_txt_with_zero_pad(self, tmp_path):
        """Test reading ASD with zero padding."""
        asd_file = tmp_path / "test_asd.txt"
        freqs = numpy.arange(10, 100, 1.0)
        asd_values = 1e-23 * numpy.ones_like(freqs)

        with open(asd_file, "w") as f:
            for freq, asd in zip(freqs, asd_values):
                f.write(f"{freq} {asd}\n")

        result = psd_module.read_asd_txt(str(asd_file), zero_pad=True)

        # Should have data starting from 0 Hz
        assert result.f0 == 0.0

    def test_read_asd_txt_read_as_psd(self, tmp_path):
        """Test reading file as PSD (not ASD)."""
        psd_file = tmp_path / "test_psd.txt"
        freqs = numpy.arange(10, 100, 1.0)
        psd_values = 1e-46 * numpy.ones_like(freqs)

        with open(psd_file, "w") as f:
            for freq, psd_val in zip(freqs, psd_values):
                f.write(f"{freq} {psd_val}\n")

        result = psd_module.read_asd_txt(str(psd_file), read_as_psd=True)

        # Should not square the values
        assert result.data.data[0] == pytest.approx(1e-46, rel=1e-3)

    def test_read_asd_txt_custom_df(self, tmp_path):
        """Test reading ASD with custom frequency resolution."""
        asd_file = tmp_path / "test_asd.txt"
        freqs = numpy.arange(10, 100, 0.5)
        asd_values = 1e-23 * numpy.ones_like(freqs)

        with open(asd_file, "w") as f:
            for freq, asd in zip(freqs, asd_values):
                f.write(f"{freq} {asd}\n")

        result = psd_module.read_asd_txt(str(asd_file), df=0.5)

        assert result.deltaF == 0.5


class TestWriteAsdTxt:
    """Tests for write_asd_txt function."""

    def test_write_asd_txt_basic(self, tmp_path):
        """Test writing ASD to text file."""
        test_psd = create_test_psd(f0=10.0, deltaF=1.0, length=100)
        asd_file = tmp_path / "test_asd.txt"

        psd_module.write_asd_txt(str(asd_file), test_psd)

        assert asd_file.exists()
        # Read back and verify
        data = numpy.loadtxt(str(asd_file))
        assert len(data) == 100
        # First column should be frequency
        assert data[0, 0] == pytest.approx(10.0)
        # Second column should be ASD (sqrt of PSD)
        assert data[0, 1] == pytest.approx(numpy.sqrt(test_psd.data.data[0]))

    def test_write_asd_txt_verbose(self, tmp_path, capsys):
        """Test writing ASD with verbose output."""
        test_psd = create_test_psd(f0=10.0, deltaF=1.0, length=100)
        asd_file = tmp_path / "test_asd.txt"

        psd_module.write_asd_txt(str(asd_file), test_psd, verbose=True)

        captured = capsys.readouterr()
        assert "writing" in captured.err


class TestInterpolatePsd:
    """Tests for interpolate_psd function."""

    def test_interpolate_psd_same_deltaf(self):
        """Test that same deltaF returns original PSD."""
        test_psd = create_test_psd(deltaF=1.0)
        result = psd_module.interpolate_psd(test_psd, 1.0)

        # Should return the same object
        assert result is test_psd

    def test_interpolate_psd_finer_resolution(self):
        """Test interpolating to finer frequency resolution."""
        test_psd = create_test_psd(deltaF=1.0, length=101)
        result = psd_module.interpolate_psd(test_psd, 0.5)

        assert result.deltaF == 0.5
        # Should have roughly twice as many bins
        assert result.data.length > test_psd.data.length

    def test_interpolate_psd_coarser_resolution(self):
        """Test interpolating to coarser frequency resolution."""
        test_psd = create_test_psd(deltaF=0.5, length=201)
        result = psd_module.interpolate_psd(test_psd, 1.0)

        assert result.deltaF == 1.0
        # Should have roughly half as many bins
        assert result.data.length < test_psd.data.length

    def test_interpolate_psd_preserves_metadata(self):
        """Test that interpolation preserves PSD metadata."""
        test_psd = create_test_psd(f0=10.0, deltaF=1.0, length=101, name="my_psd")
        result = psd_module.interpolate_psd(test_psd, 0.5)

        assert result.f0 == test_psd.f0
        assert result.name == test_psd.name


class TestMovingmedian:
    """Tests for movingmedian function."""

    def test_movingmedian_numpy_array(self):
        """Test moving median on numpy array."""
        data = numpy.ones(100) * 1e-46
        # Add some spikes
        data[50] = 1e-40

        result = psd_module.movingmedian(data, window_size=5)

        assert isinstance(result, numpy.ndarray)
        assert len(result) == len(data)

    def test_movingmedian_lal_series(self):
        """Test moving median on lal FrequencySeries."""
        test_psd = create_test_psd(length=100)

        result = psd_module.movingmedian(test_psd, window_size=5)

        assert isinstance(result, lal.REAL8FrequencySeries)
        assert result.data.length == test_psd.data.length


class TestMovingaverage:
    """Tests for movingaverage function."""

    def test_movingaverage_basic(self):
        """Test moving average on numpy array."""
        data = numpy.ones(100) * 1e-46

        result = psd_module.movingaverage(data, window_size=10)

        assert isinstance(result, numpy.ndarray)
        assert len(result) == len(data)

    def test_movingaverage_smooths_data(self):
        """Test that moving average produces smoothed output."""
        data = numpy.ones(100) * 1e-46

        result = psd_module.movingaverage(data, window_size=10)

        # Result should be similar to input for constant data
        assert isinstance(result, numpy.ndarray)
        assert len(result) == len(data)


class TestTaperzeroFseries:
    """Tests for taperzero_fseries function."""

    def test_taperzero_fseries_basic(self):
        """Test tapering a frequency series."""
        # Create a large enough array for the tapering
        length = 8192
        test_psd = create_test_psd(f0=0.0, deltaF=0.5, length=length)

        result = psd_module.taperzero_fseries(test_psd)

        assert isinstance(result, lal.REAL8FrequencySeries)
        assert result.data.length == length

    def test_taperzero_fseries_custom_bounds(self):
        """Test tapering with custom frequency bounds."""
        length = 8192
        test_psd = create_test_psd(f0=0.0, deltaF=0.5, length=length)

        result = psd_module.taperzero_fseries(
            test_psd, minfs=(20.0, 30.0), maxfs=(1000.0, 1200.0)
        )

        assert isinstance(result, lal.REAL8FrequencySeries)


class TestConditionPsd:
    """Tests for condition_psd function."""

    def test_condition_psd_basic(self):
        """Test conditioning a PSD."""
        # Create a PSD with enough frequency range
        test_psd = create_test_psd(f0=0.0, deltaF=0.125, length=16385)

        result = psd_module.condition_psd(test_psd, newdeltaF=0.125)

        assert isinstance(result, lal.REAL8FrequencySeries)

    def test_condition_psd_with_fir_whiten(self):
        """Test conditioning a PSD with FIR whitening."""
        test_psd = create_test_psd(f0=0.0, deltaF=0.125, length=16385)

        result = psd_module.condition_psd(test_psd, newdeltaF=0.125, fir_whiten=True)

        assert isinstance(result, lal.REAL8FrequencySeries)

    def test_condition_psd_custom_frequency_bounds(self):
        """Test conditioning with custom frequency bounds."""
        test_psd = create_test_psd(f0=0.0, deltaF=0.125, length=16385)

        result = psd_module.condition_psd(
            test_psd,
            newdeltaF=0.125,
            minfs=(20.0, 30.0),
            maxfs=(1000.0, 1200.0),
        )

        assert isinstance(result, lal.REAL8FrequencySeries)


class TestPolyfit:
    """Tests for polyfit function."""

    def test_polyfit_basic(self):
        """Test polynomial fitting of PSD."""
        test_psd = create_test_psd(f0=0.0, deltaF=1.0, length=2049)

        result = psd_module.polyfit(test_psd, f_low=50.0, f_high=500.0, order=3)

        assert isinstance(result, lal.REAL8FrequencySeries)
        assert result.data.length == test_psd.data.length

    def test_polyfit_with_verbose(self, capsys):
        """Test polynomial fitting with verbose output."""
        test_psd = create_test_psd(f0=0.0, deltaF=1.0, length=2049)

        result = psd_module.polyfit(
            test_psd, f_low=50.0, f_high=500.0, order=3, verbose=True
        )

        assert isinstance(result, lal.REAL8FrequencySeries)
        captured = capsys.readouterr()
        assert "Fit polynomial" in captured.err

    def test_polyfit_different_orders(self):
        """Test polynomial fitting with different orders."""
        test_psd = create_test_psd(f0=0.0, deltaF=1.0, length=2049)

        for order in [2, 4, 6]:
            result = psd_module.polyfit(test_psd, f_low=50.0, f_high=500.0, order=order)
            assert isinstance(result, lal.REAL8FrequencySeries)


class TestHarmonicMean:
    """Tests for harmonic_mean function."""

    def test_harmonic_mean_two_psds(self):
        """Test harmonic mean of two PSDs."""
        psd_dict = {
            "H1": create_test_psd(name="H1_psd"),
            "L1": create_test_psd(name="L1_psd"),
        }

        result = psd_module.harmonic_mean(psd_dict)

        assert isinstance(result, lal.REAL8FrequencySeries)
        assert result.data.length == psd_dict["H1"].data.length

    def test_harmonic_mean_three_psds(self):
        """Test harmonic mean of three PSDs."""
        psd_dict = {
            "H1": create_test_psd(name="H1_psd"),
            "L1": create_test_psd(name="L1_psd"),
            "V1": create_test_psd(name="V1_psd"),
        }

        result = psd_module.harmonic_mean(psd_dict)

        assert isinstance(result, lal.REAL8FrequencySeries)

    def test_harmonic_mean_single_psd(self):
        """Test harmonic mean of single PSD returns same values."""
        psd_dict = {"H1": create_test_psd(name="H1_psd")}

        result = psd_module.harmonic_mean(psd_dict)

        # Harmonic mean of single value should be that value
        numpy.testing.assert_array_almost_equal(
            result.data.data, psd_dict["H1"].data.data, decimal=10
        )


class TestHorizonDistance:
    """Tests for HorizonDistance class."""

    def test_horizon_distance_init(self):
        """Test HorizonDistance initialization."""
        hd = psd_module.HorizonDistance(
            f_min=10.0, f_max=1024.0, delta_f=1.0 / 32.0, m1=1.4, m2=1.4
        )

        assert hd.f_min == 10.0
        assert hd.f_max == 1024.0
        assert hd.m1 == 1.4
        assert hd.m2 == 1.4
        assert hd.model is not None

    def test_horizon_distance_call(self):
        """Test computing horizon distance."""
        hd = psd_module.HorizonDistance(
            f_min=10.0, f_max=1024.0, delta_f=1.0 / 32.0, m1=1.4, m2=1.4
        )

        # Create a test PSD matching the HorizonDistance parameters
        test_psd = lal.CreateREAL8FrequencySeries(
            "psd",
            LIGOTimeGPS(0),
            0.0,
            1.0 / 32.0,
            lal.Unit("strain^2 s"),
            hd.model.data.length,
        )
        lalsimulation.SimNoisePSDaLIGODesignSensitivityP1200087(test_psd, 0.0)

        D, (f, model) = hd(test_psd)

        assert D > 0  # Distance should be positive
        assert len(f) == len(model)
        assert len(f) > 0

    def test_horizon_distance_custom_snr(self):
        """Test computing horizon distance with custom SNR."""
        hd = psd_module.HorizonDistance(
            f_min=10.0, f_max=1024.0, delta_f=1.0 / 32.0, m1=1.4, m2=1.4
        )

        test_psd = lal.CreateREAL8FrequencySeries(
            "psd",
            LIGOTimeGPS(0),
            0.0,
            1.0 / 32.0,
            lal.Unit("strain^2 s"),
            hd.model.data.length,
        )
        lalsimulation.SimNoisePSDaLIGODesignSensitivityP1200087(test_psd, 0.0)

        D_8, _ = hd(test_psd, snr=8.0)
        D_25, _ = hd(test_psd, snr=25.0)

        # Higher SNR should give smaller distance
        assert D_25 < D_8


class TestEffectiveDistanceFactor:
    """Tests for effective_distance_factor function."""

    def test_effective_distance_factor_face_on(self):
        """Test effective distance factor for face-on source."""
        # Face-on: inclination = 0
        factor = psd_module.effective_distance_factor(inclination=0.0, fp=1.0, fc=0.0)
        assert factor > 0

    def test_effective_distance_factor_edge_on(self):
        """Test effective distance factor for edge-on source."""
        # Edge-on: inclination = pi/2
        factor = psd_module.effective_distance_factor(
            inclination=math.pi / 2, fp=1.0, fc=0.0
        )
        assert factor > 0

    def test_effective_distance_factor_with_cross_polarization(self):
        """Test effective distance factor with cross polarization."""
        factor = psd_module.effective_distance_factor(
            inclination=math.pi / 4, fp=0.5, fc=0.5
        )
        assert factor > 0

    def test_effective_distance_factor_optimal_orientation(self):
        """Test that optimal orientation gives smallest factor."""
        # Face-on with pure plus polarization is optimal
        factor_optimal = psd_module.effective_distance_factor(
            inclination=0.0, fp=1.0, fc=0.0
        )
        factor_suboptimal = psd_module.effective_distance_factor(
            inclination=math.pi / 2, fp=1.0, fc=0.0
        )

        # Optimal should have smaller effective distance factor
        assert factor_optimal < factor_suboptimal

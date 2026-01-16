"""Tests for sgnl.kernels"""

from unittest import mock

import lal
import numpy
import pytest
from lal import LIGOTimeGPS

from sgnl import kernels


def create_mock_psd(
    f0=0.0,
    deltaF=1.0,
    length=513,
    flat_value=1e-46,
):
    """Create a mock PSD for testing."""
    psd = lal.CreateREAL8FrequencySeries(
        name="mock_psd",
        epoch=LIGOTimeGPS(0),
        f0=f0,
        deltaF=deltaF,
        sampleUnits=lal.Unit("strain^2 s"),
        length=length,
    )
    # Create a simple flat PSD (white noise)
    psd.data.data[:] = flat_value
    # Set DC and Nyquist to 0 to avoid division issues
    psd.data.data[0] = 0.0
    return psd


class TestPSDFirKernel:
    """Tests for PSDFirKernel class."""

    def test_init_default(self):
        """Test default initialization."""
        kernel = kernels.PSDFirKernel()
        assert kernel.revplan is None
        assert kernel.fwdplan is None
        assert kernel.target_phase is None
        assert kernel.target_phase_mask is None

    def test_psd_to_linear_phase_whitening_fir_kernel_basic(self):
        """Test basic linear phase kernel generation."""
        psd = create_mock_psd(f0=0.0, deltaF=1.0, length=513)

        psd_kernel = kernels.PSDFirKernel()
        kernel, latency, sample_rate = (
            psd_kernel.psd_to_linear_phase_whitening_fir_kernel(psd)
        )

        # Should return valid values
        assert isinstance(kernel, numpy.ndarray)
        assert latency > 0
        assert sample_rate > 0
        # Plan should be cached
        assert psd_kernel.revplan is not None

    def test_psd_to_linear_phase_whitening_fir_kernel_with_nyquist(self):
        """Test linear phase kernel with custom Nyquist frequency."""
        psd = create_mock_psd(f0=0.0, deltaF=1.0, length=513)

        psd_kernel = kernels.PSDFirKernel()
        kernel, latency, sample_rate = (
            psd_kernel.psd_to_linear_phase_whitening_fir_kernel(psd, nyquist=256.0)
        )

        assert isinstance(kernel, numpy.ndarray)
        assert sample_rate > 0

    def test_psd_to_linear_phase_whitening_fir_kernel_no_invert(self):
        """Test linear phase kernel without inversion."""
        psd = create_mock_psd(f0=0.0, deltaF=1.0, length=513)

        psd_kernel = kernels.PSDFirKernel()
        kernel, latency, sample_rate = (
            psd_kernel.psd_to_linear_phase_whitening_fir_kernel(psd, invert=False)
        )

        assert isinstance(kernel, numpy.ndarray)

    def test_psd_to_linear_phase_whitening_fir_kernel_nonzero_f0_raises(self):
        """Test that non-zero f0 raises assertion."""
        psd = create_mock_psd(f0=10.0, deltaF=1.0, length=513)

        psd_kernel = kernels.PSDFirKernel()
        with pytest.raises(AssertionError):
            psd_kernel.psd_to_linear_phase_whitening_fir_kernel(psd)

    def test_linear_phase_to_minimum_phase_kernel(self):
        """Test conversion from linear to minimum phase kernel."""
        psd = create_mock_psd(f0=0.0, deltaF=1.0, length=513)

        psd_kernel = kernels.PSDFirKernel()
        linear_kernel, latency, sample_rate = (
            psd_kernel.psd_to_linear_phase_whitening_fir_kernel(psd)
        )

        min_phase_kernel, phase = (
            psd_kernel.linear_phase_fir_kernel_to_minimum_phase_whitening_fir_kernel(
                linear_kernel, sample_rate
            )
        )

        assert isinstance(min_phase_kernel, numpy.ndarray)
        assert isinstance(phase, numpy.ndarray)
        assert len(min_phase_kernel) == len(linear_kernel)
        # Plans should be cached
        assert psd_kernel.fwdplan is not None
        assert psd_kernel.revplan is not None

    def test_linear_phase_to_minimum_phase_creates_plans(self):
        """Test that plans are created when not present."""
        psd = create_mock_psd(f0=0.0, deltaF=1.0, length=513)

        psd_kernel = kernels.PSDFirKernel()
        linear_kernel, latency, sample_rate = (
            psd_kernel.psd_to_linear_phase_whitening_fir_kernel(psd)
        )

        # Clear plans
        psd_kernel.fwdplan = None
        psd_kernel.revplan = None

        min_phase_kernel, phase = (
            psd_kernel.linear_phase_fir_kernel_to_minimum_phase_whitening_fir_kernel(
                linear_kernel, sample_rate
            )
        )

        assert psd_kernel.fwdplan is not None
        assert psd_kernel.revplan is not None

    def test_linear_phase_to_minimum_phase_with_target_phase(self):
        """Test minimum phase kernel with target phase adjustment."""
        psd = create_mock_psd(f0=0.0, deltaF=1.0, length=513)

        psd_kernel = kernels.PSDFirKernel()
        linear_kernel, latency, sample_rate = (
            psd_kernel.psd_to_linear_phase_whitening_fir_kernel(psd)
        )

        # Set target phase
        working_length = len(linear_kernel)
        psd_kernel.target_phase = numpy.zeros(working_length // 2 + 1)
        psd_kernel.target_phase_mask = numpy.ones(working_length // 2 + 1)

        min_phase_kernel, phase = (
            psd_kernel.linear_phase_fir_kernel_to_minimum_phase_whitening_fir_kernel(
                linear_kernel, sample_rate
            )
        )

        assert isinstance(min_phase_kernel, numpy.ndarray)

    def test_set_phase(self):
        """Test set_phase method."""
        psd = create_mock_psd(f0=0.0, deltaF=1.0, length=513)

        psd_kernel = kernels.PSDFirKernel()

        # Create frequency and model arrays that match the PSD frequency range
        f_model = numpy.arange(10.0, 500.0, 1.0)  # 490 elements
        model = numpy.ones_like(f_model) * 1e-46

        mock_horizon = mock.MagicMock()
        mock_horizon.return_value = (100.0, (f_model, model))

        with mock.patch.object(kernels, "HorizonDistance", return_value=mock_horizon):
            psd_kernel.set_phase(psd, f_low=10.0, m1=1.4, m2=1.4)

        assert psd_kernel.target_phase is not None
        assert psd_kernel.target_phase_mask is not None


class TestFirWhitenerKernel:
    """Tests for fir_whitener_kernel function."""

    def test_fir_whitener_kernel_basic(self):
        """Test basic FIR whitener kernel creation."""
        psd = create_mock_psd(f0=0.0, deltaF=1.0, length=513)

        length = 1024
        duration = 1.0
        sample_rate = 1024

        result = kernels.fir_whitener_kernel(length, duration, sample_rate, psd)

        assert isinstance(result, lal.COMPLEX16FrequencySeries)
        assert len(result.data.data) == length

    def test_fir_whitener_kernel_shorter_kernel(self):
        """Test FIR whitener kernel when kernel needs padding."""
        psd = create_mock_psd(f0=0.0, deltaF=1.0, length=513)

        # Use a larger length that will require padding
        length = 2048
        duration = 2.0
        sample_rate = 1024

        result = kernels.fir_whitener_kernel(length, duration, sample_rate, psd)

        assert len(result.data.data) == length

    def test_fir_whitener_kernel_longer_kernel(self):
        """Test FIR whitener kernel when kernel needs truncation."""
        psd = create_mock_psd(f0=0.0, deltaF=0.5, length=1025)

        # Use a shorter length that will require truncation
        length = 512
        duration = 0.5
        sample_rate = 1024

        result = kernels.fir_whitener_kernel(length, duration, sample_rate, psd)

        assert len(result.data.data) == length

    def test_fir_whitener_kernel_no_psd_raises(self):
        """Test that missing PSD raises assertion."""
        with pytest.raises(AssertionError):
            kernels.fir_whitener_kernel(1024, 1.0, 1024, None)

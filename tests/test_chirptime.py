"""Tests for sgnl.chirptime"""

import warnings
from unittest import mock

import numpy
import pytest

from sgnl import chirptime


class TestTmplttime:
    """Tests for tmplttime function.

    Note: The tmplttime function uses an older LAL API that may not be
    compatible with current LAL versions. These tests mock the LAL
    functions to test the logic.
    """

    def test_tmplttime_with_zero_in_waveform(self):
        """Test waveform time when zero is found in waveform."""
        # Create mock waveform data where there's a zero at position 100
        mock_hp = mock.MagicMock()
        mock_hp.data.data = numpy.concatenate([numpy.ones(100), numpy.zeros(50)])
        mock_hp.deltaT = 1.0 / 16384.0

        mock_hc = mock.MagicMock()
        mock_hc.data.data = numpy.zeros(150)

        with mock.patch(
            "lalsimulation.SimInspiralChooseTDWaveform",
            return_value=(mock_hp, mock_hc),
        ):
            time = chirptime.tmplttime(20.0, 1e31, 1e31, 0.0, 0.0, "TaylorT4")

            # Time should be 100 samples * deltaT
            expected_time = 100 * (1.0 / 16384.0)
            assert time == pytest.approx(expected_time)

    def test_tmplttime_no_zero_in_waveform(self):
        """Test waveform time when no zero is found (ValueError fallback)."""
        # Create mock waveform data with no zeros
        mock_hp = mock.MagicMock()
        mock_hp.data.data = numpy.ones(200)
        mock_hp.deltaT = 1.0 / 16384.0

        mock_hc = mock.MagicMock()
        mock_hc.data.data = numpy.ones(200)

        with mock.patch(
            "lalsimulation.SimInspiralChooseTDWaveform",
            return_value=(mock_hp, mock_hc),
        ):
            time = chirptime.tmplttime(20.0, 1e31, 1e31, 0.0, 0.0, "TaylorT4")

            # Time should be full length * deltaT
            expected_time = 200 * (1.0 / 16384.0)
            assert time == pytest.approx(expected_time)


class TestVelocity:
    """Tests for velocity function."""

    def test_velocity_basic(self):
        """Test basic orbital velocity calculation."""
        f = 100.0  # Hz
        M = 60.0 * chirptime.gsun  # 60 solar masses in kg

        v = chirptime.velocity(f, M)

        # Velocity should be a fraction of c
        assert 0 < v < chirptime.c


class TestChirptime:
    """Tests for chirptime function."""

    def test_chirptime_basic(self):
        """Test basic chirp time calculation."""
        f = 20.0  # Hz
        M = 60.0 * chirptime.gsun  # 60 solar masses in kg
        nu = 0.25  # Symmetric mass ratio for equal masses
        chi = 0.0

        t = chirptime.chirptime(f, M, nu, chi)

        # Chirp time should be positive
        assert t > 0
        # For 30+30 Msun from 20Hz, should be order of seconds
        assert t < 100

    def test_chirptime_with_spin(self):
        """Test chirp time with spin parameter."""
        f = 20.0
        M = 60.0 * chirptime.gsun
        nu = 0.25
        chi = 0.5

        t = chirptime.chirptime(f, M, nu, chi)

        assert t > 0


class TestRingf:
    """Tests for ringf function."""

    def test_ringf_basic(self):
        """Test basic ringdown frequency calculation."""
        M = 60.0 * chirptime.gsun  # 60 solar masses
        j = 0.7  # Spin parameter

        f = chirptime.ringf(M, j)

        # Ringdown frequency should be positive
        assert f > 0
        # For 60 Msun, should be order of hundreds of Hz
        assert f < 1000


class TestRingtime:
    """Tests for ringtime function."""

    def test_ringtime_basic(self):
        """Test basic ringdown time calculation."""
        M = 60.0 * chirptime.gsun
        j = 0.7

        t = chirptime.ringtime(M, j)

        # Ringdown time should be positive
        assert t > 0
        # Should be order of milliseconds for stellar mass BH
        assert t < 1.0


class TestMergetime:
    """Tests for mergetime function."""

    def test_mergetime_basic(self):
        """Test basic merge time calculation."""
        M = 60.0 * chirptime.gsun

        t = chirptime.mergetime(M)

        # Merge time should be positive
        assert t > 0
        # Should be order of milliseconds for stellar mass BH
        assert t < 0.1


class TestOverestimateJFromChi:
    """Tests for overestimate_j_from_chi function."""

    def test_zero_spin(self):
        """Test with zero spin."""
        j = chirptime.overestimate_j_from_chi(0.0)

        # Result should be positive (final BH always has some spin from merger)
        assert j >= 0
        assert j <= 0.998

    def test_high_spin(self):
        """Test with high spin."""
        j = chirptime.overestimate_j_from_chi(0.9)

        # Should be capped at 0.998
        assert j <= 0.998
        assert j >= 0.9  # Should be at least the input spin


class TestImrTime:
    """Tests for imr_time function."""

    def test_imr_time_basic(self):
        """Test basic IMR time calculation."""
        f = 20.0  # Hz
        m1 = 30.0 * chirptime.gsun  # kg
        m2 = 30.0 * chirptime.gsun
        j1 = 0.0
        j2 = 0.0

        t = chirptime.imr_time(f, m1, m2, j1, j2)

        # Total time should be positive
        assert t > 0

    def test_imr_time_with_f_max(self):
        """Test IMR time with f_max specified."""
        f = 20.0
        m1 = 30.0 * chirptime.gsun
        m2 = 30.0 * chirptime.gsun
        j1 = 0.0
        j2 = 0.0
        f_max = 100.0

        t = chirptime.imr_time(f, m1, m2, j1, j2, f_max=f_max)

        # Time should be positive
        assert t > 0

    def test_imr_time_f_max_less_than_f_raises(self):
        """Test that f_max < f raises ValueError."""
        f = 50.0
        m1 = 30.0 * chirptime.gsun
        m2 = 30.0 * chirptime.gsun
        j1 = 0.0
        j2 = 0.0
        f_max = 20.0  # Less than f

        with pytest.raises(ValueError) as exc_info:
            chirptime.imr_time(f, m1, m2, j1, j2, f_max=f_max)

        assert "f_max must either be None or greater than f" in str(exc_info.value)

    def test_imr_time_warns_when_f_above_ringdown(self):
        """Test that warning is issued when f > ringdown frequency."""
        # Use high starting frequency that exceeds ringdown
        m1 = 100.0 * chirptime.gsun  # High mass for lower ringdown freq
        m2 = 100.0 * chirptime.gsun
        j1 = 0.0
        j2 = 0.0

        # Calculate ringdown frequency to set f above it
        M = m1 + m2
        j = chirptime.overestimate_j_from_chi(max(j1, j2))
        ringdown_f = chirptime.ringf(M, j)

        # Set f above ringdown frequency
        f = ringdown_f + 50.0

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            chirptime.imr_time(f, m1, m2, j1, j2)

            # Check warning was issued
            assert len(w) == 1
            assert "ringdown frequency" in str(w[0].message)

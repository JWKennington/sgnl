"""Tests for sgnl.templates module."""

from unittest import mock

import lal
import numpy as np
import pytest

from sgnl import templates


class TestGlobalVariables:
    """Tests for global variables in templates module."""

    def test_sgnl_fd_approximants_exists(self):
        """Test that sgnl_FD_approximants is defined."""
        assert isinstance(templates.sgnl_FD_approximants, set)
        assert "TaylorF2" in templates.sgnl_FD_approximants

    def test_sgnl_td_approximants_exists(self):
        """Test that sgnl_TD_approximants is defined."""
        assert isinstance(templates.sgnl_TD_approximants, set)
        assert "TaylorT4" in templates.sgnl_TD_approximants

    def test_sgnl_imr_approximants_exists(self):
        """Test that sgnl_IMR_approximants is defined."""
        assert isinstance(templates.sgnl_IMR_approximants, set)
        assert "IMRPhenomC" in templates.sgnl_IMR_approximants

    def test_sgnl_approximants_is_union(self):
        """Test that sgnl_approximants is union of FD and TD."""
        assert (
            templates.sgnl_approximants
            == templates.sgnl_FD_approximants | templates.sgnl_TD_approximants
        )

    def test_sgnl_imr_subset_of_approximants(self):
        """Test that IMR approximants is subset of all approximants."""
        assert templates.sgnl_IMR_approximants <= templates.sgnl_approximants


class TestSgnlValidApproximant:
    """Tests for sgnl_valid_approximant function."""

    def test_valid_approximant_taylorf2(self):
        """Test valid approximant TaylorF2."""
        # Should not raise
        templates.sgnl_valid_approximant("TaylorF2")

    def test_valid_approximant_imrphenomc(self):
        """Test valid approximant IMRPhenomC."""
        templates.sgnl_valid_approximant("IMRPhenomC")

    def test_invalid_lalsim_approximant(self):
        """Test approximant not supported by lalsimulation."""
        with pytest.raises(ValueError, match="not supported by lalsimulation"):
            templates.sgnl_valid_approximant("InvalidApproximant123")

    def test_lalsim_supported_but_not_sgnl(self):
        """Test approximant supported by lalsim but not sgnl."""
        # SpinTaylorT5 is supported by lalsim but not by sgnl
        with pytest.raises(ValueError, match="not currently supported by sgnl"):
            templates.sgnl_valid_approximant("SpinTaylorT5")


class TestQuadraturePhase:
    """Tests for QuadraturePhase dataclass."""

    def test_quadrature_phase_creation(self):
        """Test QuadraturePhase dataclass creation."""
        qp = templates.QuadraturePhase(n=1024)
        assert qp.n == 1024

    def test_add_quadrature_phase_even_n(self):
        """Test add_quadrature_phase with even n (has Nyquist)."""
        n = 8  # even number
        length = n // 2 + 1
        fseries = lal.CreateCOMPLEX16FrequencySeries(
            name="test",
            epoch=lal.LIGOTimeGPS(0),
            f0=0.0,
            deltaF=1.0,
            sampleUnits=lal.DimensionlessUnit,
            length=length,
        )
        fseries.data.data = np.array([1 + 0j, 2 + 1j, 3 + 2j, 4 + 3j, 5 + 0j])

        result = templates.QuadraturePhase.add_quadrature_phase(fseries, n)

        assert result is not None
        assert isinstance(result, lal.COMPLEX16FrequencySeries)
        # For even n, positive_frequencies exclude Nyquist (length - 1)
        # Output length = len(zeros) + len(positive_frequencies[1:]) - 1
        # = (length - 1) + (length - 1 - 1) - 1 = n - 1
        assert len(result.data.data) == n

    def test_add_quadrature_phase_odd_n(self):
        """Test add_quadrature_phase with odd n (no Nyquist)."""
        n = 7  # odd number
        length = (n + 1) // 2
        fseries = lal.CreateCOMPLEX16FrequencySeries(
            name="test",
            epoch=lal.LIGOTimeGPS(0),
            f0=0.0,
            deltaF=1.0,
            sampleUnits=lal.DimensionlessUnit,
            length=length,
        )
        fseries.data.data = np.array([1 + 0j, 2 + 1j, 3 + 2j, 4 + 3j])

        result = templates.QuadraturePhase.add_quadrature_phase(fseries, n)

        assert result is not None
        assert isinstance(result, lal.COMPLEX16FrequencySeries)
        # DC is set to 0
        assert result.data.data[0] == 0


class TestNormalizedAutocorrelation:
    """Tests for normalized_autocorrelation function."""

    def test_normalized_autocorrelation_basic(self):
        """Test normalized_autocorrelation function."""
        length = 8
        fseries = lal.CreateCOMPLEX16FrequencySeries(
            name="test",
            epoch=lal.LIGOTimeGPS(0),
            f0=0.0,
            deltaF=1.0,
            sampleUnits=lal.DimensionlessUnit,
            length=length,
        )
        fseries.data.data = np.array(
            [1 + 0j, 2 + 1j, 3 + 2j, 4 + 3j, 5 + 0j, 4 - 3j, 3 - 2j, 2 - 1j]
        )

        # Create reverse FFT plan
        revplan = lal.CreateReverseCOMPLEX16FFTPlan(length, 0)

        result = templates.normalized_autocorrelation(fseries, revplan)

        assert result is not None
        assert isinstance(result, lal.COMPLEX16TimeSeries)
        # First element should be 1.0 (normalized)
        assert np.isclose(result.data.data[0], 1.0)


class TestCeilPow2:
    """Tests for ceil_pow_2 function."""

    def test_ceil_pow_2_exact_power(self):
        """Test ceil_pow_2 with exact power of 2."""
        assert templates.ceil_pow_2(8) == 8
        assert templates.ceil_pow_2(16) == 16
        assert templates.ceil_pow_2(1024) == 1024

    def test_ceil_pow_2_non_power(self):
        """Test ceil_pow_2 with non-power of 2."""
        assert templates.ceil_pow_2(5) == 8
        assert templates.ceil_pow_2(9) == 16
        assert templates.ceil_pow_2(100) == 128
        assert templates.ceil_pow_2(1000) == 1024

    def test_ceil_pow_2_float(self):
        """Test ceil_pow_2 with float input."""
        assert templates.ceil_pow_2(5.5) == 8
        assert templates.ceil_pow_2(8.1) == 16

    def test_ceil_pow_2_small_values(self):
        """Test ceil_pow_2 with small values."""
        assert templates.ceil_pow_2(1) == 1
        assert templates.ceil_pow_2(2) == 2
        assert templates.ceil_pow_2(3) == 4


class TestTimeSlices:
    """Tests for time_slices function."""

    def _create_mock_row(self, mass1=1.4, mass2=1.4, spin1z=0.0, spin2z=0.0):
        """Helper to create a mock sngl_inspiral row."""
        row = mock.MagicMock()
        row.mass1 = mass1
        row.mass2 = mass2
        row.spin1z = spin1z
        row.spin2z = spin2z
        return row

    def test_time_slices_basic(self):
        """Test time_slices with basic input."""
        rows = [self._create_mock_row()]

        with mock.patch("sgnl.templates.chirptime.imr_time", return_value=1.0):
            result = templates.time_slices(rows, flow=40, fhigh=900)

        assert result is not None
        assert isinstance(result, np.ndarray)
        assert result.dtype.names == ("rate", "begin", "end")

    def test_time_slices_with_sample_rate(self):
        """Test time_slices with explicit sample_rate."""
        rows = [self._create_mock_row()]

        with mock.patch("sgnl.templates.chirptime.imr_time", return_value=1.0):
            result = templates.time_slices(rows, flow=40, fhigh=900, sample_rate=4096)

        assert result is not None
        # With sample_rate specified, should use that as max

    def test_time_slices_verbose(self, capsys):
        """Test time_slices with verbose output."""
        rows = [self._create_mock_row()]

        with mock.patch("sgnl.templates.chirptime.imr_time", return_value=1.0):
            templates.time_slices(rows, flow=40, fhigh=900, verbose=True)

        captured = capsys.readouterr()
        assert "Time freq boundaries" in captured.err

    def test_time_slices_too_many_templates_raises(self):
        """Test time_slices raises when too many templates."""
        # Create a lot of rows - more than segment_samples_max
        rows = [self._create_mock_row() for _ in range(5000)]

        with mock.patch("sgnl.templates.chirptime.imr_time", return_value=1.0):
            with pytest.raises(ValueError, match="input template bank must have fewer"):
                templates.time_slices(rows, flow=40, fhigh=900)

    def test_time_slices_rate_256_plus(self):
        """Test time_slices with rate >= 256."""
        rows = [self._create_mock_row()]

        # Return longer chirp time to force multiple blocks at higher rate
        with mock.patch("sgnl.templates.chirptime.imr_time", return_value=5.0):
            result = templates.time_slices(rows, flow=40, fhigh=900)

        assert result is not None
        # Should have some slices at high sample rate
        assert len(result) > 0

    def test_time_slices_rate_64_to_255(self):
        """Test time_slices with 64 <= rate < 256."""
        rows = [self._create_mock_row()]

        # Return very long chirp time to force lower rates
        with mock.patch("sgnl.templates.chirptime.imr_time", return_value=100.0):
            result = templates.time_slices(
                rows, flow=10, fhigh=100, samples_max_64=2048
            )

        assert result is not None

    def test_time_slices_rate_below_64(self):
        """Test time_slices with rate < 64."""
        rows = [self._create_mock_row()]

        # Very long chirp time with very low flow to force low rates
        with mock.patch("sgnl.templates.chirptime.imr_time", return_value=500.0):
            result = templates.time_slices(rows, flow=2, fhigh=32, samples_max=4096)

        assert result is not None

    def test_time_slices_small_blocks_binary_decomposition(self):
        """Test time_slices with varying small block counts."""
        rows = [self._create_mock_row()]

        # Return chirp times that will create multiple small blocks
        # The function uses binary decomposition of number_small_blocks
        with mock.patch("sgnl.templates.chirptime.imr_time", return_value=2.5):
            result = templates.time_slices(
                rows, flow=40, fhigh=900, samples_min=512, samples_max_256=2048
            )

        assert result is not None

    def test_time_slices_large_blocks(self):
        """Test time_slices with large blocks."""
        rows = [self._create_mock_row()]

        # Chirp time that creates large blocks
        with mock.patch("sgnl.templates.chirptime.imr_time", return_value=10.0):
            result = templates.time_slices(
                rows, flow=40, fhigh=900, samples_min=256, samples_max_256=512
            )

        assert result is not None
        assert len(result) > 0

    def test_time_slices_skips_rate_when_longest_chirp_less_than_accum(self):
        """Test time_slices skips a rate when chirp time already covered."""
        rows = [self._create_mock_row()]

        # Different chirp times for different flows will cause some rates to be skipped
        def varying_imr_time(flow, m1, m2, s1, s2):
            # Return 0 for high frequencies (short chirp), more for low
            if flow > 100:
                return 0.1
            return 0.5

        with mock.patch(
            "sgnl.templates.chirptime.imr_time", side_effect=varying_imr_time
        ):
            result = templates.time_slices(rows, flow=40, fhigh=500)

        assert result is not None

    def test_time_slices_multiple_rows(self):
        """Test time_slices with multiple template rows."""
        rows = [
            self._create_mock_row(mass1=1.4, mass2=1.4),
            self._create_mock_row(mass1=10.0, mass2=1.4),
            self._create_mock_row(mass1=30.0, mass2=30.0),
        ]

        # Different rows will have different chirp times
        def mass_dependent_imr_time(flow, m1, m2, s1, s2):
            total_mass = m1 + m2
            # Heavier systems have shorter chirp times
            return 100.0 / (total_mass / (2 * 1.4 * lal.MSUN_SI))

        with mock.patch(
            "sgnl.templates.chirptime.imr_time", side_effect=mass_dependent_imr_time
        ):
            result = templates.time_slices(rows, flow=40, fhigh=900)

        assert result is not None

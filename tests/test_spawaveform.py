"""Tests for sgnl.spawaveform module."""

import pytest

from sgnl import spawaveform


class TestSchwarzIsco:
    """Tests for schwarz_isco function."""

    def test_schwarz_isco_equal_masses(self):
        """Test schwarz_isco with equal masses."""
        result = spawaveform.schwarz_isco(1.4, 1.4)
        assert result > 0
        assert isinstance(result, float)

    def test_schwarz_isco_unequal_masses(self):
        """Test schwarz_isco with unequal masses."""
        result = spawaveform.schwarz_isco(10.0, 1.4)
        assert result > 0


class TestBklIsco:
    """Tests for bkl_isco function."""

    def test_bkl_isco_m1_less_than_m2(self):
        """Test bkl_isco when m1 < m2."""
        result = spawaveform.bkl_isco(1.4, 10.0)
        assert result > 0

    def test_bkl_isco_m1_greater_than_m2(self):
        """Test bkl_isco when m1 > m2."""
        result = spawaveform.bkl_isco(10.0, 1.4)
        assert result > 0

    def test_bkl_isco_equal_masses(self):
        """Test bkl_isco with equal masses."""
        result = spawaveform.bkl_isco(1.4, 1.4)
        assert result > 0


class TestLightRing:
    """Tests for light_ring function."""

    def test_light_ring_basic(self):
        """Test light_ring function."""
        result = spawaveform.light_ring(1.4, 1.4, chi=0.0)
        assert result > 0
        assert isinstance(result, float)

    def test_light_ring_unequal_masses(self):
        """Test light_ring with unequal masses."""
        result = spawaveform.light_ring(10.0, 1.4, chi=0.5)
        assert result > 0


class TestFfinal:
    """Tests for ffinal function."""

    def test_ffinal_schwarz_isco(self):
        """Test ffinal with schwarz_isco option."""
        result = spawaveform.ffinal(1.4, 1.4, s="schwarz_isco")
        expected = spawaveform.schwarz_isco(1.4, 1.4)
        assert result == expected

    def test_ffinal_none_defaults_to_schwarz(self):
        """Test ffinal with s=None defaults to schwarz_isco."""
        result = spawaveform.ffinal(1.4, 1.4, s=None)
        expected = spawaveform.schwarz_isco(1.4, 1.4)
        assert result == expected

    def test_ffinal_bkl_isco(self):
        """Test ffinal with bkl_isco option."""
        result = spawaveform.ffinal(1.4, 1.4, s="bkl_isco")
        expected = spawaveform.bkl_isco(1.4, 1.4)
        assert result == expected

    def test_ffinal_light_ring(self):
        """Test ffinal with light_ring option."""
        result = spawaveform.ffinal(1.4, 1.4, s="light_ring")
        expected = spawaveform.light_ring(1.4, 1.4, None)
        assert result == expected

    def test_ffinal_invalid_option(self):
        """Test ffinal with invalid option raises ValueError."""
        with pytest.raises(ValueError, match="Unrecognized ending frequency"):
            spawaveform.ffinal(1.4, 1.4, s="invalid_option")


class TestChirpTime:
    """Tests for chirp_time function."""

    def test_chirp_time_order_7(self):
        """Test chirp_time with order 7."""
        result = spawaveform.chirp_time(1.4, 1.4, 30.0, 7, 0.0)
        assert result > 0

    def test_chirp_time_order_8(self):
        """Test chirp_time with order 8."""
        result = spawaveform.chirp_time(1.4, 1.4, 30.0, 8, 0.0)
        assert result > 0

    def test_chirp_time_order_6(self):
        """Test chirp_time with order 6."""
        result = spawaveform.chirp_time(1.4, 1.4, 30.0, 6, 0.0)
        assert result > 0

    def test_chirp_time_order_5(self):
        """Test chirp_time with order 5."""
        result = spawaveform.chirp_time(1.4, 1.4, 30.0, 5, 0.0)
        assert result > 0

    def test_chirp_time_order_4(self):
        """Test chirp_time with order 4."""
        result = spawaveform.chirp_time(1.4, 1.4, 30.0, 4, 0.0)
        assert result > 0

    def test_chirp_time_with_chi(self):
        """Test chirp_time with non-zero chi."""
        result = spawaveform.chirp_time(10.0, 1.4, 30.0, 7, 0.5)
        assert result > 0

    def test_chirp_time_invalid_order(self, capsys):
        """Test chirp_time with invalid order prints error."""
        # Order 3 is invalid (not in [4,5,6,7,8])
        spawaveform.chirp_time(1.4, 1.4, 30.0, 3, 0.0)
        captured = capsys.readouterr()
        assert "ERROR!!!" in captured.err


class TestChirpTimeBetweenF1AndF2:
    """Tests for chirp_time_between_f1_and_f2 function."""

    def test_chirp_time_between_f1_and_f2(self):
        """Test chirp_time_between_f1_and_f2."""
        result = spawaveform.chirp_time_between_f1_and_f2(1.4, 1.4, 30.0, 100.0, 7, 0.0)
        # Should be positive since chirp time decreases with frequency
        assert result > 0


class TestChirptime:
    """Tests for chirptime function."""

    def test_chirptime_without_ffinal(self):
        """Test chirptime without fFinal."""
        result = spawaveform.chirptime(1.4, 1.4, 7, 30.0, fFinal=0, chi=0.0)
        expected = spawaveform.chirp_time(1.4, 1.4, 30.0, 7, 0.0)
        assert result == expected

    def test_chirptime_with_ffinal(self):
        """Test chirptime with fFinal."""
        result = spawaveform.chirptime(1.4, 1.4, 7, 30.0, fFinal=100.0, chi=0.0)
        expected = spawaveform.chirp_time_between_f1_and_f2(
            1.4, 1.4, 30.0, 100.0, 7, 0.0
        )
        assert result == expected


class TestComputeChi:
    """Tests for compute_chi function."""

    def test_compute_chi_zero_spins(self):
        """Test compute_chi with zero spins."""
        result = spawaveform.compute_chi(1.4, 1.4, 0.0, 0.0)
        assert result == 0.0

    def test_compute_chi_equal_spins(self):
        """Test compute_chi with equal spins."""
        result = spawaveform.compute_chi(1.4, 1.4, 0.5, 0.5)
        assert result == 0.5

    def test_compute_chi_unequal_masses(self):
        """Test compute_chi with unequal masses."""
        result = spawaveform.compute_chi(10.0, 1.4, 0.5, 0.3)
        assert isinstance(result, float)

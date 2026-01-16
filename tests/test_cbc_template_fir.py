"""Tests for sgnl.cbc_template_fir"""

from unittest import mock

import lal
import numpy
import pytest

from sgnl import cbc_template_fir


class TestTukeywindow:
    """Tests for tukeywindow function."""

    def test_basic_window(self):
        """Test basic tukey window generation."""
        data = numpy.zeros(1000)
        window = cbc_template_fir.tukeywindow(data, samps=100)
        assert len(window) == len(data)
        # Tukey window should be 1 in the middle
        assert window[500] == pytest.approx(1.0)

    def test_default_samps(self):
        """Test with default samps parameter."""
        data = numpy.zeros(500)
        window = cbc_template_fir.tukeywindow(data)
        assert len(window) == len(data)

    def test_assertion_too_few_samples(self):
        """Test assertion when data is too short."""
        data = numpy.zeros(100)
        with pytest.raises(AssertionError):
            cbc_template_fir.tukeywindow(data, samps=200)


class TestGenerateTemplate:
    """Tests for generate_template function."""

    def _create_mock_row(self, mass1=30.0, mass2=30.0):
        """Create a mock template bank row."""
        row = mock.MagicMock()
        row.mass1 = mass1
        row.mass2 = mass2
        row.spin1x = 0.0
        row.spin1y = 0.0
        row.spin1z = 0.0
        row.spin2x = 0.0
        row.spin2y = 0.0
        row.spin2z = 0.0
        row.mchirp = (mass1 * mass2) ** 0.6 / (mass1 + mass2) ** 0.2
        return row

    def test_unsupported_approximant(self):
        """Test that unsupported approximant raises ValueError."""
        row = self._create_mock_row()
        with pytest.raises(ValueError) as exc_info:
            cbc_template_fir.generate_template(
                row, "UnsupportedApproximant", 4096, 16.0, 20.0, 1024.0
            )
        assert "Unsupported approximant" in str(exc_info.value)

    def test_generate_template_taylorf2(self):
        """Test template generation with TaylorF2 approximant."""
        row = self._create_mock_row(mass1=30.0, mass2=30.0)

        fseries = cbc_template_fir.generate_template(
            row,
            "TaylorF2",
            sample_rate=4096,
            duration=16.0,
            f_low=20.0,
            f_high=1024.0,
        )

        assert fseries is not None
        # Check it's a LAL frequency series
        assert hasattr(fseries, "data")
        assert hasattr(fseries.data, "data")

    def test_generate_template_with_padding(self):
        """Test template generation when f_high < sample_rate/2."""
        row = self._create_mock_row(mass1=30.0, mass2=30.0)

        # f_high (512) < sample_rate/2 (1024)
        fseries = cbc_template_fir.generate_template(
            row,
            "TaylorF2",
            sample_rate=2048,
            duration=8.0,
            f_low=20.0,
            f_high=512.0,
        )

        assert fseries is not None
        # The output should be padded to sample_rate * duration // 2 + 1
        expected_length = int(round(2048 * 8.0)) // 2 + 1
        assert len(fseries.data.data) == expected_length


class TestConditionImrTemplate:
    """Tests for condition_imr_template function."""

    def test_basic_conditioning(self):
        """Test basic IMR template conditioning."""
        n = 1000
        sample_rate = 4096
        data = numpy.exp(1j * numpy.linspace(0, 10, n))
        epoch_time = -0.1  # 100ms before end

        result, target_index = cbc_template_fir.condition_imr_template(
            "SEOBNRv4_ROM",
            data.copy(),
            epoch_time,
            sample_rate,
            max_ringtime=0.05,
        )

        assert len(result) == n
        assert isinstance(target_index, int)

    def test_assertion_bad_epoch(self):
        """Test assertion when epoch time is invalid."""
        data = numpy.ones(1000, dtype=complex)
        # epoch_time must be < 0 and > -len/sample_rate
        with pytest.raises(AssertionError):
            cbc_template_fir.condition_imr_template(
                "SEOBNRv4_ROM", data, 0.1, 4096, 0.05
            )


class TestConditionEarWarnTemplate:
    """Tests for condition_ear_warn_template function."""

    def test_basic_conditioning(self):
        """Test basic early warning template conditioning."""
        n = 1000
        sample_rate = 4096
        data = numpy.exp(1j * numpy.linspace(0, 10, n))
        epoch_time = -0.1

        result, target_index = cbc_template_fir.condition_ear_warn_template(
            "TaylorF2",
            data.copy(),
            epoch_time,
            sample_rate,
            max_shift_time=0.05,
        )

        assert len(result) == n
        assert isinstance(target_index, int)

    def test_assertion_bad_epoch(self):
        """Test assertion when epoch time is invalid."""
        data = numpy.ones(1000, dtype=complex)
        with pytest.raises(AssertionError):
            cbc_template_fir.condition_ear_warn_template(
                "TaylorF2", data, 0.1, 4096, 0.05
            )


class TestComputeAutocorrelationMask:
    """Tests for compute_autocorrelation_mask function."""

    def test_returns_ones(self):
        """Test that function returns ones matrix."""
        autocorr = numpy.zeros((10, 20))
        mask = cbc_template_fir.compute_autocorrelation_mask(autocorr)
        assert mask.shape == autocorr.shape
        assert numpy.all(mask == 1)


class TestTemplatesWorkspace:
    """Tests for templates_workspace class."""

    def _create_mock_row(self, mass1=30.0, mass2=30.0):
        """Create a mock template bank row."""
        row = mock.MagicMock()
        row.mass1 = mass1
        row.mass2 = mass2
        row.spin1x = 0.0
        row.spin1y = 0.0
        row.spin1z = 0.0
        row.spin2x = 0.0
        row.spin2y = 0.0
        row.spin2z = 0.0
        row.mchirp = (mass1 * mass2) ** 0.6 / (mass1 + mass2) ** 0.2
        return row

    def _create_mock_psd(self, length=8193, delta_f=0.125):
        """Create a mock PSD."""
        psd = lal.CreateREAL8FrequencySeries(
            "psd",
            lal.LIGOTimeGPS(0),
            0.0,
            delta_f,
            lal.Unit("s strain^2"),
            length,
        )
        # Fill with simple values (avoid zeros to prevent division issues)
        psd.data.data[:] = 1e-46
        return psd

    def _create_time_slices(self):
        """Create time slices structured array."""
        time_slices = numpy.array(
            [(4096, 0.0, 4.0)],
            dtype=[("rate", float), ("begin", float), ("end", float)],
        )
        return time_slices

    def test_empty_template_table_raises(self):
        """Test that empty template table raises ValueError."""
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with pytest.raises(ValueError) as exc_info:
            cbc_template_fir.templates_workspace(
                template_table=[],
                approximant="TaylorF2",
                psd=psd,
                f_low=20.0,
                time_slices=time_slices,
                autocorrelation_length=None,
                fhigh=1024.0,
            )
        assert "template list is empty" in str(exc_info.value)

    def test_negative_f_low_raises(self):
        """Test that negative f_low raises ValueError."""
        row = self._create_mock_row()
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with pytest.raises(ValueError) as exc_info:
            cbc_template_fir.templates_workspace(
                template_table=[row],
                approximant="TaylorF2",
                psd=psd,
                f_low=-10.0,
                time_slices=time_slices,
                autocorrelation_length=None,
                fhigh=1024.0,
            )
        assert "f_low must be >= 0" in str(exc_info.value)

    def test_workspace_initialization_imr(self):
        """Test workspace initialization with IMR approximant."""
        row = self._create_mock_row()
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd):
            workspace = cbc_template_fir.templates_workspace(
                template_table=[row],
                approximant="SEOBNRv4_ROM",
                psd=psd,
                f_low=20.0,
                time_slices=time_slices,
                autocorrelation_length=None,
                fhigh=1024.0,
            )

            assert workspace.sample_rate_max == 4096
            assert workspace.duration == 4.0
            assert hasattr(workspace, "max_ringtime")

    def test_workspace_initialization_non_imr(self):
        """Test workspace initialization with non-IMR approximant."""
        row = self._create_mock_row()
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd):
            workspace = cbc_template_fir.templates_workspace(
                template_table=[row],
                approximant="TaylorF2",
                psd=psd,
                f_low=20.0,
                time_slices=time_slices,
                autocorrelation_length=None,
                fhigh=1024.0,
            )

            assert workspace.sample_rate_max == 4096
            assert workspace.duration == 4.0

    def test_workspace_with_early_warning(self):
        """Test workspace with early warning (sample_rate > 2*fhigh)."""
        row = self._create_mock_row()
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd):
            # fhigh = 512, sample_rate_max = 4096 > 2*512 = 1024
            workspace = cbc_template_fir.templates_workspace(
                template_table=[row],
                approximant="TaylorF2",
                psd=psd,
                f_low=20.0,
                time_slices=time_slices,
                autocorrelation_length=None,
                fhigh=512.0,
            )

            assert hasattr(workspace, "max_shift_time")

    def test_workspace_with_fir_whitener(self):
        """Test workspace with FIR whitener enabled."""
        row = self._create_mock_row()
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        # Mock both condition_psd and the FIR whitener functions
        mock_kernel_fseries = mock.MagicMock()
        mock_kernel_fseries.data.data = numpy.ones(16385, dtype=complex)

        with (
            mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd),
            mock.patch(
                "sgnl.cbc_template_fir.kernels.fir_whitener_kernel",
                return_value=numpy.ones(32768),
            ),
            mock.patch(
                "sgnl.cbc_template_fir.taperzero_fseries",
                return_value=mock_kernel_fseries,
            ),
        ):
            workspace = cbc_template_fir.templates_workspace(
                template_table=[row],
                approximant="TaylorF2",
                psd=psd,
                f_low=20.0,
                time_slices=time_slices,
                autocorrelation_length=None,
                fhigh=1024.0,
                FIR_WHITENER=1,
            )

            assert hasattr(workspace, "kernel_fseries")

    def test_workspace_default_values(self):
        """Test workspace sets default values correctly."""
        row = self._create_mock_row()
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd):
            workspace = cbc_template_fir.templates_workspace(
                template_table=[row],
                approximant="TaylorF2",
                psd=psd,
                f_low=20.0,
                time_slices=time_slices,
                autocorrelation_length=None,
                fhigh=None,  # Should default to sample_rate_max / 2
            )

            assert workspace.fhigh == workspace.sample_rate_max / 2.0


class TestMakeWhitenedTemplate:
    """Tests for templates_workspace.make_whitened_template method."""

    def _create_mock_row(self, mass1=30.0, mass2=30.0):
        """Create a mock template bank row."""
        row = mock.MagicMock()
        row.mass1 = mass1
        row.mass2 = mass2
        row.spin1x = 0.0
        row.spin1y = 0.0
        row.spin1z = 0.0
        row.spin2x = 0.0
        row.spin2y = 0.0
        row.spin2z = 0.0
        row.mchirp = (mass1 * mass2) ** 0.6 / (mass1 + mass2) ** 0.2
        return row

    def _create_mock_psd(self, length=8193, delta_f=0.125):
        """Create a mock PSD."""
        psd = lal.CreateREAL8FrequencySeries(
            "psd",
            lal.LIGOTimeGPS(0),
            0.0,
            delta_f,
            lal.Unit("s strain^2"),
            length,
        )
        psd.data.data[:] = 1e-46
        return psd

    def _create_time_slices(self):
        """Create time slices structured array."""
        time_slices = numpy.array(
            [(4096, 0.0, 4.0)],
            dtype=[("rate", float), ("begin", float), ("end", float)],
        )
        return time_slices

    def test_make_whitened_template_non_imr(self):
        """Test making whitened template with non-IMR approximant."""
        row = self._create_mock_row()
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with (
            mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd),
            mock.patch("lal.WhitenCOMPLEX16FrequencySeries"),
        ):
            workspace = cbc_template_fir.templates_workspace(
                template_table=[row],
                approximant="TaylorF2",
                psd=psd,
                f_low=20.0,
                time_slices=time_slices,
                autocorrelation_length=None,
                fhigh=1024.0,
            )

            data, autocorrelation, sigmasq = workspace.make_whitened_template(row)

            assert data is not None
            assert autocorrelation is None  # No autocorrelation requested
            assert sigmasq > 0

    def test_make_whitened_template_imr_workspace(self):
        """Test workspace initialization with IMR approximant.

        Note: The full make_whitened_template with IMR approximants requires
        external ROM data files (SEOBNRv4_ROM etc). This test verifies the
        IMR-specific workspace initialization (max_ringtime calculation).
        """
        row = self._create_mock_row()
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd):
            workspace = cbc_template_fir.templates_workspace(
                template_table=[row],
                approximant="SEOBNRv4_ROM",
                psd=psd,
                f_low=20.0,
                time_slices=time_slices,
                autocorrelation_length=None,
                fhigh=1024.0,
            )

            # Verify IMR-specific attributes are set
            assert hasattr(workspace, "max_ringtime")
            assert workspace.max_ringtime > 0

    def test_make_whitened_template_with_autocorrelation(self):
        """Test making whitened template with autocorrelation."""
        row = self._create_mock_row()
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with (
            mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd),
            mock.patch("lal.WhitenCOMPLEX16FrequencySeries"),
        ):
            workspace = cbc_template_fir.templates_workspace(
                template_table=[row],
                approximant="TaylorF2",
                psd=psd,
                f_low=20.0,
                time_slices=time_slices,
                autocorrelation_length=101,  # Must be odd
                fhigh=1024.0,
            )

            data, autocorrelation, sigmasq = workspace.make_whitened_template(row)

            assert data is not None
            assert autocorrelation is not None
            assert sigmasq > 0

    def test_make_whitened_template_early_warning(self):
        """Test making whitened template with early warning mode."""
        row = self._create_mock_row()
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with (
            mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd),
            mock.patch("lal.WhitenCOMPLEX16FrequencySeries"),
        ):
            # sample_rate (4096) > 2 * fhigh (512) triggers early warning
            workspace = cbc_template_fir.templates_workspace(
                template_table=[row],
                approximant="TaylorF2",
                psd=psd,
                f_low=20.0,
                time_slices=time_slices,
                autocorrelation_length=None,
                fhigh=512.0,
            )

            data, autocorrelation, sigmasq = workspace.make_whitened_template(row)

            assert data is not None
            assert sigmasq > 0

    def test_make_whitened_template_fir_whitener(self):
        """Test making whitened template with FIR whitener.

        This tests the FIR whitener code path (lines 307-314) where:
        1. The kernel_fseries is multiplied with the template fseries
        2. Quadrature phase is added via templates.QuadraturePhase
        """
        row = self._create_mock_row()
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        # working_length = ceil_pow_2(16384 + 8*4096) = 65536
        # fseries length = 65536 // 2 + 1 = 32769
        # kernel_fseries needs: len(kernel_fseries) // 2 + 1 == len(fseries)
        # So kernel_fseries length = 65536
        working_length = 65536
        fseries_len = working_length // 2 + 1  # 32769

        mock_kernel_fseries = mock.MagicMock()
        mock_kernel_fseries.data.data = numpy.ones(working_length, dtype=complex)

        # Create a mock fseries for generate_template to return
        mock_fseries = lal.CreateCOMPLEX16FrequencySeries(
            "template",
            lal.LIGOTimeGPS(-0.1),  # negative epoch for proper conditioning
            0.0,
            1.0 / 16.0,  # deltaF matching working_duration
            lal.Unit("strain s"),
            fseries_len,
        )
        mock_fseries.data.data[:] = numpy.ones(fseries_len, dtype=complex) * 1e-21

        with (
            mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd),
            mock.patch(
                "sgnl.cbc_template_fir.kernels.fir_whitener_kernel",
                return_value=numpy.ones(working_length),
            ),
            mock.patch(
                "sgnl.cbc_template_fir.taperzero_fseries",
                return_value=mock_kernel_fseries,
            ),
            mock.patch(
                "sgnl.cbc_template_fir.generate_template",
                return_value=mock_fseries,
            ),
        ):
            workspace = cbc_template_fir.templates_workspace(
                template_table=[row],
                approximant="TaylorF2",
                psd=psd,
                f_low=20.0,
                time_slices=time_slices,
                autocorrelation_length=None,
                fhigh=1024.0,
                FIR_WHITENER=1,
            )

            # Actually call make_whitened_template to exercise lines 307-314
            data, autocorrelation, sigmasq = workspace.make_whitened_template(row)

            assert data is not None
            assert workspace.FIR_WHITENER == 1
            assert hasattr(workspace, "kernel_fseries")

    def test_make_whitened_template_imr_approximant(self):
        """Test making whitened template with IMR approximant.

        This tests the IMR conditioning code path (lines 358-366) where:
        1. condition_imr_template is called to align peaks
        2. template_table_row.end is set to the new end time
        """
        row = self._create_mock_row()
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        # working_length = 65536, fseries_len = 32769
        working_length = 65536
        fseries_len = working_length // 2 + 1

        # Create a mock fseries with negative epoch (required for IMR conditioning)
        # Epoch must satisfy: -len(data)/sample_rate <= epoch < 0
        # With working_length=65536 and sample_rate=4096: -16 <= epoch < 0
        # Additionally, tukey_beta = 2*abs(target_index - epoch_index)/len must be <= 1
        # epoch_index = -int(epoch * rate) - 1
        # target_index = len - 1 - int(rate * max_ringtime) ≈ 65372
        # We need epoch_index ≈ target_index, so epoch ≈ -15.96
        mock_fseries = lal.CreateCOMPLEX16FrequencySeries(
            "template",
            lal.LIGOTimeGPS(-15.96),  # epoch chosen to make epoch_index ≈ target_index
            0.0,
            1.0 / 16.0,  # deltaF matching working_duration
            lal.Unit("strain s"),
            fseries_len,
        )
        mock_fseries.data.data[:] = numpy.ones(fseries_len, dtype=complex) * 1e-21

        with (
            mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd),
            mock.patch("lal.WhitenCOMPLEX16FrequencySeries"),
            mock.patch(
                "sgnl.cbc_template_fir.generate_template",
                return_value=mock_fseries,
            ),
        ):
            workspace = cbc_template_fir.templates_workspace(
                template_table=[row],
                approximant="SEOBNRv4_ROM",  # IMR approximant
                psd=psd,
                f_low=20.0,
                time_slices=time_slices,
                autocorrelation_length=None,
                fhigh=1024.0,
            )

            # Verify IMR-specific attributes
            assert hasattr(workspace, "max_ringtime")
            assert workspace.max_ringtime > 0

            # Actually call make_whitened_template to exercise lines 358-366
            data, autocorrelation, sigmasq = workspace.make_whitened_template(row)

            assert data is not None
            # Verify that row.end was set (line 366-368)
            assert hasattr(row, "end")


class TestGenerateTemplates:
    """Tests for generate_templates function."""

    def _create_mock_row(self, mass1=30.0, mass2=30.0):
        """Create a mock template bank row."""
        row = mock.MagicMock()
        row.mass1 = mass1
        row.mass2 = mass2
        row.spin1x = 0.0
        row.spin1y = 0.0
        row.spin1z = 0.0
        row.spin2x = 0.0
        row.spin2y = 0.0
        row.spin2z = 0.0
        row.mchirp = (mass1 * mass2) ** 0.6 / (mass1 + mass2) ** 0.2
        return row

    def _create_mock_psd(self, length=8193, delta_f=0.125):
        """Create a mock PSD."""
        psd = lal.CreateREAL8FrequencySeries(
            "psd",
            lal.LIGOTimeGPS(0),
            0.0,
            delta_f,
            lal.Unit("s strain^2"),
            length,
        )
        psd.data.data[:] = 1e-46
        return psd

    def _create_time_slices(self):
        """Create time slices structured array."""
        time_slices = numpy.array(
            [(4096, 0.0, 4.0)],
            dtype=[("rate", float), ("begin", float), ("end", float)],
        )
        return time_slices

    def test_generate_templates_basic(self):
        """Test basic template generation."""
        rows = [self._create_mock_row()]
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with (
            mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd),
            mock.patch("lal.WhitenCOMPLEX16FrequencySeries"),
        ):
            template_bank, autocorr_bank, autocorr_mask, sigmasq, workspace = (
                cbc_template_fir.generate_templates(
                    rows, "TaylorF2", psd, 20.0, time_slices
                )
            )

            assert len(template_bank) == len(time_slices)
            assert autocorr_bank is None
            assert autocorr_mask is None
            assert len(sigmasq) == len(rows)

    def test_generate_templates_with_autocorrelation(self):
        """Test template generation with autocorrelation."""
        rows = [self._create_mock_row()]
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with (
            mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd),
            mock.patch("lal.WhitenCOMPLEX16FrequencySeries"),
        ):
            template_bank, autocorr_bank, autocorr_mask, sigmasq, workspace = (
                cbc_template_fir.generate_templates(
                    rows,
                    "TaylorF2",
                    psd,
                    20.0,
                    time_slices,
                    autocorrelation_length=101,
                )
            )

            assert autocorr_bank is not None
            assert autocorr_bank.shape == (len(rows), 101)
            assert autocorr_mask is not None

    def test_generate_templates_even_autocorrelation_raises(self):
        """Test that even autocorrelation length raises ValueError."""
        rows = [self._create_mock_row()]
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd):
            with pytest.raises(ValueError) as exc_info:
                cbc_template_fir.generate_templates(
                    rows,
                    "TaylorF2",
                    psd,
                    20.0,
                    time_slices,
                    autocorrelation_length=100,  # Even number
                )
            assert "must be odd" in str(exc_info.value)

    def test_generate_templates_verbose(self, capsys):
        """Test template generation with verbose output."""
        rows = [self._create_mock_row()]
        psd = self._create_mock_psd()
        time_slices = self._create_time_slices()

        with (
            mock.patch("sgnl.cbc_template_fir.condition_psd", return_value=psd),
            mock.patch("lal.WhitenCOMPLEX16FrequencySeries"),
        ):
            cbc_template_fir.generate_templates(
                rows, "TaylorF2", psd, 20.0, time_slices, verbose=True
            )

            captured = capsys.readouterr()
            assert "generating template" in captured.err


class TestDecomposeTemplates:
    """Tests for decompose_templates function."""

    def test_identity_decomposition(self):
        """Test identity decomposition (no-op mode)."""
        template_bank = numpy.random.randn(10, 100)

        U, s, Vh, chifacs = cbc_template_fir.decompose_templates(
            template_bank, tolerance=0.99, identity=True
        )

        assert numpy.array_equal(U, template_bank)
        assert s is None
        assert Vh is None
        assert len(chifacs) == 10

    def test_svd_decomposition(self):
        """Test SVD decomposition."""
        template_bank = numpy.random.randn(10, 100)

        U, s, Vh, chifacs = cbc_template_fir.decompose_templates(
            template_bank, tolerance=0.99, identity=False
        )

        assert U is not None
        assert s is not None
        assert Vh is not None
        assert len(chifacs) == 10

    def test_svd_decomposition_with_linalg_error(self, capsys):
        """Test SVD decomposition falls back to spawaveform on LinAlgError.

        Note: This tests the fallback path when scipy_svd raises LinAlgError.
        The fallback uses spawaveform.svd which may or may not be available.
        """
        template_bank = numpy.random.randn(10, 100)

        # Create mock SVD result - spawaveform.svd returns same format as scipy_svd
        mock_U = numpy.random.randn(100, 10)
        mock_s = numpy.sort(numpy.random.rand(10))[::-1]  # descending singular values
        mock_Vh = numpy.random.randn(10, 10)

        with (
            mock.patch(
                "sgnl.cbc_template_fir.scipy_svd",
                side_effect=numpy.linalg.LinAlgError("SVD failed"),
            ),
            mock.patch.object(
                cbc_template_fir.spawaveform,
                "svd",
                return_value=(mock_U, mock_s, mock_Vh),
                create=True,  # Create the attribute if it doesn't exist
            ),
        ):
            U, s, Vh, chifacs = cbc_template_fir.decompose_templates(
                template_bank, tolerance=0.99, identity=False
            )

            assert U is not None
            assert s is not None
            assert Vh is not None

            # Check that fallback message was printed
            captured = capsys.readouterr()
            assert "Falling back on spawaveform" in captured.err

    def test_chifacs_calculation(self):
        """Test that chifacs (sum of squares) is calculated correctly."""
        template_bank = numpy.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])

        U, s, Vh, chifacs = cbc_template_fir.decompose_templates(
            template_bank, tolerance=0.99, identity=True
        )

        # chifacs[0] should be 1^2 + 2^2 + 3^2 = 14
        assert chifacs[0] == pytest.approx(14.0)
        # chifacs[1] should be 4^2 + 5^2 + 6^2 = 77
        assert chifacs[1] == pytest.approx(77.0)

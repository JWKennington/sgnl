"""Tests for sgnl.measure_psd"""

from unittest import mock

import pytest

from sgnl import measure_psd


class TestMeasurePsd:
    """Tests for measure_psd function."""

    @pytest.fixture
    def mock_pipeline(self):
        """Create a mock Pipeline."""
        with mock.patch.object(measure_psd, "Pipeline") as mock_pipeline_class:
            mock_instance = mock.MagicMock()
            mock_pipeline_class.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def mock_components(self):
        """Mock all pipeline components."""
        with (
            mock.patch.object(measure_psd, "DevShmSource") as mock_source,
            mock.patch.object(measure_psd, "Resampler") as mock_resampler,
            mock.patch.object(measure_psd, "Whiten") as mock_whiten,
            mock.patch.object(measure_psd, "NullSeriesSink") as mock_null_sink,
            mock.patch.object(measure_psd, "PSDSink") as mock_psd_sink,
        ):
            yield {
                "source": mock_source,
                "resampler": mock_resampler,
                "whiten": mock_whiten,
                "null_sink": mock_null_sink,
                "psd_sink": mock_psd_sink,
            }

    @pytest.fixture
    def mock_gw_data_source_info(self):
        """Create mock GW data source info."""
        mock_info = mock.MagicMock()
        mock_info.seg = mock.MagicMock()
        mock_info.seg.__abs__ = mock.MagicMock(return_value=100.0)
        mock_info.seg.__str__ = mock.MagicMock(return_value="[0, 100)")
        return mock_info

    def test_measure_psd_basic(
        self, mock_pipeline, mock_components, mock_gw_data_source_info
    ):
        """Test basic PSD measurement."""
        measure_psd.measure_psd(
            gw_data_source_info=mock_gw_data_source_info,
            channel_name="H1:GDS-CALIB_STRAIN",
            shared_memory_dir="/dev/shm",
            wait_time=10.0,
            sample_rate=4096,
            whitening_method="median",
            reference_psd=None,
            instrument="H1",
            rate=4096,
        )

        # Pipeline should be created and run
        mock_pipeline.insert.assert_called()
        mock_pipeline.run.assert_called_once()

    def test_measure_psd_with_verbose(
        self, mock_pipeline, mock_components, mock_gw_data_source_info, capsys
    ):
        """Test PSD measurement with verbose output."""
        measure_psd.measure_psd(
            gw_data_source_info=mock_gw_data_source_info,
            channel_name="H1:GDS-CALIB_STRAIN",
            shared_memory_dir="/dev/shm",
            wait_time=10.0,
            sample_rate=4096,
            whitening_method="median",
            reference_psd=None,
            instrument="H1",
            rate=4096,
            verbose=True,
        )

        # Check that verbose output was produced
        captured = capsys.readouterr()
        assert "measuring PSD" in captured.err
        assert "building pipeline" in captured.err
        assert "running pipeline" in captured.err
        assert "PSD measurement complete" in captured.err

    def test_measure_psd_segment_too_short(self, mock_components):
        """Test that short segment raises ValueError."""
        mock_info = mock.MagicMock()
        mock_info.seg = mock.MagicMock()
        mock_info.seg.__abs__ = mock.MagicMock(
            return_value=10.0
        )  # Less than 8 * 8 = 64
        mock_info.seg.__str__ = mock.MagicMock(return_value="[0, 10)")

        with pytest.raises(ValueError) as exc_info:
            measure_psd.measure_psd(
                gw_data_source_info=mock_info,
                channel_name="H1:GDS-CALIB_STRAIN",
                shared_memory_dir="/dev/shm",
                wait_time=10.0,
                sample_rate=4096,
                whitening_method="median",
                reference_psd=None,
                instrument="H1",
                rate=4096,
            )

        assert "too short" in str(exc_info.value)

    def test_measure_psd_with_custom_fft_length(
        self, mock_pipeline, mock_components, mock_gw_data_source_info
    ):
        """Test PSD measurement with custom FFT length."""
        measure_psd.measure_psd(
            gw_data_source_info=mock_gw_data_source_info,
            channel_name="H1:GDS-CALIB_STRAIN",
            shared_memory_dir="/dev/shm",
            wait_time=10.0,
            sample_rate=4096,
            whitening_method="median",
            reference_psd=None,
            instrument="H1",
            rate=4096,
            psd_fft_length=4,
        )

        mock_pipeline.run.assert_called_once()

    def test_measure_psd_with_none_segment(self, mock_pipeline, mock_components):
        """Test PSD measurement when segment is None."""
        mock_info = mock.MagicMock()
        mock_info.seg = None  # No segment specified

        measure_psd.measure_psd(
            gw_data_source_info=mock_info,
            channel_name="H1:GDS-CALIB_STRAIN",
            shared_memory_dir="/dev/shm",
            wait_time=10.0,
            sample_rate=4096,
            whitening_method="median",
            reference_psd=None,
            instrument="H1",
            rate=4096,
        )

        # Should not raise, pipeline should run
        mock_pipeline.run.assert_called_once()

    def test_measure_psd_components_created(
        self, mock_pipeline, mock_components, mock_gw_data_source_info
    ):
        """Test that all components are created with correct parameters."""
        measure_psd.measure_psd(
            gw_data_source_info=mock_gw_data_source_info,
            channel_name="H1:GDS-CALIB_STRAIN",
            shared_memory_dir="/dev/shm",
            wait_time=10.0,
            sample_rate=4096,
            whitening_method="median",
            reference_psd=None,
            instrument="H1",
            rate=4096,
        )

        # Check that each component was created
        mock_components["source"].assert_called_once()
        mock_components["resampler"].assert_called_once()
        mock_components["whiten"].assert_called_once()
        mock_components["null_sink"].assert_called_once()
        mock_components["psd_sink"].assert_called_once()

"""Tests for sgnl.sinks.impulse module."""

from unittest import mock

import numpy as np

from sgnl.sinks import impulse


class TestImpulseSink:
    """Tests for ImpulseSink class."""

    @mock.patch("sgnl.sinks.impulse.Audioadapter")
    @mock.patch("sgnl.sinks.impulse.TSSink.__post_init__")
    def test_post_init(self, mock_super_init, mock_audioadapter):
        """Test __post_init__ initializes counters and adapters."""
        with mock.patch.object(impulse.ImpulseSink, "__post_init__"):
            sink = object.__new__(impulse.ImpulseSink)

        sink.sink_pads = ["pad1", "pad2"]

        impulse.ImpulseSink.__post_init__(sink)

        assert sink.cnt == {"pad1": 0, "pad2": 0}
        assert mock_audioadapter.call_count == 2

    @mock.patch("sgnl.sinks.impulse.Audioadapter")
    @mock.patch("sgnl.sinks.impulse.TSSink.__post_init__")
    def test_pull_data_pad_buffers_none(self, mock_super_init, mock_audioadapter):
        """Test pull method with data_pad when buffers is None."""
        with mock.patch.object(impulse.ImpulseSink, "__post_init__"):
            sink = object.__new__(impulse.ImpulseSink)

        mock_pad = mock.MagicMock()
        mock_pad.name = "test:data_pad"

        sink.sink_pads = [mock_pad]
        sink.cnt = {mock_pad: 0}
        sink.data_pad = "data_pad"
        sink.impulse_pad = "impulse_pad"
        sink.verbose = False
        sink.A = mock.MagicMock()
        sink.Ainput = mock.MagicMock()

        mock_bufs = mock.MagicMock()
        mock_bufs.metadata = {"impulse_offset": 1000}
        mock_bufs.buffers = None
        mock_bufs.__iter__ = mock.MagicMock(return_value=iter([]))

        with mock.patch.object(impulse.TSSink, "pull"):
            sink.pull(mock_pad, mock_bufs)

        assert sink.cnt[mock_pad] == 1
        assert sink.impulse_offset == 1000

    @mock.patch("sgnl.sinks.impulse.Offset")
    @mock.patch("sgnl.sinks.impulse.Audioadapter")
    @mock.patch("sgnl.sinks.impulse.TSSink.__post_init__")
    def test_pull_data_pad_verbose(
        self, mock_super_init, mock_audioadapter, mock_offset
    ):
        """Test pull method with data_pad in verbose mode."""
        with mock.patch.object(impulse.ImpulseSink, "__post_init__"):
            sink = object.__new__(impulse.ImpulseSink)

        mock_pad = mock.MagicMock()
        mock_pad.name = "test:data_pad"

        sink.sink_pads = [mock_pad]
        sink.cnt = {mock_pad: 0}
        sink.data_pad = "data_pad"
        sink.impulse_pad = "impulse_pad"
        sink.verbose = True
        sink.A = mock.MagicMock()
        sink.Ainput = mock.MagicMock()

        mock_bufs = mock.MagicMock()
        mock_bufs.metadata = {"impulse_offset": 1000}
        mock_bufs.buffers = [mock.MagicMock()]
        mock_bufs.__iter__ = mock.MagicMock(return_value=iter([]))

        with mock.patch.object(impulse.TSSink, "pull"):
            sink.pull(mock_pad, mock_bufs)

        assert sink.cnt[mock_pad] == 1

    @mock.patch("sgnl.sinks.impulse.Offset")
    @mock.patch("sgnl.sinks.impulse.Audioadapter")
    @mock.patch("sgnl.sinks.impulse.TSSink.__post_init__")
    def test_pull_data_pad_buf_before_impulse(
        self, mock_super_init, mock_audioadapter, mock_offset
    ):
        """Test pull method when buffer is before impulse offset."""
        with mock.patch.object(impulse.ImpulseSink, "__post_init__"):
            sink = object.__new__(impulse.ImpulseSink)

        mock_pad = mock.MagicMock()
        mock_pad.name = "test:data_pad"

        sink.sink_pads = [mock_pad]
        sink.cnt = {mock_pad: 0}
        sink.data_pad = "data_pad"
        sink.impulse_pad = "impulse_pad"
        sink.verbose = False
        sink.template_duration = 1.0
        sink.bankno = 0
        sink.A = mock.MagicMock()
        sink.Ainput = mock.MagicMock()

        # Buffer ends before impulse offset
        mock_buf = mock.MagicMock()
        mock_buf.end_offset = 500
        mock_buf.offset = 0

        mock_bufs = mock.MagicMock()
        mock_bufs.metadata = {"impulse_offset": 1000}
        mock_bufs.buffers = [mock_buf]
        mock_bufs.__iter__ = mock.MagicMock(return_value=iter([mock_buf]))

        with mock.patch.object(impulse.TSSink, "pull"):
            sink.pull(mock_pad, mock_bufs)

        # Should not push to A since buffer is before impulse
        sink.A.push.assert_not_called()

    @mock.patch("sgnl.sinks.impulse.Offset")
    @mock.patch("sgnl.sinks.impulse.Audioadapter")
    @mock.patch("sgnl.sinks.impulse.TSSink.__post_init__")
    def test_pull_data_pad_buf_around_impulse(
        self, mock_super_init, mock_audioadapter, mock_offset
    ):
        """Test pull method when buffer is around impulse offset."""
        with mock.patch.object(impulse.ImpulseSink, "__post_init__"):
            sink = object.__new__(impulse.ImpulseSink)

        mock_pad = mock.MagicMock()
        mock_pad.name = "test:data_pad"

        sink.sink_pads = [mock_pad]
        sink.cnt = {mock_pad: 0}
        sink.data_pad = "data_pad"
        sink.impulse_pad = "impulse_pad"
        sink.verbose = False
        sink.template_duration = 1.0
        sink.bankno = 0
        sink.A = mock.MagicMock()
        sink.Ainput = mock.MagicMock()

        mock_offset.fromsec.return_value = 2000

        # Buffer overlaps with impulse offset
        mock_buf = mock.MagicMock()
        mock_buf.end_offset = 1500
        mock_buf.offset = 500
        mock_buf.data = np.array([[1, 2, 3], [4, 5, 6]])

        mock_bufs = mock.MagicMock()
        mock_bufs.metadata = {"impulse_offset": 1000}
        mock_bufs.buffers = [mock_buf]
        mock_bufs.__iter__ = mock.MagicMock(return_value=iter([mock_buf]))

        with mock.patch.object(impulse.TSSink, "pull"):
            sink.pull(mock_pad, mock_bufs)

        # Should push to A since buffer is around impulse
        sink.A.push.assert_called_once()

    @mock.patch("sgnl.sinks.impulse.Offset")
    @mock.patch("sgnl.sinks.impulse.Audioadapter")
    @mock.patch("sgnl.sinks.impulse.TSSink.__post_init__")
    def test_pull_data_pad_buf_after_impulse_passed(
        self, mock_super_init, mock_audioadapter, mock_offset
    ):
        """Test pull when buffer is after impulse and test should run."""
        with mock.patch.object(impulse.ImpulseSink, "__post_init__"):
            sink = object.__new__(impulse.ImpulseSink)

        mock_pad = mock.MagicMock()
        mock_pad.name = "test:data_pad"

        sink.sink_pads = [mock_pad]
        sink.cnt = {mock_pad: 0}
        sink.data_pad = "data_pad"
        sink.impulse_pad = "impulse_pad"
        sink.verbose = False
        sink.template_duration = 1.0
        sink.bankno = 0
        sink.A = mock.MagicMock()
        sink.Ainput = mock.MagicMock()
        sink.mark_eos = mock.MagicMock()

        # fromsec returns values that make buf.offset > impulse_offset + duration
        mock_offset.fromsec.return_value = 100

        # Buffer is after the impulse window
        mock_buf = mock.MagicMock()
        mock_buf.end_offset = 5000
        mock_buf.offset = 4000

        mock_bufs = mock.MagicMock()
        mock_bufs.metadata = {"impulse_offset": 1000}
        mock_bufs.buffers = [mock_buf]
        mock_bufs.__iter__ = mock.MagicMock(return_value=iter([mock_buf]))

        # Mock impulse_test to return passed test
        with (
            mock.patch.object(impulse.TSSink, "pull"),
            mock.patch.object(sink, "impulse_test", return_value=(1000, 0.999)),
        ):
            sink.pull(mock_pad, mock_bufs)

        sink.mark_eos.assert_called_once()

    @mock.patch("sgnl.sinks.impulse.Offset")
    @mock.patch("sgnl.sinks.impulse.Audioadapter")
    @mock.patch("sgnl.sinks.impulse.TSSink.__post_init__")
    def test_pull_data_pad_impulse_test_failed(
        self, mock_super_init, mock_audioadapter, mock_offset
    ):
        """Test pull when impulse test fails."""
        with mock.patch.object(impulse.ImpulseSink, "__post_init__"):
            sink = object.__new__(impulse.ImpulseSink)

        mock_pad = mock.MagicMock()
        mock_pad.name = "test:data_pad"

        sink.sink_pads = [mock_pad]
        sink.cnt = {mock_pad: 0}
        sink.data_pad = "data_pad"
        sink.impulse_pad = "impulse_pad"
        sink.verbose = False
        sink.template_duration = 1.0
        sink.bankno = 0
        sink.A = mock.MagicMock()
        sink.Ainput = mock.MagicMock()
        sink.mark_eos = mock.MagicMock()

        mock_offset.fromsec.return_value = 100

        mock_buf = mock.MagicMock()
        mock_buf.end_offset = 5000
        mock_buf.offset = 4000

        mock_bufs = mock.MagicMock()
        mock_bufs.metadata = {"impulse_offset": 1000}
        mock_bufs.buffers = [mock_buf]
        mock_bufs.__iter__ = mock.MagicMock(return_value=iter([mock_buf]))

        # Mock impulse_test to return failed test (low match)
        with (
            mock.patch.object(impulse.TSSink, "pull"),
            mock.patch.object(sink, "impulse_test", return_value=(999, 0.5)),
        ):
            sink.pull(mock_pad, mock_bufs)

        sink.mark_eos.assert_called_once()

    @mock.patch("sgnl.sinks.impulse.Offset")
    @mock.patch("sgnl.sinks.impulse.Audioadapter")
    @mock.patch("sgnl.sinks.impulse.TSSink.__post_init__")
    def test_pull_impulse_pad(self, mock_super_init, mock_audioadapter, mock_offset):
        """Test pull method with impulse_pad."""
        with mock.patch.object(impulse.ImpulseSink, "__post_init__"):
            sink = object.__new__(impulse.ImpulseSink)

        mock_pad = mock.MagicMock()
        mock_pad.name = "test:impulse_pad"

        sink.sink_pads = [mock_pad]
        sink.cnt = {mock_pad: 0}
        sink.data_pad = "data_pad"
        sink.impulse_pad = "impulse_pad"
        sink.verbose = False
        sink.template_duration = 1.0
        sink.A = mock.MagicMock()
        sink.Ainput = mock.MagicMock()

        # Set up Offset.fromsec to return predictable values
        mock_offset.fromsec.side_effect = [100, 200]  # -1 sec, +duration

        # Buffer within impulse window
        mock_buf = mock.MagicMock()
        mock_buf.offset = 950

        mock_bufs = mock.MagicMock()
        mock_bufs.metadata = {"impulse_offset": 1000}
        mock_bufs.buffers = [mock_buf]
        mock_bufs.__iter__ = mock.MagicMock(return_value=iter([mock_buf]))

        with mock.patch.object(impulse.TSSink, "pull"):
            sink.pull(mock_pad, mock_bufs)

        sink.Ainput.push.assert_called_once_with(mock_buf)

    @mock.patch("sgnl.sinks.impulse.Offset")
    @mock.patch("sgnl.sinks.impulse.Audioadapter")
    @mock.patch("sgnl.sinks.impulse.TSSink.__post_init__")
    def test_pull_impulse_pad_outside_window(
        self, mock_super_init, mock_audioadapter, mock_offset
    ):
        """Test pull method with impulse_pad when buffer is outside window."""
        with mock.patch.object(impulse.ImpulseSink, "__post_init__"):
            sink = object.__new__(impulse.ImpulseSink)

        mock_pad = mock.MagicMock()
        mock_pad.name = "test:impulse_pad"

        sink.sink_pads = [mock_pad]
        sink.cnt = {mock_pad: 0}
        sink.data_pad = "data_pad"
        sink.impulse_pad = "impulse_pad"
        sink.verbose = False
        sink.template_duration = 1.0
        sink.A = mock.MagicMock()
        sink.Ainput = mock.MagicMock()

        mock_offset.fromsec.side_effect = [100, 200]

        # Buffer outside impulse window (before)
        mock_buf = mock.MagicMock()
        mock_buf.offset = 500

        mock_bufs = mock.MagicMock()
        mock_bufs.metadata = {"impulse_offset": 1000}
        mock_bufs.buffers = [mock_buf]
        mock_bufs.__iter__ = mock.MagicMock(return_value=iter([mock_buf]))

        with mock.patch.object(impulse.TSSink, "pull"):
            sink.pull(mock_pad, mock_bufs)

        sink.Ainput.push.assert_not_called()

    @mock.patch("sgnl.sinks.impulse.h5py.File")
    @mock.patch("sgnl.sinks.impulse.Offset")
    def test_impulse_test_basic(self, mock_offset, mock_h5file):
        """Test impulse_test method."""
        with mock.patch.object(impulse.ImpulseSink, "__post_init__"):
            sink = object.__new__(impulse.ImpulseSink)

        # Create mock templates
        n_templates = 8
        template_length = 100
        mock_templates = np.random.randn(n_templates, template_length)

        mock_file = mock.MagicMock()
        mock_file.__enter__ = mock.MagicMock(return_value=mock_file)
        mock_file.__exit__ = mock.MagicMock(return_value=False)
        mock_file.__getitem__ = mock.MagicMock(return_value=mock_templates)
        mock_h5file.return_value = mock_file

        # Mock filter output
        mock_filter_output = mock.MagicMock()
        mock_filter_output.cpu.return_value.numpy.return_value = np.random.randn(
            n_templates, template_length * 2
        )

        mock_A = mock.MagicMock()
        mock_A.size = 1000
        mock_A.copy_samples.return_value = mock_filter_output
        mock_A.offset = 0

        mock_Ainput = mock.MagicMock()
        mock_Ainput.size = 100
        mock_Ainput.copy_samples.return_value = np.random.randn(100)

        sink.original_templates = "test.h5"
        sink.verbose = False
        sink.plotname = None
        sink.A = mock_A
        sink.Ainput = mock_Ainput

        mock_offset.fromsamples.return_value = 100

        result_offset, result_match = sink.impulse_test()

        assert isinstance(result_match, float)
        mock_h5file.assert_called_once_with("test.h5", "r")

    @mock.patch("sgnl.sinks.impulse.h5py.File")
    @mock.patch("sgnl.sinks.impulse.Offset")
    def test_impulse_test_verbose(self, mock_offset, mock_h5file):
        """Test impulse_test method in verbose mode."""
        with mock.patch.object(impulse.ImpulseSink, "__post_init__"):
            sink = object.__new__(impulse.ImpulseSink)

        n_templates = 8
        template_length = 100
        mock_templates = np.random.randn(n_templates, template_length)

        mock_file = mock.MagicMock()
        mock_file.__enter__ = mock.MagicMock(return_value=mock_file)
        mock_file.__exit__ = mock.MagicMock(return_value=False)
        mock_file.__getitem__ = mock.MagicMock(return_value=mock_templates)
        mock_h5file.return_value = mock_file

        mock_filter_output = mock.MagicMock()
        mock_filter_output.cpu.return_value.numpy.return_value = np.random.randn(
            n_templates, template_length * 2
        )

        mock_A = mock.MagicMock()
        mock_A.size = 1000
        mock_A.copy_samples.return_value = mock_filter_output
        mock_A.offset = 0

        mock_Ainput = mock.MagicMock()
        mock_Ainput.size = 100
        mock_Ainput.copy_samples.return_value = np.random.randn(100)

        sink.original_templates = "test.h5"
        sink.verbose = True
        sink.plotname = None
        sink.A = mock_A
        sink.Ainput = mock_Ainput

        mock_offset.fromsamples.return_value = 100

        result_offset, result_match = sink.impulse_test()

        assert isinstance(result_match, float)

    @mock.patch("sgnl.sinks.impulse.plt")
    @mock.patch("sgnl.sinks.impulse.h5py.File")
    @mock.patch("sgnl.sinks.impulse.Offset")
    def test_impulse_test_with_plot(self, mock_offset, mock_h5file, mock_plt):
        """Test impulse_test method with plotting enabled."""
        with mock.patch.object(impulse.ImpulseSink, "__post_init__"):
            sink = object.__new__(impulse.ImpulseSink)

        n_templates = 8
        template_length = 100
        mock_templates = np.random.randn(n_templates, template_length)

        mock_file = mock.MagicMock()
        mock_file.__enter__ = mock.MagicMock(return_value=mock_file)
        mock_file.__exit__ = mock.MagicMock(return_value=False)
        mock_file.__getitem__ = mock.MagicMock(return_value=mock_templates)
        mock_h5file.return_value = mock_file

        mock_filter_output = mock.MagicMock()
        mock_filter_output.cpu.return_value.numpy.return_value = np.random.randn(
            n_templates, template_length * 2
        )

        mock_A = mock.MagicMock()
        mock_A.size = 1000
        mock_A.copy_samples.return_value = mock_filter_output
        mock_A.offset = 0

        mock_Ainput = mock.MagicMock()
        mock_Ainput.size = 100
        mock_Ainput.copy_samples.return_value = np.random.randn(100)

        sink.original_templates = "test.h5"
        sink.verbose = True
        sink.plotname = "test_plot"
        sink.A = mock_A
        sink.Ainput = mock_Ainput

        mock_offset.fromsamples.return_value = 100

        result_offset, result_match = sink.impulse_test()

        # Should call plot_wave
        assert mock_plt.figure.called

    @mock.patch("sgnl.sinks.impulse.plt")
    def test_plot_wave(self, mock_plt):
        """Test plot_wave method."""
        with mock.patch.object(impulse.ImpulseSink, "__post_init__"):
            sink = object.__new__(impulse.ImpulseSink)

        data = [
            np.array([1, 2, 3, 4, 5]),
            np.array([1, 2, 3]),
            np.array([1, 2]),
        ]
        dataname = ["input", "output", "response"]
        figname = "test_fig"
        matchdata = np.array([0.9, 0.95, 0.98])
        cavgn = 0.94

        sink.plot_wave(data, dataname, figname, matchdata, cavgn)

        # Should create two figures
        assert mock_plt.figure.call_count == 2
        # Should save two files
        assert mock_plt.savefig.call_count == 2
        mock_plt.savefig.assert_any_call("test_figresponse")
        mock_plt.savefig.assert_any_call("test_figmatch")

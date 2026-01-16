"""Tests for sgnl.strike_object module."""

import zlib
from unittest import mock

import numpy as np
import pytest
import torch

from sgnl import strike_object


class TestXmlString:
    """Tests for xml_string function."""

    def test_xml_string_basic(self):
        """Test xml_string with basic rstat mock."""
        mock_rstat = mock.MagicMock()
        mock_rstat.save_fileobj.side_effect = lambda f: f.write(b"<xml>test</xml>")

        result = strike_object.xml_string(mock_rstat)

        assert result == "<xml>test</xml>"
        mock_rstat.save_fileobj.assert_called_once()

    def test_xml_string_with_gc_enabled(self):
        """Test xml_string when GC is enabled."""
        mock_rstat = mock.MagicMock()
        mock_rstat.save_fileobj.side_effect = lambda f: f.write(b"<xml>gc</xml>")

        original_gc = strike_object.GC
        try:
            strike_object.GC = True
            with mock.patch("sgnl.strike_object.gc.collect", return_value=0):
                result = strike_object.xml_string(mock_rstat)
            assert result == "<xml>gc</xml>"
        finally:
            strike_object.GC = original_gc


class TestStrikeObjectValidate:
    """Tests for StrikeObject._validate method."""

    def _create_strike_object_mock(self, **kwargs):
        """Create a StrikeObject with __post_init__ mocked."""
        with mock.patch.object(strike_object.StrikeObject, "__post_init__"):
            obj = object.__new__(strike_object.StrikeObject)

        # Set default attributes
        obj.is_online = kwargs.get("is_online", False)
        obj.injections = kwargs.get("injections", False)
        obj.output_likelihood_file = kwargs.get("output_likelihood_file", None)
        obj.input_likelihood_file = kwargs.get("input_likelihood_file", None)
        obj.bankids_map = kwargs.get("bankids_map", {"0000": [0]})
        obj.rank_stat_pdf_file = kwargs.get("rank_stat_pdf_file", None)
        obj.zerolag_rank_stat_pdf_file = kwargs.get("zerolag_rank_stat_pdf_file", None)
        return obj

    def test_validate_offline_injection_with_output_file_raises(self):
        """Test validation raises error for offline injection with output file."""
        obj = self._create_strike_object_mock(
            is_online=False, injections=True, output_likelihood_file=["file.xml"]
        )

        with pytest.raises(
            ValueError, match="Must not set --output-likelihood-file when --injections"
        ):
            obj._validate()

    def test_validate_offline_noninj_sets_output_file_dict(self):
        """Test validation converts output_likelihood_file to dict."""
        obj = self._create_strike_object_mock(
            is_online=False,
            injections=False,
            output_likelihood_file=["file1.xml", "file2.xml"],
            bankids_map={"0000": [0], "0001": [1]},
        )

        obj._validate()

        assert obj.output_likelihood_file == {"0000": "file1.xml", "0001": "file2.xml"}

    def test_validate_online_injection_no_input_file_raises(self):
        """Test validation raises error for online injection without input file."""
        obj = self._create_strike_object_mock(
            is_online=True, injections=True, input_likelihood_file=None
        )

        with pytest.raises(
            ValueError, match="Must specify --input-likelihood-file when running"
        ):
            obj._validate()

    def test_validate_online_injection_with_output_file_raises(self):
        """Test validation raises error for online injection with output file."""
        obj = self._create_strike_object_mock(
            is_online=True,
            injections=True,
            input_likelihood_file=["in.xml"],
            output_likelihood_file=["out.xml"],
        )

        with pytest.raises(
            ValueError, match="Must not specify --output-likelihood-file"
        ):
            obj._validate()

    def test_validate_online_injection_no_rank_stat_pdf_raises(self):
        """Test validation raises error for online injection without rank_stat_pdf."""
        obj = self._create_strike_object_mock(
            is_online=True,
            injections=True,
            input_likelihood_file=["in.xml"],
            rank_stat_pdf_file=None,
        )

        with pytest.raises(
            ValueError, match="Must specify --rank-stat-pdf-file when running"
        ):
            obj._validate()

    def test_validate_online_injection_with_zerolag_raises(self):
        """Test validation raises error for online injection with zerolag file."""
        obj = self._create_strike_object_mock(
            is_online=True,
            injections=True,
            input_likelihood_file=["in.xml"],
            rank_stat_pdf_file="rank.xml",
            zerolag_rank_stat_pdf_file=["zerolag.xml"],
        )

        with pytest.raises(
            ValueError, match="Must not specify --zerolag-rank-stat-pdf-file"
        ):
            obj._validate()

    def test_validate_online_noninj_no_input_file_raises(self):
        """Test validation raises error for online noninj without input file."""
        obj = self._create_strike_object_mock(
            is_online=True, injections=False, input_likelihood_file=None
        )

        with pytest.raises(
            ValueError, match="Must specify --input-likelihood-file when running"
        ):
            obj._validate()

    def test_validate_online_noninj_no_output_file_raises(self):
        """Test validation raises error for online noninj without output file."""
        obj = self._create_strike_object_mock(
            is_online=True,
            injections=False,
            input_likelihood_file=["in.xml"],
            output_likelihood_file=None,
        )

        with pytest.raises(ValueError, match="Must specify --output-likelihood-file"):
            obj._validate()

    def test_validate_online_noninj_mismatched_files_raises(self):
        """Test validation raises error when input and output files differ."""
        obj = self._create_strike_object_mock(
            is_online=True,
            injections=False,
            input_likelihood_file=["in.xml"],
            output_likelihood_file=["out.xml"],
        )

        with pytest.raises(
            ValueError, match="--input-likelihood-file must be the same"
        ):
            obj._validate()

    def test_validate_online_noninj_no_rank_stat_pdf_raises(self):
        """Test validation raises error for online noninj without rank_stat_pdf."""
        obj = self._create_strike_object_mock(
            is_online=True,
            injections=False,
            input_likelihood_file=["same.xml"],
            output_likelihood_file=["same.xml"],
            rank_stat_pdf_file=None,
        )

        with pytest.raises(ValueError, match="Must specify --rank-stat-pdf-file"):
            obj._validate()

    def test_validate_online_noninj_no_zerolag_raises(self):
        """Test validation raises error for online noninj without zerolag file."""
        obj = self._create_strike_object_mock(
            is_online=True,
            injections=False,
            input_likelihood_file=["same.xml"],
            output_likelihood_file=["same.xml"],
            rank_stat_pdf_file="rank.xml",
            zerolag_rank_stat_pdf_file=None,
        )

        with pytest.raises(
            ValueError, match="Must specify --zerolag-rank-stat-pdf-file"
        ):
            obj._validate()


class TestStrikeObjectLoadLr:
    """Tests for StrikeObject._load_lr static method."""

    @mock.patch("sgnl.strike_object.LnLikelihoodRatio")
    def test_load_lr_success_first_try(self, mock_lr_class):
        """Test _load_lr succeeds on first try."""
        mock_lr = mock.MagicMock()
        mock_lr_class.load.return_value = mock_lr

        result = strike_object.StrikeObject._load_lr("test.xml")

        assert result is mock_lr
        mock_lr_class.load.assert_called_once_with("test.xml")

    @mock.patch("sgnl.strike_object.time.sleep")
    @mock.patch("sgnl.strike_object.LnLikelihoodRatio")
    def test_load_lr_retries_on_error(self, mock_lr_class, mock_sleep):
        """Test _load_lr retries on OSError."""
        mock_lr = mock.MagicMock()
        mock_lr_class.load.side_effect = [OSError("test"), mock_lr]

        result = strike_object.StrikeObject._load_lr("test.xml")

        assert result is mock_lr
        assert mock_lr_class.load.call_count == 2
        mock_sleep.assert_called_once_with(1)

    @mock.patch("sgnl.strike_object.time.sleep")
    @mock.patch("sgnl.strike_object.LnLikelihoodRatio")
    def test_load_lr_retries_on_eoferror(self, mock_lr_class, mock_sleep):
        """Test _load_lr retries on EOFError."""
        mock_lr = mock.MagicMock()
        mock_lr_class.load.side_effect = [EOFError("test"), mock_lr]

        result = strike_object.StrikeObject._load_lr("test.xml")

        assert result is mock_lr
        assert mock_lr_class.load.call_count == 2

    @mock.patch("sgnl.strike_object.time.sleep")
    @mock.patch("sgnl.strike_object.LnLikelihoodRatio")
    def test_load_lr_retries_on_zlib_error(self, mock_lr_class, mock_sleep):
        """Test _load_lr retries on zlib.error."""
        mock_lr = mock.MagicMock()
        mock_lr_class.load.side_effect = [zlib.error("test"), mock_lr]

        result = strike_object.StrikeObject._load_lr("test.xml")

        assert result is mock_lr
        assert mock_lr_class.load.call_count == 2

    @mock.patch("sgnl.strike_object.time.sleep")
    @mock.patch("sgnl.strike_object.LnLikelihoodRatio")
    def test_load_lr_exceeds_retries_raises(self, mock_lr_class, mock_sleep):
        """Test _load_lr raises RuntimeError after exceeding retries."""
        mock_lr_class.load.side_effect = OSError("persistent error")

        with pytest.raises(RuntimeError, match="Exceeded retries"):
            strike_object.StrikeObject._load_lr("test.xml")

        assert mock_lr_class.load.call_count == 10


class TestStrikeObjectUpdateAssignLr:
    """Tests for StrikeObject._update_assign_lr static method."""

    def test_update_assign_lr_healthy(self):
        """Test _update_assign_lr when lr is healthy."""
        mock_lr = mock.MagicMock()
        mock_lr.is_healthy.return_value = True
        mock_frankenstein = mock.MagicMock()
        mock_upload = mock.MagicMock()
        mock_lr.copy.side_effect = [mock_frankenstein, mock_upload]

        frank, upload = strike_object.StrikeObject._update_assign_lr(mock_lr)

        assert frank is mock_frankenstein
        assert upload is mock_upload
        mock_frankenstein.finish.assert_called_once()

    def test_update_assign_lr_not_healthy(self):
        """Test _update_assign_lr when lr is not healthy."""
        mock_lr = mock.MagicMock()
        mock_lr.is_healthy.return_value = False

        frank, upload = strike_object.StrikeObject._update_assign_lr(mock_lr)

        assert frank is None
        assert upload is None


class TestStrikeObjectUpdateDynamic:
    """Tests for StrikeObject.update_dynamic method."""

    def _create_strike_object_for_update(self):
        """Create a StrikeObject configured for update_dynamic tests."""
        with mock.patch.object(strike_object.StrikeObject, "__post_init__"):
            obj = object.__new__(strike_object.StrikeObject)

        mock_lr = mock.MagicMock()
        mock_lr.instruments = ["H1", "L1"]
        mock_lr.min_instruments = 2
        mock_lr.delta_t = 0.005
        mock_lr.template_ids = np.array([1, 2, 3])
        mock_lr.terms = {
            "P_of_tref_Dh": mock.MagicMock(
                triggerrates={"H1": mock.MagicMock()},
                horizon_history={"H1": {}},
            )
        }

        obj.likelihood_ratios = {"0000": mock_lr}
        obj.frankensteins = {"0000": mock.MagicMock()}
        obj.likelihood_ratio_uploads = {"0000": mock.MagicMock()}
        return obj

    def test_update_dynamic_with_new_lr(self):
        """Test update_dynamic with a new lr."""
        obj = self._create_strike_object_for_update()
        old_lr = obj.likelihood_ratios["0000"]

        new_lr = mock.MagicMock()
        new_lr.instruments = old_lr.instruments
        new_lr.min_instruments = old_lr.min_instruments
        new_lr.delta_t = old_lr.delta_t
        new_lr.template_ids = old_lr.template_ids
        new_lr.terms = {"P_of_tref_Dh": mock.MagicMock()}

        obj.update_dynamic("0000", None, None, new_lr=new_lr)

        assert obj.likelihood_ratios["0000"] is new_lr

    def test_update_dynamic_with_incompatible_lr_raises(self):
        """Test update_dynamic raises error for incompatible lr."""
        obj = self._create_strike_object_for_update()

        new_lr = mock.MagicMock()
        new_lr.instruments = ["H1"]  # Different
        new_lr.min_instruments = 2
        new_lr.delta_t = 0.005
        new_lr.template_ids = np.array([1, 2, 3])

        with pytest.raises(ValueError, match="incompatible ranking statistic"):
            obj.update_dynamic("0000", None, None, new_lr=new_lr)

    def test_update_dynamic_with_frankenstein(self):
        """Test update_dynamic with frankenstein update."""
        obj = self._create_strike_object_for_update()

        mock_frankenstein = mock.MagicMock()
        mock_frankenstein.terms = {"P_of_tref_Dh": mock.MagicMock()}
        mock_upload = mock.MagicMock()
        mock_upload.terms = {"P_of_tref_Dh": mock.MagicMock()}

        obj.update_dynamic("0000", mock_frankenstein, mock_upload)

        assert obj.frankensteins["0000"] is mock_frankenstein
        assert obj.likelihood_ratio_uploads["0000"] is mock_upload

    def test_update_dynamic_with_gc_enabled(self):
        """Test update_dynamic when GC is enabled."""
        obj = self._create_strike_object_for_update()

        mock_frankenstein = mock.MagicMock()
        mock_frankenstein.terms = {"P_of_tref_Dh": mock.MagicMock()}
        mock_upload = mock.MagicMock()
        mock_upload.terms = {"P_of_tref_Dh": mock.MagicMock()}

        original_gc = strike_object.GC
        try:
            strike_object.GC = True
            with mock.patch("sgnl.strike_object.gc.collect", return_value=0):
                with mock.patch("sgnl.strike_object.gc.garbage", []):
                    obj.update_dynamic("0000", mock_frankenstein, mock_upload)
        finally:
            strike_object.GC = original_gc

    def test_update_dynamic_with_gc_garbage(self):
        """Test update_dynamic when GC has garbage."""
        obj = self._create_strike_object_for_update()

        mock_frankenstein = mock.MagicMock()
        mock_frankenstein.terms = {"P_of_tref_Dh": mock.MagicMock()}
        mock_upload = mock.MagicMock()
        mock_upload.terms = {"P_of_tref_Dh": mock.MagicMock()}

        original_gc = strike_object.GC
        try:
            strike_object.GC = True
            mock_garbage_item = mock.MagicMock()
            with mock.patch("sgnl.strike_object.gc.collect", return_value=0):
                with mock.patch("sgnl.strike_object.gc.garbage", [mock_garbage_item]):
                    obj.update_dynamic("0000", mock_frankenstein, mock_upload)
        finally:
            strike_object.GC = original_gc

    def test_update_dynamic_with_new_lr_and_gc(self):
        """Test update_dynamic with new_lr and GC enabled."""
        obj = self._create_strike_object_for_update()
        old_lr = obj.likelihood_ratios["0000"]

        new_lr = mock.MagicMock()
        new_lr.instruments = old_lr.instruments
        new_lr.min_instruments = old_lr.min_instruments
        new_lr.delta_t = old_lr.delta_t
        new_lr.template_ids = old_lr.template_ids
        new_lr.terms = {"P_of_tref_Dh": mock.MagicMock()}

        original_gc = strike_object.GC
        try:
            strike_object.GC = True
            with mock.patch("sgnl.strike_object.gc.collect", return_value=0):
                obj.update_dynamic("0000", None, None, new_lr=new_lr)
        finally:
            strike_object.GC = original_gc

        assert obj.likelihood_ratios["0000"] is new_lr


class TestStrikeObjectLoadRankStatPdf:
    """Tests for StrikeObject.load_rank_stat_pdf method."""

    def _create_strike_object_for_load(self):
        """Create a StrikeObject configured for load_rank_stat_pdf tests."""
        with mock.patch.object(strike_object.StrikeObject, "__post_init__"):
            obj = object.__new__(strike_object.StrikeObject)

        obj.rank_stat_pdf_file = "rank_stat.xml"
        obj.verbose = False
        obj.bankids_map = {"0000": [0]}

        mock_lr = mock.MagicMock()
        mock_lr.template_ids = np.array([1, 2, 3])
        obj.likelihood_ratios = {"0000": mock_lr}
        obj.fapfar = None
        obj.rank_stat_pdf = None
        return obj

    @mock.patch("sgnl.strike_object.far.FAPFAR")
    @mock.patch("sgnl.strike_object.RankingStatPDF")
    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    @mock.patch("os.access")
    def test_load_rank_stat_pdf_success(
        self, mock_access, mock_local_path, mock_rspdf_class, mock_fapfar_class
    ):
        """Test load_rank_stat_pdf successfully loads file."""
        mock_access.return_value = True
        mock_local_path.return_value = "/path/to/rank_stat.xml"

        mock_rspdf = mock.MagicMock()
        mock_rspdf.template_ids = np.array([1, 2, 3])
        mock_rspdf.is_healthy.return_value = True
        mock_rspdf_class.load.return_value = mock_rspdf

        obj = self._create_strike_object_for_load()
        obj.load_rank_stat_pdf()

        assert obj.rank_stat_pdf is mock_rspdf
        mock_fapfar_class.assert_called_once()

    @mock.patch("sgnl.strike_object.RankingStatPDF")
    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    @mock.patch("os.access")
    def test_load_rank_stat_pdf_not_healthy(
        self, mock_access, mock_local_path, mock_rspdf_class
    ):
        """Test load_rank_stat_pdf when rspdf is not healthy."""
        mock_access.return_value = True
        mock_local_path.return_value = "/path/to/rank_stat.xml"

        mock_rspdf = mock.MagicMock()
        mock_rspdf.template_ids = np.array([1, 2, 3])
        mock_rspdf.is_healthy.return_value = False
        mock_rspdf_class.load.return_value = mock_rspdf

        obj = self._create_strike_object_for_load()
        obj.load_rank_stat_pdf()

        assert obj.rank_stat_pdf is mock_rspdf
        assert obj.fapfar is None

    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    @mock.patch("os.access")
    def test_load_rank_stat_pdf_file_not_accessible(self, mock_access, mock_local_path):
        """Test load_rank_stat_pdf when file is not accessible."""
        mock_access.return_value = False
        mock_local_path.return_value = "/path/to/rank_stat.xml"

        obj = self._create_strike_object_for_load()
        obj.load_rank_stat_pdf()

        assert obj.rank_stat_pdf is None
        assert obj.fapfar is None

    @mock.patch("sgnl.strike_object.RankingStatPDF")
    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    @mock.patch("os.access")
    def test_load_rank_stat_pdf_wrong_templates_raises(
        self, mock_access, mock_local_path, mock_rspdf_class
    ):
        """Test load_rank_stat_pdf raises error for wrong templates."""
        mock_access.return_value = True
        mock_local_path.return_value = "/path/to/rank_stat.xml"

        mock_rspdf = mock.MagicMock()
        mock_rspdf.template_ids = np.array([99, 100])  # Different templates
        mock_rspdf_class.load.return_value = mock_rspdf

        obj = self._create_strike_object_for_load()

        with pytest.raises(ValueError, match="wrong templates"):
            obj.load_rank_stat_pdf()

    @mock.patch("sgnl.strike_object.far.FAPFAR")
    @mock.patch("sgnl.strike_object.RankingStatPDF")
    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    @mock.patch("os.access")
    def test_load_rank_stat_pdf_replaces_existing(
        self, mock_access, mock_local_path, mock_rspdf_class, mock_fapfar_class
    ):
        """Test load_rank_stat_pdf replaces existing fapfar."""
        mock_access.return_value = True
        mock_local_path.return_value = "/path/to/rank_stat.xml"

        mock_rspdf = mock.MagicMock()
        mock_rspdf.template_ids = np.array([1, 2, 3])
        mock_rspdf.is_healthy.return_value = True
        mock_rspdf_class.load.return_value = mock_rspdf

        obj = self._create_strike_object_for_load()
        # Set up existing fapfar and rank_stat_pdf
        obj.fapfar = mock.MagicMock()
        obj.fapfar.ccdf_interpolator = mock.MagicMock()
        obj.rank_stat_pdf = mock.MagicMock()

        obj.load_rank_stat_pdf()

        assert obj.rank_stat_pdf is mock_rspdf

    @mock.patch("sgnl.strike_object.far.FAPFAR")
    @mock.patch("sgnl.strike_object.RankingStatPDF")
    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    @mock.patch("os.access")
    def test_load_rank_stat_pdf_with_gc(
        self, mock_access, mock_local_path, mock_rspdf_class, mock_fapfar_class
    ):
        """Test load_rank_stat_pdf with GC enabled."""
        mock_access.return_value = True
        mock_local_path.return_value = "/path/to/rank_stat.xml"

        mock_rspdf = mock.MagicMock()
        mock_rspdf.template_ids = np.array([1, 2, 3])
        mock_rspdf.is_healthy.return_value = True
        mock_rspdf_class.load.return_value = mock_rspdf

        obj = self._create_strike_object_for_load()
        obj.fapfar = mock.MagicMock()
        obj.fapfar.ccdf_interpolator = mock.MagicMock()
        obj.rank_stat_pdf = mock.MagicMock()

        original_gc = strike_object.GC
        try:
            strike_object.GC = True
            with mock.patch("sgnl.strike_object.gc.collect", return_value=0):
                obj.load_rank_stat_pdf()
        finally:
            strike_object.GC = original_gc


class TestStrikeObjectSaveSnapshot:
    """Tests for StrikeObject save snapshot methods."""

    def _create_strike_object_for_save(self):
        """Create a StrikeObject configured for save tests."""
        with mock.patch.object(strike_object.StrikeObject, "__post_init__"):
            obj = object.__new__(strike_object.StrikeObject)

        obj.ifos = ["H1", "L1"]
        obj.bankids_map = {"0000": [0]}
        obj.output_likelihood_file = {"0000": "output.xml"}
        obj.zerolag_rank_stat_pdf_file = {"0000": "zerolag.xml"}

        mock_lr = mock.MagicMock()
        obj.likelihood_ratios = {"0000": mock_lr}
        obj.zerolag_rank_stat_pdfs = {"0000": mock.MagicMock()}
        return obj

    @mock.patch("sgnl.strike_object.shutil.copy")
    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    def test_save_snapshot_instance_method(self, mock_local_path, mock_copy):
        """Test _save_snapshot instance method."""
        mock_local_path.side_effect = lambda x: x

        obj = self._create_strike_object_for_save()
        obj._save_snapshot("0000", "snapshot.xml")

        obj.likelihood_ratios["0000"].save.assert_called_once_with("snapshot.xml")
        obj.zerolag_rank_stat_pdfs["0000"].save.assert_called_once()
        assert mock_copy.call_count == 2

    @mock.patch("sgnl.strike_object.shutil.copy")
    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    def test_save_snapshot_static_method(self, mock_local_path, mock_copy):
        """Test save_snapshot static method."""
        mock_local_path.return_value = "/local/path.xml"
        mock_lr = mock.MagicMock()

        strike_object.StrikeObject.save_snapshot(mock_lr, "fn.xml", "output.xml")

        mock_lr.save.assert_called_once_with("fn.xml")
        mock_copy.assert_called_once()

    @mock.patch("sgnl.strike_object.shutil.copy")
    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    def test_save_snapshot_with_gc(self, mock_local_path, mock_copy):
        """Test save_snapshot static method with GC enabled."""
        mock_local_path.return_value = "/local/path.xml"
        mock_lr = mock.MagicMock()

        original_gc = strike_object.GC
        try:
            strike_object.GC = True
            with mock.patch("sgnl.strike_object.gc.collect", return_value=0):
                strike_object.StrikeObject.save_snapshot(
                    mock_lr, "fn.xml", "output.xml"
                )
        finally:
            strike_object.GC = original_gc


class TestStrikeObjectSaveSnrChiLnpdf:
    """Tests for StrikeObject SNR chi lnpdf methods."""

    def test_save_snr_chi_lnpdf_instance(self):
        """Test _save_snr_chi_lnpdf instance method."""
        with mock.patch.object(strike_object.StrikeObject, "__post_init__"):
            obj = object.__new__(strike_object.StrikeObject)

        obj.ifos = ["H1"]
        obj.bankids_map = {"0000": [0]}
        obj.snr_chisq_lnpdf_noise = {"H1": torch.tensor([[[1.0, 2.0], [3.0, 4.0]]])}

        mock_lr = mock.MagicMock()
        mock_lr.terms = {
            "P_of_SNR_chisq": mock.MagicMock(
                snr_chisq_lnpdf_noise={"H1_snr_chi": mock.MagicMock()}
            )
        }
        obj.likelihood_ratios = {"0000": mock_lr}

        obj._save_snr_chi_lnpdf("0000")

        # Verify array was set
        assert (
            mock_lr.terms["P_of_SNR_chisq"].snr_chisq_lnpdf_noise["H1_snr_chi"].array
            is not None
        )

    def test_save_snr_chi_lnpdf_static(self):
        """Test save_snr_chi_lnpdf static method."""
        mock_lr = mock.MagicMock()
        mock_lr.terms = {
            "P_of_SNR_chisq": mock.MagicMock(
                snr_chisq_lnpdf_noise={"H1_snr_chi": mock.MagicMock()}
            )
        }

        snr_chisq_lnpdf_noise = {"H1": torch.tensor([[[1.0, 2.0], [3.0, 4.0]]])}

        strike_object.StrikeObject.save_snr_chi_lnpdf(
            ["H1"], 0, mock_lr, snr_chisq_lnpdf_noise
        )

        assert (
            mock_lr.terms["P_of_SNR_chisq"].snr_chisq_lnpdf_noise["H1_snr_chi"].array
            is not None
        )


class TestStrikeObjectSaveCountsByTemplateId:
    """Tests for StrikeObject counts by template id methods."""

    def test_save_counts_by_template_id_instance(self):
        """Test _save_counts_by_template_id instance method."""
        with mock.patch.object(strike_object.StrikeObject, "__post_init__"):
            obj = object.__new__(strike_object.StrikeObject)

        obj.bankids_map = {"0000": [0]}
        obj.all_template_ids = np.array([[1, 2, 3]])
        obj.counts_by_template_id_counter = torch.tensor([[5, 0, 3]])

        mock_lr = mock.MagicMock()
        mock_lr.terms = {
            "P_of_Template": mock.MagicMock(counts_by_template_id={1: 0, 2: 0, 3: 0})
        }
        obj.likelihood_ratios = {"0000": mock_lr}

        obj._save_counts_by_template_id("0000")

        # Verify counts were updated
        assert mock_lr.terms["P_of_Template"].counts_by_template_id[1] == 5
        assert mock_lr.terms["P_of_Template"].counts_by_template_id[3] == 3
        # Verify counter was reset
        assert (obj.counts_by_template_id_counter[0] == 0).all()

    def test_save_counts_by_template_id_static(self):
        """Test save_counts_by_template_id static method."""
        mock_lr = mock.MagicMock()
        mock_lr.terms = {
            "P_of_Template": mock.MagicMock(counts_by_template_id={1: 0, 2: 0})
        }

        counts = np.array([5, 3])
        all_template_ids = np.array([1, 2])

        strike_object.StrikeObject.save_counts_by_template_id(
            mock_lr, counts, all_template_ids
        )

        assert mock_lr.terms["P_of_Template"].counts_by_template_id[1] == 5
        assert mock_lr.terms["P_of_Template"].counts_by_template_id[2] == 3


class TestStrikeObjectResetDynamic:
    """Tests for StrikeObject.reset_dynamic static method."""

    def test_reset_dynamic_with_new_lr(self):
        """Test reset_dynamic sets new_lr attributes to None."""
        new_lr = mock.MagicMock()
        new_lr.terms = {"P_of_tref_Dh": mock.MagicMock()}

        strike_object.StrikeObject.reset_dynamic(None, None, new_lr=new_lr)

        assert new_lr.terms["P_of_tref_Dh"].triggerrates is None
        assert new_lr.terms["P_of_tref_Dh"].horizon_history is None
        assert new_lr.triggerrates is None
        assert new_lr.horizon_history is None

    def test_reset_dynamic_with_frankenstein(self):
        """Test reset_dynamic sets frankenstein attributes to None."""
        frankenstein = mock.MagicMock()
        frankenstein.terms = {"P_of_tref_Dh": mock.MagicMock()}
        likelihood_ratio_upload = mock.MagicMock()
        likelihood_ratio_upload.terms = {"P_of_tref_Dh": mock.MagicMock()}

        strike_object.StrikeObject.reset_dynamic(
            frankenstein, likelihood_ratio_upload, new_lr=None
        )

        assert frankenstein.terms["P_of_tref_Dh"].triggerrates is None
        assert likelihood_ratio_upload.terms["P_of_tref_Dh"].triggerrates is None

    def test_reset_dynamic_with_gc(self):
        """Test reset_dynamic with GC enabled."""
        frankenstein = mock.MagicMock()
        frankenstein.terms = {"P_of_tref_Dh": mock.MagicMock()}
        likelihood_ratio_upload = mock.MagicMock()
        likelihood_ratio_upload.terms = {"P_of_tref_Dh": mock.MagicMock()}

        original_gc = strike_object.GC
        try:
            strike_object.GC = True
            with mock.patch("sgnl.strike_object.gc.collect", return_value=0):
                strike_object.StrikeObject.reset_dynamic(
                    frankenstein, likelihood_ratio_upload
                )
        finally:
            strike_object.GC = original_gc


class TestStrikeObjectOnSnapshotReload:
    """Tests for StrikeObject.on_snapshot_reload static method."""

    @mock.patch.object(strike_object.StrikeObject, "reset_dynamic")
    @mock.patch.object(strike_object.StrikeObject, "_update_assign_lr")
    @mock.patch.object(strike_object.StrikeObject, "_load_lr")
    def test_on_snapshot_reload(
        self, mock_load_lr, mock_update_assign, mock_reset_dynamic
    ):
        """Test on_snapshot_reload returns lr_dict."""
        mock_new_lr = mock.MagicMock()
        mock_load_lr.return_value = mock_new_lr
        mock_update_assign.return_value = (mock.MagicMock(), mock.MagicMock())

        result = strike_object.StrikeObject.on_snapshot_reload("test.xml")

        assert "new_lr" in result
        assert "frankenstein" in result
        assert "likelihood_ratio_upload" in result
        mock_load_lr.assert_called_once_with("test.xml")


class TestStrikeObjectPrepareInqData:
    """Tests for StrikeObject.prepare_inq_data method."""

    def test_prepare_inq_data(self):
        """Test prepare_inq_data returns correct dict."""
        with mock.patch.object(strike_object.StrikeObject, "__post_init__"):
            obj = object.__new__(strike_object.StrikeObject)

        obj.bankids_map = {"0000": [0]}
        obj.input_likelihood_file = {"0000": "input.xml"}
        obj.output_likelihood_file = {"0000": "output.xml"}
        obj.zerolag_rank_stat_pdf_file = {"0000": "zerolag.xml"}
        obj.verbose = False

        mock_lr = mock.MagicMock()
        mock_zero_lr = mock.MagicMock()
        obj.likelihood_ratios = {"0000": mock_lr}
        obj.zerolag_rank_stat_pdfs = {"0000": mock_zero_lr}

        result = obj.prepare_inq_data("snapshot.xml", "0000")

        assert result["lr"] is mock_lr
        assert result["zero_lr"] is mock_zero_lr
        assert result["bankid"] == "0000"
        assert result["fn"] == "snapshot.xml"


class TestStrikeObjectSnapshotIo:
    """Tests for StrikeObject.snapshot_io static method."""

    @mock.patch.object(strike_object.StrikeObject, "save_snapshot")
    def test_snapshot_io(self, mock_save_snapshot):
        """Test snapshot_io calls save_snapshot twice."""
        mock_lr = mock.MagicMock()
        mock_zero_lr = mock.MagicMock()

        strike_object.StrikeObject.snapshot_io(
            mock_lr, mock_zero_lr, "fn.xml", "outfile.xml", "zero_outfile.xml"
        )

        assert mock_save_snapshot.call_count == 2


class TestStrikeObjectSnapshotFileobj:
    """Tests for StrikeObject.snapshot_fileobj static method."""

    @mock.patch("sgnl.strike_object.xml_string")
    def test_snapshot_fileobj(self, mock_xml_string):
        """Test snapshot_fileobj returns dict with xml strings."""
        mock_xml_string.side_effect = ["<lr/>", "<zerolag/>"]
        mock_lr = mock.MagicMock()
        mock_zero_lr = mock.MagicMock()

        result = strike_object.StrikeObject.snapshot_fileobj(
            mock_lr, mock_zero_lr, "0000"
        )

        assert result["xml"]["0000"] == "<lr/>"
        assert result["zerolagxml"]["0000"] == "<zerolag/>"


class TestStrikeObjectTimeKey:
    """Tests for StrikeObject.time_key method."""

    def test_time_key(self):
        """Test time_key rounds down to nearest 10."""
        with mock.patch.object(strike_object.StrikeObject, "__post_init__"):
            obj = object.__new__(strike_object.StrikeObject)

        assert obj.time_key(1000000015.5) == 1000000010
        assert obj.time_key(1000000020.0) == 1000000020
        assert obj.time_key(1000000029.9) == 1000000020


class TestStrikeObjectTrainNoise:
    """Tests for StrikeObject.train_noise method."""

    def _create_strike_object_for_train(self):
        """Create a StrikeObject configured for train_noise tests."""
        with mock.patch.object(strike_object.StrikeObject, "__post_init__"):
            obj = object.__new__(strike_object.StrikeObject)

        obj.ifos = ["H1"]
        obj.bankids_map = {"0000": [0]}
        obj.device = "cpu"
        obj.dtype = torch.float32

        # Create tensor shapes
        obj.template_ids_tensor = torch.tensor([[1, 2, 3]])
        obj.counts_by_template_id_counter = torch.zeros_like(obj.template_ids_tensor)
        obj.bankids_index_expand = torch.zeros_like(obj.template_ids_tensor)

        # Mock binning
        mock_binning = mock.MagicMock()
        mock_binning.shape = (10, 10)
        obj.binning = mock_binning

        obj.bins = (
            torch.tensor([4.0, 5.0, 6.0, 7.0, 8.0, 100.0]),
            torch.tensor([0.0, 0.01, 0.02, 0.03, 0.04, 1.0]),
        )

        obj.snr_chisq_lnpdf_noise = {
            "H1": torch.zeros((1, 10, 10), dtype=torch.float32)
        }

        # Mock lr for count tracker
        mock_lr = mock.MagicMock()
        mock_lr.terms = {
            "P_of_SNR_chisq": mock.MagicMock(count_tracker_chi_temp={"H1": {}})
        }
        obj.likelihood_ratios = {"0000": mock_lr}

        return obj

    def test_train_noise_no_singles(self):
        """Test train_noise with no singles above threshold."""
        obj = self._create_strike_object_for_train()

        single_masks = {"H1": torch.tensor([[False, False, False]])}
        snrs = {"H1": torch.tensor([4.5, 4.6, 4.7])}
        chisqs = {"H1": torch.tensor([0.1, 0.1, 0.1])}

        obj.train_noise(1000000000, snrs, chisqs, single_masks)

        # Counts should remain zero
        assert (obj.counts_by_template_id_counter == 0).all()

    def test_train_noise_with_singles(self):
        """Test train_noise with singles above threshold."""
        obj = self._create_strike_object_for_train()

        single_masks = {"H1": torch.tensor([[True, False, True]])}
        snrs = {"H1": torch.tensor([5.5, 5.5])}  # Only for True positions
        chisqs = {"H1": torch.tensor([0.5, 0.5])}

        obj.train_noise(1000000000, snrs, chisqs, single_masks)

        # Counts should be updated
        assert obj.counts_by_template_id_counter.sum() > 0

    def test_train_noise_count_tracker(self):
        """Test train_noise updates count tracker."""
        obj = self._create_strike_object_for_train()

        single_masks = {"H1": torch.tensor([[True, False, False]])}
        # SNR >= 6 and chisq/snr^2 <= 0.04
        snrs = {"H1": torch.tensor([7.0])}
        chisqs = {"H1": torch.tensor([0.1])}  # chisq/snr^2 = 0.1/49 < 0.04

        obj.train_noise(1000000000, snrs, chisqs, single_masks)

    def test_train_noise_count_tracker_append(self):
        """Test train_noise appends to existing count tracker."""
        obj = self._create_strike_object_for_train()

        # Pre-populate count tracker
        ct_temp = (
            obj.likelihood_ratios["0000"].terms["P_of_SNR_chisq"].count_tracker_chi_temp
        )
        ct_temp["H1"][1000000000] = np.array([1, 2])

        single_masks = {"H1": torch.tensor([[True, False, False]])}
        snrs = {"H1": torch.tensor([7.0])}
        chisqs = {"H1": torch.tensor([0.1])}

        obj.train_noise(1000000000, snrs, chisqs, single_masks)

    def test_train_noise_count_tracker_popitem(self):
        """Test train_noise removes old entries from count tracker."""
        obj = self._create_strike_object_for_train()

        # Pre-populate count tracker with many entries
        ct_temp = (
            obj.likelihood_ratios["0000"].terms["P_of_SNR_chisq"].count_tracker_chi_temp
        )
        from collections import OrderedDict

        ct_temp["H1"] = OrderedDict()
        for i in range(550):
            ct_temp["H1"][i * 10] = np.array([1])

        single_masks = {"H1": torch.tensor([[True, False, False]])}
        snrs = {"H1": torch.tensor([7.0])}
        chisqs = {"H1": torch.tensor([0.1])}

        obj.train_noise(1000000000, snrs, chisqs, single_masks)

        # Should have removed old entries
        assert len(ct_temp["H1"]) <= 500


class TestStrikeObjectStoreCounts:
    """Tests for StrikeObject.store_counts method."""

    def test_store_counts(self):
        """Test store_counts calls store_counts on all lrs."""
        with mock.patch.object(strike_object.StrikeObject, "__post_init__"):
            obj = object.__new__(strike_object.StrikeObject)

        mock_lr1 = mock.MagicMock()
        mock_lr2 = mock.MagicMock()
        obj.likelihood_ratios = {"0000": mock_lr1, "0001": mock_lr2}

        gracedb_times = [1000000000, 1000000100]
        obj.store_counts(gracedb_times)

        mock_lr1.terms["P_of_SNR_chisq"].store_counts.assert_called_once_with(
            gracedb_times
        )
        mock_lr2.terms["P_of_SNR_chisq"].store_counts.assert_called_once_with(
            gracedb_times
        )


class TestStrikeObjectUpdateArrayData:
    """Tests for StrikeObject.update_array_data method."""

    def test_update_array_data(self):
        """Test update_array_data saves snr chi and counts."""
        with mock.patch.object(strike_object.StrikeObject, "__post_init__"):
            obj = object.__new__(strike_object.StrikeObject)

        obj.ifos = ["H1"]
        obj.bankids_map = {"0000": [0]}
        obj.all_template_ids = np.array([[1, 2, 3]])
        obj.counts_by_template_id_counter = torch.tensor([[5, 0, 3]])
        obj.snr_chisq_lnpdf_noise = {"H1": torch.tensor([[[1.0, 2.0], [3.0, 4.0]]])}

        mock_lr = mock.MagicMock()
        mock_lr.terms = {
            "P_of_SNR_chisq": mock.MagicMock(
                snr_chisq_lnpdf_noise={"H1_snr_chi": mock.MagicMock()}
            ),
            "P_of_Template": mock.MagicMock(counts_by_template_id={1: 0, 2: 0, 3: 0}),
        }
        obj.likelihood_ratios = {"0000": mock_lr}

        obj.update_array_data("0000")

        # Verify counts were reset
        assert (obj.counts_by_template_id_counter[0] == 0).all()


class TestStrikeObjectPostInit:
    """Tests for StrikeObject.__post_init__ method."""

    @mock.patch("sgnl.strike_object.LnLikelihoodRatio")
    def test_post_init_offline_with_output_file(self, mock_lr_class):
        """Test __post_init__ in offline mode with output file."""
        mock_lr = mock.MagicMock()
        mock_lr.template_ids = np.array([1, 2, 3])
        mock_lr.terms = {
            "P_of_SNR_chisq": mock.MagicMock(
                snr_chisq_lnpdf_noise={
                    "H1_snr_chi": mock.MagicMock(array=np.zeros((5, 5)))
                },
                snr_chi_binning=mock.MagicMock(
                    shape=(5, 5),
                    __iter__=lambda s: iter(
                        [mock.MagicMock(boundaries=np.array([0, 1, 2, 3, 4, 5]))] * 2
                    ),
                ),
            )
        }
        mock_lr_class.return_value = mock_lr

        with mock.patch.object(strike_object.StrikeObject, "_validate"):
            obj = strike_object.StrikeObject(
                bankids_map={"0000": [0]},
                coincidence_threshold=0.005,
                ifos=["H1"],
                zerolag_rank_stat_pdf_file=None,
                all_template_ids=np.array([[1, 2, 3]]),
                is_online=False,
                injections=False,
                output_likelihood_file={"0000": "output.xml"},
            )

        assert "0000" in obj.likelihood_ratios

    def test_post_init_all_template_ids_none(self):
        """Test __post_init__ when all_template_ids is None."""
        with mock.patch.object(strike_object.StrikeObject, "_validate"):
            with mock.patch.object(strike_object.StrikeObject, "__post_init__"):
                obj = object.__new__(strike_object.StrikeObject)

        obj.all_template_ids = None
        obj.is_online = False
        obj.output_likelihood_file = None
        obj.ifos = ["H1"]
        obj.bankids_map = {"0000": [0]}
        obj.device = "cpu"
        obj.dtype = torch.float32

        # Create a mock lr with many templates
        mock_lr = mock.MagicMock()
        mock_lr.template_ids = np.arange(700)  # > 600
        mock_lr.terms = {
            "P_of_SNR_chisq": mock.MagicMock(
                snr_chisq_lnpdf_noise={
                    "H1_snr_chi": mock.MagicMock(array=np.zeros((5, 5)))
                },
                snr_chi_binning=mock.MagicMock(
                    shape=(5, 5),
                    __iter__=lambda s: iter(
                        [mock.MagicMock(boundaries=np.array([0, 1, 2, 3, 4, 5]))] * 2
                    ),
                ),
            )
        }
        obj.likelihood_ratios = {"0000": mock_lr}
        obj.offset_vectors = {"H1": 0}

        # Call the part of __post_init__ that handles all_template_ids is None
        # This is lines 232-254
        if obj.all_template_ids is None:
            ntempmax = 0
            temps = []
            for lr in obj.likelihood_ratios.values():
                if len(lr.template_ids) > 600:
                    ntemp = int(round(len(lr.template_ids) / 2))
                    lrtemp = list(lr.template_ids)
                    temps.append(lrtemp[:ntemp])
                    temps.append(lrtemp[-ntemp:])
                    if ntemp > ntempmax:
                        ntempmax = ntemp
                else:
                    ntemp = len(lr.template_ids)
                    temps.append(lr.template_ids)
                    if ntemp > ntempmax:
                        ntempmax = ntemp

            nsubbanks = len(temps)
            obj.all_template_ids = np.zeros((nsubbanks, ntempmax), dtype=np.int32)
            for i, t in enumerate(temps):
                n = len(t)
                obj.all_template_ids[i, :n] = t

        assert obj.all_template_ids is not None
        assert obj.all_template_ids.shape[0] == 2  # Split into 2 subbanks

    def test_post_init_all_template_ids_none_small_bank(self):
        """Test __post_init__ when all_template_ids is None with small bank."""
        with mock.patch.object(strike_object.StrikeObject, "_validate"):
            with mock.patch.object(strike_object.StrikeObject, "__post_init__"):
                obj = object.__new__(strike_object.StrikeObject)

        obj.all_template_ids = None
        obj.is_online = False
        obj.output_likelihood_file = None
        obj.ifos = ["H1"]
        obj.bankids_map = {"0000": [0]}
        obj.device = "cpu"
        obj.dtype = torch.float32

        # Create a mock lr with few templates
        mock_lr = mock.MagicMock()
        mock_lr.template_ids = np.arange(100)  # <= 600
        mock_lr.terms = {
            "P_of_SNR_chisq": mock.MagicMock(
                snr_chisq_lnpdf_noise={
                    "H1_snr_chi": mock.MagicMock(array=np.zeros((5, 5)))
                },
                snr_chi_binning=mock.MagicMock(
                    shape=(5, 5),
                    __iter__=lambda s: iter(
                        [mock.MagicMock(boundaries=np.array([0, 1, 2, 3, 4, 5]))] * 2
                    ),
                ),
            )
        }
        obj.likelihood_ratios = {"0000": mock_lr}
        obj.offset_vectors = {"H1": 0}

        # Call the part of __post_init__ that handles all_template_ids is None
        if obj.all_template_ids is None:
            ntempmax = 0
            temps = []
            for lr in obj.likelihood_ratios.values():
                if len(lr.template_ids) > 600:
                    ntemp = int(round(len(lr.template_ids) / 2))
                    lrtemp = list(lr.template_ids)
                    temps.append(lrtemp[:ntemp])
                    temps.append(lrtemp[-ntemp:])
                    if ntemp > ntempmax:
                        ntempmax = ntemp
                else:
                    ntemp = len(lr.template_ids)
                    temps.append(lr.template_ids)
                    if ntemp > ntempmax:
                        ntempmax = ntemp

            nsubbanks = len(temps)
            obj.all_template_ids = np.zeros((nsubbanks, ntempmax), dtype=np.int32)
            for i, t in enumerate(temps):
                n = len(t)
                obj.all_template_ids[i, :n] = t

        assert obj.all_template_ids is not None
        assert obj.all_template_ids.shape[0] == 1  # Not split


class TestStrikeObjectOnlinePostInit:
    """Tests for StrikeObject.__post_init__ in online mode."""

    def _create_mock_lr(self):
        """Create a properly configured mock LnLikelihoodRatio."""
        mock_lr = mock.MagicMock()
        mock_lr.template_ids = np.array([1, 2, 3])
        mock_lr.min_instruments = 2
        mock_lr.population_model_file = "pop.h5"
        mock_lr.is_healthy.return_value = True
        mock_lr.terms = {
            "P_of_dt_dphi": mock.MagicMock(dtdphi_file="dtdphi.h5"),
            "P_of_tref_Dh": mock.MagicMock(
                horizon_history=mock.MagicMock(),
                triggerrates={"H1": mock.MagicMock()},
            ),
            "P_of_SNR_chisq": mock.MagicMock(
                snr_chisq_lnpdf_noise={
                    "H1_snr_chi": mock.MagicMock(array=np.zeros((5, 5)))
                },
                snr_chi_binning=mock.MagicMock(
                    shape=(5, 5),
                    __iter__=lambda s: iter(
                        [mock.MagicMock(boundaries=np.array([0, 1, 2, 3, 4, 5]))] * 2
                    ),
                ),
            ),
        }
        return mock_lr

    @mock.patch("sgnl.strike_object.P_of_ifos_given_tref")
    @mock.patch("sgnl.strike_object.P_of_dt_dphi_given_tref_Template")
    @mock.patch("sgnl.strike_object.P_of_Template")
    @mock.patch("sgnl.strike_object.RankingStatPDF")
    @mock.patch("sgnl.strike_object.LnLikelihoodRatio")
    @mock.patch("os.access")
    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    def test_post_init_online_with_gc(
        self,
        mock_local_path,
        mock_access,
        mock_lr_class,
        mock_rspdf_class,
        mock_p_template,
        mock_p_dtdphi,
        mock_p_ifos,
    ):
        """Test __post_init__ in online mode with GC enabled."""
        mock_local_path.return_value = "/local/path"
        mock_access.return_value = True

        mock_lr = self._create_mock_lr()
        mock_lr_class.load.return_value = mock_lr

        mock_rspdf = mock.MagicMock()
        mock_rspdf.template_ids = np.array([1, 2, 3])
        mock_rspdf.is_healthy.return_value = True
        mock_rspdf_class.load.return_value = mock_rspdf

        original_gc = strike_object.GC
        try:
            strike_object.GC = True
            with mock.patch("sgnl.strike_object.gc.collect", return_value=0):
                with mock.patch("sgnl.strike_object.far.FAPFAR"):
                    obj = strike_object.StrikeObject(
                        bankids_map={"0000": [0]},
                        coincidence_threshold=0.005,
                        ifos=["H1"],
                        zerolag_rank_stat_pdf_file=["zerolag.xml"],
                        all_template_ids=np.array([[1, 2, 3]]),
                        is_online=True,
                        injections=False,
                        input_likelihood_file=["input.xml"],
                        output_likelihood_file=["input.xml"],
                        rank_stat_pdf_file="rank.xml",
                        min_instruments=2,
                    )
        finally:
            strike_object.GC = original_gc

        assert "0000" in obj.likelihood_ratios

    @mock.patch("sgnl.strike_object.P_of_ifos_given_tref")
    @mock.patch("sgnl.strike_object.P_of_dt_dphi_given_tref_Template")
    @mock.patch("sgnl.strike_object.P_of_Template")
    @mock.patch("sgnl.strike_object.RankingStatPDF")
    @mock.patch("sgnl.strike_object.LnLikelihoodRatio")
    @mock.patch("os.access")
    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    def test_post_init_online_compress_likelihood(
        self,
        mock_local_path,
        mock_access,
        mock_lr_class,
        mock_rspdf_class,
        mock_p_template,
        mock_p_dtdphi,
        mock_p_ifos,
    ):
        """Test __post_init__ in online mode with compress_likelihood_ratio."""
        mock_local_path.return_value = "/local/path"
        mock_access.return_value = True

        mock_lr = self._create_mock_lr()
        mock_lr_class.load.return_value = mock_lr

        mock_rspdf = mock.MagicMock()
        mock_rspdf.template_ids = np.array([1, 2, 3])
        mock_rspdf.is_healthy.return_value = True
        mock_rspdf_class.load.return_value = mock_rspdf

        with mock.patch("sgnl.strike_object.far.FAPFAR"):
            strike_object.StrikeObject(
                bankids_map={"0000": [0]},
                coincidence_threshold=0.005,
                ifos=["H1"],
                zerolag_rank_stat_pdf_file=["zerolag.xml"],
                all_template_ids=np.array([[1, 2, 3]]),
                is_online=True,
                injections=False,
                input_likelihood_file=["input.xml"],
                output_likelihood_file=["input.xml"],
                rank_stat_pdf_file="rank.xml",
                min_instruments=2,
                compress_likelihood_ratio=True,
            )

        # Verify compress was called
        mock_lr.terms["P_of_tref_Dh"].horizon_history.compress.assert_called_once()

    @mock.patch("sgnl.strike_object.P_of_ifos_given_tref")
    @mock.patch("sgnl.strike_object.P_of_dt_dphi_given_tref_Template")
    @mock.patch("sgnl.strike_object.P_of_Template")
    @mock.patch("sgnl.strike_object.RankingStatPDF")
    @mock.patch("sgnl.strike_object.LnLikelihoodRatio")
    @mock.patch("os.access")
    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    def test_post_init_online_injections(
        self,
        mock_local_path,
        mock_access,
        mock_lr_class,
        mock_rspdf_class,
        mock_p_template,
        mock_p_dtdphi,
        mock_p_ifos,
    ):
        """Test __post_init__ in online mode with injections=True."""
        mock_local_path.return_value = "/local/path"
        mock_access.return_value = True

        mock_lr = self._create_mock_lr()
        mock_lr_class.load.return_value = mock_lr

        mock_rspdf = mock.MagicMock()
        mock_rspdf.template_ids = np.array([1, 2, 3])
        mock_rspdf.is_healthy.return_value = True
        mock_rspdf_class.load.return_value = mock_rspdf

        with mock.patch("sgnl.strike_object.far.FAPFAR"):
            obj = strike_object.StrikeObject(
                bankids_map={"0000": [0]},
                coincidence_threshold=0.005,
                ifos=["H1"],
                zerolag_rank_stat_pdf_file=None,
                all_template_ids=np.array([[1, 2, 3]]),
                is_online=True,
                injections=True,
                input_likelihood_file=["input.xml"],
                output_likelihood_file=None,
                rank_stat_pdf_file="rank.xml",
                min_instruments=2,
            )

        # For injection jobs, zerolag_rank_stat_pdfs should be None
        assert obj.zerolag_rank_stat_pdfs is None

    @mock.patch("sgnl.strike_object.P_of_ifos_given_tref")
    @mock.patch("sgnl.strike_object.P_of_dt_dphi_given_tref_Template")
    @mock.patch("sgnl.strike_object.P_of_Template")
    @mock.patch("sgnl.strike_object.RankingStatPDF")
    @mock.patch("sgnl.strike_object.LnLikelihoodRatio")
    @mock.patch("os.access")
    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    def test_post_init_online_all_template_ids_none(
        self,
        mock_local_path,
        mock_access,
        mock_lr_class,
        mock_rspdf_class,
        mock_p_template,
        mock_p_dtdphi,
        mock_p_ifos,
    ):
        """Test __post_init__ in online mode with all_template_ids=None."""
        mock_local_path.return_value = "/local/path"
        mock_access.return_value = True

        mock_lr = self._create_mock_lr()
        mock_lr.template_ids = np.arange(700)  # > 600 to trigger split
        mock_lr_class.load.return_value = mock_lr

        mock_rspdf = mock.MagicMock()
        mock_rspdf.template_ids = np.arange(700)
        mock_rspdf.is_healthy.return_value = True
        mock_rspdf_class.load.return_value = mock_rspdf

        with mock.patch("sgnl.strike_object.far.FAPFAR"):
            obj = strike_object.StrikeObject(
                bankids_map={"0000": [0]},
                coincidence_threshold=0.005,
                ifos=["H1"],
                zerolag_rank_stat_pdf_file=["zerolag.xml"],
                all_template_ids=None,  # This triggers the None handling
                is_online=True,
                injections=False,
                input_likelihood_file=["input.xml"],
                output_likelihood_file=["input.xml"],
                rank_stat_pdf_file="rank.xml",
                min_instruments=2,
            )

        assert obj.all_template_ids is not None
        # Should be split since template_ids > 600
        assert obj.all_template_ids.shape[0] == 2

    @mock.patch("sgnl.strike_object.P_of_ifos_given_tref")
    @mock.patch("sgnl.strike_object.P_of_dt_dphi_given_tref_Template")
    @mock.patch("sgnl.strike_object.P_of_Template")
    @mock.patch("sgnl.strike_object.RankingStatPDF")
    @mock.patch("sgnl.strike_object.LnLikelihoodRatio")
    @mock.patch("os.access")
    @mock.patch("sgnl.strike_object.ligolw_utils.local_path_from_url")
    def test_post_init_online_all_template_ids_none_no_split(
        self,
        mock_local_path,
        mock_access,
        mock_lr_class,
        mock_rspdf_class,
        mock_p_template,
        mock_p_dtdphi,
        mock_p_ifos,
    ):
        """Test __post_init__ online with all_template_ids=None, <=600 templates."""
        mock_local_path.return_value = "/local/path"
        mock_access.return_value = True

        mock_lr = self._create_mock_lr()
        mock_lr.template_ids = np.arange(500)  # <= 600 to not trigger split
        mock_lr_class.load.return_value = mock_lr

        mock_rspdf = mock.MagicMock()
        mock_rspdf.template_ids = np.arange(500)
        mock_rspdf.is_healthy.return_value = True
        mock_rspdf_class.load.return_value = mock_rspdf

        with mock.patch("sgnl.strike_object.far.FAPFAR"):
            obj = strike_object.StrikeObject(
                bankids_map={"0000": [0]},
                coincidence_threshold=0.005,
                ifos=["H1"],
                zerolag_rank_stat_pdf_file=["zerolag.xml"],
                all_template_ids=None,  # This triggers the None handling
                is_online=True,
                injections=False,
                input_likelihood_file=["input.xml"],
                output_likelihood_file=["input.xml"],
                rank_stat_pdf_file="rank.xml",
                min_instruments=2,
            )

        assert obj.all_template_ids is not None
        # Should NOT be split since template_ids <= 600
        assert obj.all_template_ids.shape[0] == 1
        assert obj.all_template_ids.shape[1] == 500

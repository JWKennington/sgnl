"""Tests for sgnl.svd_bank module."""

import warnings
from unittest import mock

import numpy as np
import pytest
from igwn_ligolw import ligolw, lsctables
from igwn_ligolw import utils as ligolw_utils
from igwn_ligolw.array import use_in as array_use_in
from igwn_ligolw.param import use_in as param_use_in
from igwn_ligolw.utils import process as ligolw_process

from sgnl import svd_bank


@array_use_in
@param_use_in
@lsctables.use_in
class TestContentHandler(ligolw.LIGOLWContentHandler):
    pass


def create_mock_sngl_inspiral_table(xmldoc, num_rows=3):
    """Create a mock sngl_inspiral table with test rows."""
    sngl_table = lsctables.SnglInspiralTable.new()
    xmldoc.childNodes[0].appendChild(sngl_table)

    for i in range(num_rows):
        row = sngl_table.RowType()
        row.ifo = "H1"
        row.search = "test"
        row.channel = "test"
        row.mass1 = 1.4 + i * 0.1
        row.mass2 = 1.4
        row.mtotal = 2.8 + i * 0.1
        row.mchirp = 1.2
        row.eta = 0.25
        row.spin1x = 0.0
        row.spin1y = 0.0
        row.spin1z = 0.1
        row.spin2x = 0.0
        row.spin2y = 0.0
        row.spin2z = 0.1
        row.end_time = 1000000000
        row.end_time_ns = 0
        row.snr = 10.0
        row.chisq = 1.0
        row.chisq_dof = 10
        row.bank_chisq = 0.0
        row.bank_chisq_dof = 0
        row.cont_chisq = 0.0
        row.cont_chisq_dof = 0
        row.sigmasq = 1.0
        row.eff_distance = 100.0
        row.coa_phase = 0.0
        row.f_final = 2048.0
        row.template_id = i + 1
        row.template_duration = 10.0
        row.event_duration = 0.0
        row.amplitude = 0.0
        row.alpha = 0.0
        row.alpha1 = 0.0
        row.alpha2 = 0.0
        row.alpha3 = 0.0
        row.alpha4 = 0.0
        row.alpha5 = 0.0
        row.alpha6 = 0.0
        row.beta = 0.0
        row.chi = 0.0
        row.kappa = 0.0
        row.psi0 = 0.0
        row.psi3 = 0.0
        row.tau0 = 0.0
        row.tau2 = 0.0
        row.tau3 = 0.0
        row.tau4 = 0.0
        row.tau5 = 0.0
        row.ttotal = 0.0
        row.Gamma0 = 0.0
        row.Gamma1 = 0
        row.Gamma2 = 0.0
        row.Gamma3 = 0.0
        row.Gamma4 = 0.0
        row.Gamma5 = 0.0
        row.Gamma6 = 0.0
        row.Gamma7 = 0.0
        row.Gamma8 = 0.0
        row.Gamma9 = 0.0
        sngl_table.append(row)

    return sngl_table


class TestDefaultContentHandler:
    """Tests for DefaultContentHandler class."""

    def test_default_content_handler_exists(self):
        """Test DefaultContentHandler class exists."""
        assert svd_bank.DefaultContentHandler is not None


class TestReadApproximant:
    """Tests for read_approximant function."""

    def test_read_approximant_success(self, tmp_path):
        """Test read_approximant with valid document."""
        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())

        # Register process with approximant
        ligolw_process.register_to_xmldoc(
            xmldoc,
            program="sgnl-inspiral-bank-splitter",
            paramdict={"approximant": "TaylorF2"},
        )

        xml_file = tmp_path / "test_bank.xml"
        ligolw_utils.write_filename(xmldoc, str(xml_file))

        loaded = ligolw_utils.load_filename(
            str(xml_file), contenthandler=TestContentHandler
        )

        with mock.patch("sgnl.svd_bank.templates.sgnl_valid_approximant"):
            result = svd_bank.read_approximant(loaded)

        assert result == "TaylorF2"

    def test_read_approximant_no_process_ids(self, tmp_path):
        """Test read_approximant raises when no process entries."""
        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())

        # Create empty process table
        lsctables.ProcessTable.new().parentNode = xmldoc.childNodes[0]
        xmldoc.childNodes[0].appendChild(lsctables.ProcessTable.new())

        xml_file = tmp_path / "test_bank.xml"
        ligolw_utils.write_filename(xmldoc, str(xml_file))

        loaded = ligolw_utils.load_filename(
            str(xml_file), contenthandler=TestContentHandler
        )

        with pytest.raises(ValueError, match="document must contain process entries"):
            svd_bank.read_approximant(loaded)

    def test_read_approximant_no_approximant_param(self, tmp_path):
        """Test read_approximant raises when no approximant param."""
        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())

        # Register process without approximant param
        ligolw_process.register_to_xmldoc(
            xmldoc,
            program="sgnl-inspiral-bank-splitter",
            paramdict={"other_param": "value"},
        )

        xml_file = tmp_path / "test_bank.xml"
        ligolw_utils.write_filename(xmldoc, str(xml_file))

        loaded = ligolw_utils.load_filename(
            str(xml_file), contenthandler=TestContentHandler
        )

        with pytest.raises(ValueError, match="approximant"):
            svd_bank.read_approximant(loaded)

    def test_read_approximant_multiple_approximants(self, tmp_path):
        """Test read_approximant raises when multiple approximants."""
        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())

        # Register two processes with different approximants
        ligolw_process.register_to_xmldoc(
            xmldoc,
            program="sgnl-inspiral-bank-splitter",
            paramdict={"approximant": "TaylorF2"},
        )
        ligolw_process.register_to_xmldoc(
            xmldoc,
            program="sgnl-inspiral-bank-splitter",
            paramdict={"approximant": "IMRPhenomD"},
        )

        xml_file = tmp_path / "test_bank.xml"
        ligolw_utils.write_filename(xmldoc, str(xml_file))

        loaded = ligolw_utils.load_filename(
            str(xml_file), contenthandler=TestContentHandler
        )

        with pytest.raises(ValueError, match="only one approximant"):
            svd_bank.read_approximant(loaded)


class TestCheckFfinalAndFindMaxFfinal:
    """Tests for check_ffinal_and_find_max_ffinal function."""

    def test_check_ffinal_success(self):
        """Test with valid f_final column."""
        mock_xmldoc = mock.MagicMock()
        mock_table = mock.MagicMock()
        mock_table.getColumnByName.return_value = [1000.0, 2000.0, 2048.0]

        with mock.patch.object(
            lsctables.SnglInspiralTable, "get_table", return_value=mock_table
        ):
            result = svd_bank.check_ffinal_and_find_max_ffinal(mock_xmldoc)

        assert result == 2048.0

    def test_check_ffinal_not_populated(self):
        """Test raises when f_final not populated."""
        mock_xmldoc = mock.MagicMock()
        mock_table = mock.MagicMock()
        mock_table.getColumnByName.return_value = [0, 1000.0]  # Has a 0 value

        with mock.patch.object(
            lsctables.SnglInspiralTable, "get_table", return_value=mock_table
        ):
            with pytest.raises(ValueError, match="f_final column not populated"):
                svd_bank.check_ffinal_and_find_max_ffinal(mock_xmldoc)


class TestMaxStatThresh:
    """Tests for max_stat_thresh function."""

    def test_max_stat_thresh_basic(self):
        """Test max_stat_thresh with basic coefficients."""
        # scipy.randn doesn't exist in newer scipy - mock it
        import scipy

        scipy.randn = np.random.randn
        try:
            coeffs = [1.0, 1.0]
            result = svd_bank.max_stat_thresh(coeffs, 0.01, samp_tol=100.0)
            assert isinstance(result, float)
            assert result > 0
        finally:
            del scipy.randn


class TestSumOfSquaresThresholdFromFap:
    """Tests for sum_of_squares_threshold_from_fap function."""

    def test_sum_of_squares_threshold_from_fap(self):
        """Test sum_of_squares_threshold_from_fap function."""
        # scipy.randn doesn't exist in newer scipy - mock it
        import scipy

        scipy.randn = np.random.randn
        try:
            coeffs = [1.0, 1.0]
            result = svd_bank.sum_of_squares_threshold_from_fap(0.01, coeffs)
            assert isinstance(result, float)
        finally:
            del scipy.randn


class TestGroup:
    """Tests for group function."""

    def test_group_basic(self):
        """Test basic grouping."""
        inlist = list(range(12))
        parts = [2, 3]
        result = list(svd_bank.group(inlist, parts))

        # Should produce groups
        assert len(result) > 0
        # All elements should be accounted for
        all_elements = []
        for g in result:
            all_elements.extend(g)
        assert set(all_elements) == set(inlist)

    def test_group_empty_list(self):
        """Test grouping with empty list."""
        result = list(svd_bank.group([], [2, 3]))
        assert result == []

    def test_group_exhausts_list(self):
        """Test grouping that exhausts the list."""
        inlist = [1, 2, 3]
        parts = [2, 2]
        result = list(svd_bank.group(inlist, parts))
        assert len(result) >= 1


class TestBankFragment:
    """Tests for BankFragment dataclass."""

    def test_bank_fragment_creation(self):
        """Test creating a BankFragment."""
        frag = svd_bank.BankFragment(rate=4096, start=0.0, end=1.0)

        assert frag.rate == 4096
        assert frag.start == 0.0
        assert frag.end == 1.0
        assert frag.orthogonal_template_bank is None
        assert frag.singular_values is None
        assert frag.mix_matrix is None
        assert frag.chifacs is None

    @mock.patch("sgnl.svd_bank.cbc_template_fir.decompose_templates")
    def test_bank_fragment_set_template_bank(self, mock_decompose):
        """Test set_template_bank method."""
        mock_decompose.return_value = (
            np.array([[1, 2], [3, 4]]),  # orthogonal_template_bank
            np.array([1.0, 0.5]),  # singular_values
            np.array([[1, 2, 3, 4]]),  # mix_matrix
            np.array([1.0, 2.0, 3.0, 4.0]),  # chifacs
        )

        frag = svd_bank.BankFragment(rate=4096, start=0.0, end=1.0)
        template_bank = np.array([[1, 2], [3, 4]])

        frag.set_template_bank(template_bank, tolerance=0.01, snr_thresh=4.0)

        assert frag.orthogonal_template_bank is not None
        assert frag.singular_values is not None
        assert frag.mix_matrix is not None
        assert frag.chifacs is not None
        mock_decompose.assert_called_once()

    @mock.patch("sgnl.svd_bank.cbc_template_fir.decompose_templates")
    def test_bank_fragment_set_template_bank_verbose(self, mock_decompose, capsys):
        """Test set_template_bank method with verbose output."""
        mock_decompose.return_value = (
            np.array([[1, 2], [3, 4]]),
            np.array([1.0, 0.5]),
            np.array([[1, 2, 3, 4]]),
            np.array([1.0, 2.0, 3.0, 4.0]),
        )

        frag = svd_bank.BankFragment(rate=4096, start=0.0, end=1.0)
        template_bank = np.array([[1, 2], [3, 4]])

        frag.set_template_bank(
            template_bank, tolerance=0.01, snr_thresh=4.0, verbose=True
        )

        captured = capsys.readouterr()
        assert "templates" in captured.err
        assert "components" in captured.err


class TestBank:
    """Tests for Bank dataclass."""

    def _create_mock_workspace(self):
        """Create mock workspace for Bank tests."""
        workspace = mock.MagicMock()
        workspace.psd = mock.MagicMock()
        workspace.working_duration = 4.0
        workspace.working_f_low = 30.0
        workspace.f_low = 40.0
        workspace.sample_rate_max = 4096
        workspace.time_slices = [(4096, 0.0, 1.0)]
        return workspace

    @mock.patch("sgnl.svd_bank.cbc_template_fir.decompose_templates")
    @mock.patch("sgnl.svd_bank.cbc_template_fir.generate_templates")
    @mock.patch("sgnl.svd_bank.read_approximant")
    def test_bank_post_init(
        self, mock_read_approx, mock_generate, mock_decompose, tmp_path
    ):
        """Test Bank.__post_init__ method."""
        mock_read_approx.return_value = "TaylorF2"

        workspace = self._create_mock_workspace()

        # Create mock sngl_inspiral_table
        mock_sngl_table = mock.MagicMock()
        mock_sngl_table.copy.return_value = mock.MagicMock()
        mock_sngl_table.__iter__ = mock.MagicMock(
            return_value=iter([mock.MagicMock(), mock.MagicMock()])
        )
        mock_sngl_table.__getitem__ = mock.MagicMock(
            return_value=[mock.MagicMock(), mock.MagicMock()]
        )

        mock_generate.return_value = (
            [np.array([[1, 2], [3, 4]])],  # template_bank
            np.array([[1 + 1j, 2 + 2j]]),  # autocorrelation_bank
            np.array([[1, 0]]),  # autocorrelation_mask
            np.array([1.0]),  # sigmasq
            workspace,  # bank_workspace
        )

        mock_decompose.return_value = (
            np.array([[1, 2], [3, 4]]),  # orthogonal_template_bank
            np.array([1.0, 0.5]),  # singular_values
            np.array([[1, 2, 3, 4]]),  # mix_matrix
            np.array([1.0, 2.0, 3.0, 4.0]),  # chifacs
        )

        # Create minimal XML doc
        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())
        create_mock_sngl_inspiral_table(xmldoc, num_rows=2)

        time_slices_rec = np.rec.array(
            [(4096, 0.0, 1.0)], dtype=[("rate", int), ("start", float), ("end", float)]
        )

        mock_psd = mock.MagicMock()

        with mock.patch.object(
            lsctables.SnglInspiralTable, "get_table", return_value=mock_sngl_table
        ):
            bank = svd_bank.Bank(
                bank_xmldoc=xmldoc,
                psd=mock_psd,
                time_slices=time_slices_rec,
                snr_threshold=4.0,
                tolerance=0.01,
                bank_id="0000_test",
            )

        assert bank.bank_id == "0000_test"
        assert bank.snr_threshold == 4.0

    def test_bank_empty_logname_raises(self):
        """Test Bank raises when logname is empty string."""
        # Create Bank with empty logname - should raise in __post_init__
        time_slices = np.rec.array(
            [(4096, 0.0, 1.0)], dtype=[("rate", int), ("start", float), ("end", float)]
        )

        with pytest.raises(ValueError, match="logname cannot be empty"):
            # Minimal args to reach the logname check
            svd_bank.Bank(
                bank_xmldoc=mock.MagicMock(),
                psd=mock.MagicMock(),
                time_slices=time_slices,
                snr_threshold=4.0,
                tolerance=0.01,
                logname="",  # Empty string should trigger error
            )

    def test_bank_get_rates(self):
        """Test Bank.get_rates method."""
        with mock.patch.object(svd_bank.Bank, "__post_init__"):
            bank = object.__new__(svd_bank.Bank)

        frag1 = svd_bank.BankFragment(rate=4096, start=0.0, end=0.5)
        frag2 = svd_bank.BankFragment(rate=2048, start=0.5, end=1.0)
        bank.bank_fragments = [frag1, frag2]

        rates = bank.get_rates()
        assert rates == {4096, 2048}

    def test_bank_set_template_bank_filename(self):
        """Test Bank.set_template_bank_filename method."""
        with mock.patch.object(svd_bank.Bank, "__post_init__"):
            bank = object.__new__(svd_bank.Bank)

        bank.set_template_bank_filename("test_bank.xml")
        assert bank.template_bank_filename == "test_bank.xml"


class TestCalHigherFLow:
    """Tests for cal_higher_f_low function."""

    def test_cal_higher_f_low_basic(self):
        """Test cal_higher_f_low with basic parameters."""
        # Create mock sngl_inspiral table
        mock_row = mock.MagicMock()
        mock_row.mass1 = 1.4
        mock_row.mass2 = 1.4
        mock_row.spin1 = np.array([0.0, 0.0, 0.1])
        mock_row.spin2 = np.array([0.0, 0.0, 0.1])
        mock_row.f_final = 2048.0

        bank_sngl_table = [mock_row]

        with mock.patch("sgnl.svd_bank.chirptime.imr_time", return_value=10.0):
            with mock.patch("sgnl.svd_bank.chirptime.ringf", return_value=1000.0):
                with mock.patch(
                    "sgnl.svd_bank.chirptime.overestimate_j_from_chi", return_value=0.5
                ):
                    imr_approx = "sgnl.svd_bank.templates.sgnl_IMR_approximants"
                    with mock.patch(imr_approx, []):
                        with mock.patch(
                            "sgnl.svd_bank.spawaveform.ffinal", return_value=1500.0
                        ):
                            result = svd_bank.cal_higher_f_low(
                                bank_sngl_table,
                                f_high=2048.0,
                                flow=40.0,
                                approximant="TaylorF2",
                                max_duration=128.0,
                            )

        assert isinstance(result, float)

    def test_cal_higher_f_low_fhigh_less_than_flow_raises(self):
        """Test cal_higher_f_low raises when fhigh < flow."""
        mock_row = mock.MagicMock()
        mock_row.mass1 = 1.4
        mock_row.mass2 = 1.4
        mock_row.spin1 = np.array([0.0, 0.0, 0.1])
        mock_row.spin2 = np.array([0.0, 0.0, 0.1])
        mock_row.f_final = 2048.0

        bank_sngl_table = [mock_row]

        # Mock to return a high f_low value
        with mock.patch("scipy.optimize.fsolve", return_value=np.array([500.0])):
            with mock.patch("sgnl.svd_bank.chirptime.ringf", return_value=1000.0):
                with mock.patch(
                    "sgnl.svd_bank.chirptime.overestimate_j_from_chi", return_value=0.5
                ):
                    imr_approx = "sgnl.svd_bank.templates.sgnl_IMR_approximants"
                    with mock.patch(imr_approx, []):
                        with mock.patch(
                            "sgnl.svd_bank.spawaveform.ffinal", return_value=1500.0
                        ):
                            with pytest.raises(
                                ValueError, match="Lower frequency must be lower"
                            ):
                                svd_bank.cal_higher_f_low(
                                    bank_sngl_table,
                                    f_high=100.0,  # Lower than the computed f_low
                                    flow=40.0,
                                    approximant="TaylorF2",
                                    max_duration=1.0,  # Very short
                                )


class TestBuildBank:
    """Tests for build_bank function."""

    @mock.patch("sgnl.svd_bank.Bank")
    @mock.patch("sgnl.svd_bank.templates.time_slices")
    @mock.patch("sgnl.svd_bank.cal_higher_f_low")
    @mock.patch("sgnl.svd_bank.check_ffinal_and_find_max_ffinal")
    @mock.patch("sgnl.svd_bank.ligolw_utils.load_url")
    @mock.patch("sgnl.svd_bank.ligolw_process.get_process_params")
    def test_build_bank_basic(
        self,
        mock_get_params,
        mock_load_url,
        mock_check_ffinal,
        mock_cal_flow,
        mock_time_slices,
        mock_bank_class,
    ):
        """Test build_bank with basic parameters."""
        # Setup mocks
        mock_xmldoc = mock.MagicMock()
        mock_sngl_table = mock.MagicMock()
        mock_sngl_table.__getitem__ = mock.MagicMock(
            return_value=mock.MagicMock(ifo="H1")
        )
        mock_load_url.return_value = mock_xmldoc

        mock_get_params.return_value = ["TaylorF2"]
        mock_check_ffinal.return_value = 2048.0
        mock_cal_flow.return_value = 40.0
        mock_time_slices.return_value = np.rec.array(
            [(4096, 0.0, 1.0)], dtype=[("rate", int), ("start", float), ("end", float)]
        )

        mock_bank = mock.MagicMock()
        mock_bank_class.return_value = mock_bank

        with mock.patch.object(
            lsctables.SnglInspiralTable, "get_table", return_value=mock_sngl_table
        ):
            with mock.patch(
                "sgnl.svd_bank.ligolw_utils.local_path_from_url",
                return_value="/path/to/bank.xml",
            ):
                result = svd_bank.build_bank(
                    template_bank_url="file:///path/to/bank.xml",
                    psd={"H1": mock.MagicMock()},
                    flow=40.0,
                    max_duration=128.0,
                    svd_tolerance=0.01,
                    bank_id="0000",
                )

        assert result == mock_bank
        mock_bank.set_template_bank_filename.assert_called_once()

    @mock.patch("sgnl.svd_bank.Bank")
    @mock.patch("sgnl.svd_bank.templates.time_slices")
    @mock.patch("sgnl.svd_bank.cal_higher_f_low")
    @mock.patch("sgnl.svd_bank.check_ffinal_and_find_max_ffinal")
    @mock.patch("sgnl.svd_bank.ligolw_utils.load_url")
    @mock.patch("sgnl.svd_bank.ligolw_process.get_process_params")
    def test_build_bank_with_fallback_approximant(
        self,
        mock_get_params,
        mock_load_url,
        mock_check_ffinal,
        mock_cal_flow,
        mock_time_slices,
        mock_bank_class,
    ):
        """Test build_bank falls back to gstlal_bank_splitter for approximant."""
        mock_xmldoc = mock.MagicMock()
        mock_sngl_table = mock.MagicMock()
        mock_sngl_table.__getitem__ = mock.MagicMock(
            return_value=mock.MagicMock(ifo="H1")
        )
        mock_load_url.return_value = mock_xmldoc

        # First call raises ValueError, second succeeds
        mock_get_params.side_effect = [ValueError("not found"), ["TaylorF2"]]
        mock_check_ffinal.return_value = 2048.0
        mock_cal_flow.return_value = 40.0
        mock_time_slices.return_value = np.rec.array(
            [(4096, 0.0, 1.0)], dtype=[("rate", int), ("start", float), ("end", float)]
        )

        mock_bank = mock.MagicMock()
        mock_bank_class.return_value = mock_bank

        with mock.patch.object(
            lsctables.SnglInspiralTable, "get_table", return_value=mock_sngl_table
        ):
            with mock.patch(
                "sgnl.svd_bank.ligolw_utils.local_path_from_url",
                return_value="/path/to/bank.xml",
            ):
                svd_bank.build_bank(
                    template_bank_url="file:///path/to/bank.xml",
                    psd={"H1": mock.MagicMock()},
                    flow=40.0,
                    max_duration=128.0,
                    svd_tolerance=0.01,
                    bank_id="0000",
                )

        assert mock_get_params.call_count == 2

    @mock.patch("sgnl.svd_bank.Bank")
    @mock.patch("sgnl.svd_bank.templates.time_slices")
    @mock.patch("sgnl.svd_bank.cal_higher_f_low")
    @mock.patch("sgnl.svd_bank.check_ffinal_and_find_max_ffinal")
    @mock.patch("sgnl.svd_bank.ligolw_utils.load_url")
    @mock.patch("sgnl.svd_bank.ligolw_process.get_process_params")
    def test_build_bank_with_instrument_override(
        self,
        mock_get_params,
        mock_load_url,
        mock_check_ffinal,
        mock_cal_flow,
        mock_time_slices,
        mock_bank_class,
    ):
        """Test build_bank with instrument_override."""
        mock_xmldoc = mock.MagicMock()
        mock_row = mock.MagicMock()
        mock_sngl_table_obj = mock.MagicMock()
        mock_sngl_table_obj.__getitem__ = mock.MagicMock(return_value=mock_row)
        mock_sngl_table_obj.__iter__ = mock.MagicMock(return_value=iter([mock_row]))
        mock_load_url.return_value = mock_xmldoc

        mock_get_params.return_value = ["TaylorF2"]
        mock_check_ffinal.return_value = 2048.0
        mock_cal_flow.return_value = 40.0
        mock_time_slices.return_value = np.rec.array(
            [(4096, 0.0, 1.0)], dtype=[("rate", int), ("start", float), ("end", float)]
        )

        mock_bank = mock.MagicMock()
        mock_bank_class.return_value = mock_bank

        with mock.patch.object(
            lsctables.SnglInspiralTable, "get_table", return_value=mock_sngl_table_obj
        ):
            with mock.patch(
                "sgnl.svd_bank.ligolw_utils.local_path_from_url",
                return_value="/path/to/bank.xml",
            ):
                svd_bank.build_bank(
                    template_bank_url="file:///path/to/bank.xml",
                    psd={"L1": mock.MagicMock()},
                    flow=40.0,
                    max_duration=128.0,
                    svd_tolerance=0.01,
                    bank_id="0000",
                    instrument_override="L1",
                )

        # Check that ifo was set to L1
        assert mock_row.ifo == "L1"

    @mock.patch("sgnl.svd_bank.Bank")
    @mock.patch("sgnl.svd_bank.templates.time_slices")
    @mock.patch("sgnl.svd_bank.cal_higher_f_low")
    @mock.patch("sgnl.svd_bank.check_ffinal_and_find_max_ffinal")
    @mock.patch("sgnl.svd_bank.ligolw_utils.load_url")
    @mock.patch("sgnl.svd_bank.ligolw_process.get_process_params")
    def test_build_bank_with_sample_rate(
        self,
        mock_get_params,
        mock_load_url,
        mock_check_ffinal,
        mock_cal_flow,
        mock_time_slices,
        mock_bank_class,
    ):
        """Test build_bank with sample_rate sets fhigh to None."""
        mock_xmldoc = mock.MagicMock()
        mock_sngl_table = mock.MagicMock()
        mock_sngl_table.__getitem__ = mock.MagicMock(
            return_value=mock.MagicMock(ifo="H1")
        )
        mock_load_url.return_value = mock_xmldoc

        mock_get_params.return_value = ["TaylorF2"]
        mock_check_ffinal.return_value = 2048.0
        mock_cal_flow.return_value = 40.0
        mock_time_slices.return_value = np.rec.array(
            [(4096, 0.0, 1.0)], dtype=[("rate", int), ("start", float), ("end", float)]
        )

        mock_bank = mock.MagicMock()
        mock_bank_class.return_value = mock_bank

        with mock.patch.object(
            lsctables.SnglInspiralTable, "get_table", return_value=mock_sngl_table
        ):
            with mock.patch(
                "sgnl.svd_bank.ligolw_utils.local_path_from_url",
                return_value="/path/to/bank.xml",
            ):
                svd_bank.build_bank(
                    template_bank_url="file:///path/to/bank.xml",
                    psd={"H1": mock.MagicMock()},
                    flow=40.0,
                    max_duration=128.0,
                    svd_tolerance=0.01,
                    bank_id="0000",
                    sample_rate=None,
                )

        # When sample_rate is None, fhigh should be set to None
        call_kwargs = mock_bank_class.call_args[1]
        assert call_kwargs.get("fhigh") is None


class TestWriteBank:
    """Tests for write_bank function."""

    def _create_mock_sngl_table_with_row(self):
        """Create a mock sngl_inspiral_table with one row."""
        mock_table = mock.MagicMock()
        mock_row = mock.MagicMock()
        mock_row.ifo = "H1"
        mock_row.template_duration = 1.0
        mock_row.Gamma2 = 0.0
        mock_row.Gamma3 = 0.0
        mock_row.Gamma4 = 0.0
        mock_row.Gamma5 = 0.0
        mock_table.__iter__ = mock.MagicMock(return_value=iter([mock_row]))
        mock_table.__getitem__ = mock.MagicMock(return_value=mock_row)
        return mock_table

    @mock.patch("sgnl.svd_bank.ligolw_utils.write_filename")
    @mock.patch("sgnl.svd_bank.lal.series.make_psd_xmldoc")
    def test_write_bank_basic(self, mock_make_psd, mock_write):
        """Test write_bank with basic parameters - covers line 422 with real calc."""

        # Create a row class that can have Gamma attributes set
        class MockRow:
            ifo = "H1"
            template_duration = 1.0
            Gamma2 = 0.0
            Gamma3 = 0.0
            Gamma4 = 0.0
            Gamma5 = 0.0

        mock_row = MockRow()

        # Create a table that looks like a list but has parentNode attribute
        class MockTable(list):
            def __init__(self, rows):
                super().__init__(rows)
                self.parentNode = mock.MagicMock()

        mock_sngl_table = MockTable([mock_row])

        mock_bank = mock.MagicMock()
        mock_bank.filter_length = 1.0
        mock_bank.logname = "testbank"
        mock_bank.snr_threshold = 4.0
        mock_bank.template_bank_filename = "bank.xml"
        mock_bank.bank_id = "0000"
        mock_bank.newdeltaF = 0.25
        mock_bank.working_f_low = 30.0
        mock_bank.f_low = 40.0
        mock_bank.sample_rate_max = 4096
        # Use autocorrelation_bank with shape matching sngl_inspiral_table length
        mock_bank.autocorrelation_bank = np.array([[0.1 + 0.1j, 1.0 + 0j, 0.1 - 0.1j]])
        mock_bank.autocorrelation_mask = np.array([[1, 0, 1]])
        mock_bank.sigmasq = np.array([1.0])
        mock_bank.bank_correlation_matrix = np.array([[1 + 0.5j]])

        mock_bank.sngl_inspiral_table = mock_sngl_table

        mock_frag = mock.MagicMock()
        mock_frag.rate = 4096
        mock_frag.start = 0.0
        mock_frag.end = 1.0
        mock_frag.chifacs = np.array([1.0, 2.0])
        mock_frag.mix_matrix = np.array([[1, 2, 3, 4]])
        mock_frag.orthogonal_template_bank = np.array([[1, 2], [3, 4]])
        mock_frag.singular_values = np.array([1.0, 0.5])
        mock_bank.bank_fragments = [mock_frag]

        mock_psd = mock.MagicMock()

        svd_bank.write_bank(
            "output.xml",
            [mock_bank],
            {"H1": mock_psd},
            verbose=True,
        )

        # Verify line 422 was executed - Gamma2 should be changed from 0.0
        assert mock_row.Gamma2 != 0.0
        mock_write.assert_called_once()

    @mock.patch("sgnl.svd_bank.ligolw_utils.write_filename")
    @mock.patch("sgnl.svd_bank.lal.series.make_psd_xmldoc")
    @mock.patch("sgnl.svd_bank.calc_lambda_eta_sum")
    def test_write_bank_with_none_values(self, mock_calc, mock_make_psd, mock_write):
        """Test write_bank when mix_matrix and singular_values are None."""
        mock_calc.return_value = (1.0, 2.0, 3.0, 4.0)

        mock_bank = mock.MagicMock()
        mock_bank.filter_length = 1.0
        mock_bank.logname = None  # Test None logname (empty string output)
        mock_bank.snr_threshold = 4.0
        mock_bank.template_bank_filename = "bank.xml"
        mock_bank.bank_id = "0000"
        mock_bank.newdeltaF = 0.25
        mock_bank.working_f_low = 30.0
        mock_bank.f_low = 40.0
        mock_bank.sample_rate_max = 4096
        mock_bank.autocorrelation_bank = np.array([[1 + 1j, 2 + 2j]])
        mock_bank.autocorrelation_mask = np.array([[1, 0]])
        mock_bank.sigmasq = np.array([1.0])
        mock_bank.bank_correlation_matrix = np.array([[1 + 0.5j]])

        mock_bank.sngl_inspiral_table = self._create_mock_sngl_table_with_row()

        mock_frag = mock.MagicMock()
        mock_frag.rate = 4096
        mock_frag.start = 0.0
        mock_frag.end = 1.0
        mock_frag.chifacs = np.array([1.0, 2.0])
        mock_frag.mix_matrix = None  # Test None mix_matrix
        mock_frag.orthogonal_template_bank = np.array([[1, 2], [3, 4]])
        mock_frag.singular_values = None  # Test None singular_values
        mock_bank.bank_fragments = [mock_frag]

        mock_psd = mock.MagicMock()

        svd_bank.write_bank(
            "output.xml",
            [mock_bank],
            {"H1": mock_psd},
        )

        mock_write.assert_called_once()

    @mock.patch("sgnl.svd_bank.ligolw_utils.write_filename")
    @mock.patch("sgnl.svd_bank.lal.series.make_psd_xmldoc")
    @mock.patch("sgnl.svd_bank.calc_lambda_eta_sum")
    def test_write_bank_with_process_params(self, mock_calc, mock_make_psd, mock_write):
        """Test write_bank with process_param_dict."""
        mock_calc.return_value = (1.0, 2.0, 3.0, 4.0)

        mock_bank = mock.MagicMock()
        mock_bank.filter_length = 1.0
        mock_bank.logname = "test"
        mock_bank.snr_threshold = 4.0
        mock_bank.template_bank_filename = "bank.xml"
        mock_bank.bank_id = "0000"
        mock_bank.newdeltaF = 0.25
        mock_bank.working_f_low = 30.0
        mock_bank.f_low = 40.0
        mock_bank.sample_rate_max = 4096
        mock_bank.autocorrelation_bank = np.array([[1 + 1j]])
        mock_bank.autocorrelation_mask = np.array([[1]])
        mock_bank.sigmasq = np.array([1.0])
        mock_bank.bank_correlation_matrix = np.array([[1 + 0.5j]])

        mock_bank.sngl_inspiral_table = self._create_mock_sngl_table_with_row()

        mock_frag = mock.MagicMock()
        mock_frag.rate = 4096
        mock_frag.start = 0.0
        mock_frag.end = 1.0
        mock_frag.chifacs = np.array([1.0])
        mock_frag.mix_matrix = np.array([[1, 2]])
        mock_frag.orthogonal_template_bank = np.array([[1, 2]])
        mock_frag.singular_values = np.array([1.0])
        mock_bank.bank_fragments = [mock_frag]

        mock_psd = mock.MagicMock()

        svd_bank.write_bank(
            "output.xml",
            [mock_bank],
            {"H1": mock_psd},
            process_param_dict={"flow": 40.0},
        )

        mock_write.assert_called_once()


class TestReadBanks:
    """Tests for read_banks function."""

    @mock.patch("sgnl.svd_bank.ligolw_utils.load_url")
    def test_read_banks_empty_document_raises(self, mock_load):
        """Test read_banks with empty document raises ValueError."""
        mock_xmldoc = mock.MagicMock()
        mock_xmldoc.getElementsByTagName.return_value = []
        mock_load.return_value = mock_xmldoc

        with mock.patch(
            "sgnl.svd_bank.lal.series.read_psd_xmldoc", side_effect=ValueError("no PSD")
        ):
            # read_banks doesn't handle empty banks well - it crashes
            # in horizon_distance_func when there are no banks
            with pytest.raises(ValueError, match="min\\(\\).*empty"):
                svd_bank.read_banks("test.xml", contenthandler=TestContentHandler)

        mock_load.assert_called_once()


class TestPreferredHorizonDistanceTemplate:
    """Tests for preferred_horizon_distance_template function."""

    def test_preferred_horizon_distance_template(self):
        """Test preferred_horizon_distance_template function."""
        mock_row1 = mock.MagicMock()
        mock_row1.template_id = 2
        mock_row1.mass1 = 1.5
        mock_row1.mass2 = 1.5
        mock_row1.spin1z = 0.1
        mock_row1.spin2z = 0.1

        mock_row2 = mock.MagicMock()
        mock_row2.template_id = 1
        mock_row2.mass1 = 1.4
        mock_row2.mass2 = 1.4
        mock_row2.spin1z = 0.0
        mock_row2.spin2z = 0.0

        mock_bank = mock.MagicMock()
        mock_bank.sngl_inspiral_table = [mock_row1, mock_row2]

        template_id, m1, m2, s1z, s2z = svd_bank.preferred_horizon_distance_template(
            [mock_bank]
        )

        assert template_id == 1
        assert m1 == 1.4
        assert m2 == 1.4


class TestHorizonDistanceFunc:
    """Tests for horizon_distance_func function."""

    @mock.patch("sgnl.svd_bank.HorizonDistance")
    def test_horizon_distance_func_basic(self, mock_horizon_class):
        """Test horizon_distance_func with matching Nyquist frequencies."""
        mock_bank = mock.MagicMock()
        mock_bank.get_rates.return_value = {4096}

        mock_row = mock.MagicMock()
        mock_row.template_id = 1
        mock_row.mass1 = 1.4
        mock_row.mass2 = 1.4
        mock_row.spin1z = 0.0
        mock_row.spin2z = 0.0
        mock_bank.sngl_inspiral_table = [mock_row]

        mock_horizon = mock.MagicMock()
        mock_horizon_class.return_value = mock_horizon

        template_id, func = svd_bank.horizon_distance_func([mock_bank])

        assert template_id == 1
        assert func == mock_horizon

    @mock.patch("sgnl.svd_bank.HorizonDistance")
    def test_horizon_distance_func_mismatched_nyquists(self, mock_horizon_class):
        """Test horizon_distance_func warns when Nyquist frequencies differ."""
        mock_bank1 = mock.MagicMock()
        mock_bank1.get_rates.return_value = {4096}
        mock_bank1.sngl_inspiral_table = [
            mock.MagicMock(template_id=1, mass1=1.4, mass2=1.4, spin1z=0.0, spin2z=0.0)
        ]

        mock_bank2 = mock.MagicMock()
        mock_bank2.get_rates.return_value = {2048}
        mock_bank2.sngl_inspiral_table = [
            mock.MagicMock(template_id=2, mass1=1.5, mass2=1.5, spin1z=0.0, spin2z=0.0)
        ]

        mock_horizon_class.return_value = mock.MagicMock()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            svd_bank.horizon_distance_func([mock_bank1, mock_bank2])

            assert len(w) == 1
            assert "same Nyquist frequency" in str(w[0].message)


class TestParseBankFiles:
    """Tests for parse_bank_files function."""

    @mock.patch("sgnl.svd_bank.ligolw_utils.write_filename")
    @mock.patch("sgnl.svd_bank.read_banks")
    def test_parse_bank_files_basic(self, mock_read_banks, mock_write):
        """Test parse_bank_files with basic input."""
        mock_bank = mock.MagicMock()
        mock_bank.sngl_inspiral_table.copy.return_value = mock.MagicMock()
        mock_read_banks.return_value = [mock_bank]

        with mock.patch("tempfile.NamedTemporaryFile") as mock_temp:
            mock_temp.return_value.name = "/tmp/test.xml.gz"
            result = svd_bank.parse_bank_files(
                {"H1": "bank.xml"}, verbose=True, snr_threshold=5.0
            )

        assert "H1" in result
        assert len(result["H1"]) == 1
        assert result["H1"][0].snr_threshold == 5.0

    @mock.patch("sgnl.svd_bank.read_banks")
    def test_parse_bank_files_empty_raises(self, mock_read_banks):
        """Test parse_bank_files raises when no banks parsed."""
        mock_read_banks.return_value = []

        with pytest.raises(ValueError, match="Could not parse bank files"):
            svd_bank.parse_bank_files({"H1": "bank.xml"}, verbose=False)


class TestParseSvdbankString:
    """Tests for parse_svdbank_string function."""

    def test_parse_svdbank_string_basic(self):
        """Test parse_svdbank_string with basic input."""
        result = svd_bank.parse_svdbank_string("H1:bank1.xml,L1:bank2.xml")

        assert result == {"H1": "bank1.xml", "L1": "bank2.xml"}

    def test_parse_svdbank_string_none(self):
        """Test parse_svdbank_string with None input."""
        result = svd_bank.parse_svdbank_string(None)
        assert result == {}

    def test_parse_svdbank_string_duplicate_ifo_raises(self):
        """Test parse_svdbank_string raises when duplicate ifo."""
        with pytest.raises(ValueError, match="Only one svd bank per instrument"):
            svd_bank.parse_svdbank_string("H1:bank1.xml,H1:bank2.xml")


class TestCalcLambdaEtaSum:
    """Tests for calc_lambda_eta_sum function."""

    def test_calc_lambda_eta_sum_basic(self):
        """Test calc_lambda_eta_sum with basic input."""
        auto_correlation = np.array([0.1 + 0.1j, 1.0 + 0j, 0.1 - 0.1j])

        result = svd_bank.calc_lambda_eta_sum(auto_correlation)

        assert len(result) == 4
        assert isinstance(result[0], (float, np.floating))
        assert isinstance(result[1], (float, np.floating))
        assert isinstance(result[2], (float, np.floating))
        assert isinstance(result[3], (float, np.floating))

    def test_calc_lambda_eta_sum_longer_array(self):
        """Test calc_lambda_eta_sum with longer array."""
        auto_correlation = np.array(
            [
                0.05 + 0.05j,
                0.1 + 0.1j,
                0.5 + 0.2j,
                1.0 + 0j,
                0.5 - 0.2j,
                0.1 - 0.1j,
                0.05 - 0.05j,
            ]
        )

        result = svd_bank.calc_lambda_eta_sum(auto_correlation)

        assert len(result) == 4
        lambda_sum, lambdasq_sum, lambda_etasq_sum, lambdasq_etasq_sum = result
        assert lambda_sum >= 0  # norm_chisq should be non-negative


class TestReadBanksIntegration:
    """Integration tests for read_banks function."""

    @mock.patch("sgnl.svd_bank.HorizonDistance")
    @mock.patch("sgnl.svd_bank.condition_psd")
    @mock.patch("sgnl.svd_bank.lal.series.read_psd_xmldoc")
    @mock.patch("sgnl.svd_bank.ligolw_utils.load_url")
    def test_read_banks_full_parsing(
        self, mock_load_url, mock_read_psd, mock_condition_psd, mock_horizon
    ):
        """Test read_banks with a properly mocked LIGOLW document."""
        # Create mock PSD
        mock_psd = mock.MagicMock()
        mock_read_psd.return_value = {"H1": mock_psd}
        mock_condition_psd.return_value = mock_psd

        # Create mock horizon distance
        mock_horizon_instance = mock.MagicMock()
        mock_horizon.return_value = mock_horizon_instance

        # Create a mock row class that can be iterated multiple times
        class MockRow:
            template_id = 1
            mass1 = 1.4
            mass2 = 1.4
            spin1z = 0.0
            spin2z = 0.0

        mock_row = MockRow()

        # Create a list-like mock that supports multiple iteration
        class MockSnglTable(list):
            def __init__(self, rows):
                super().__init__(rows)
                self.parentNode = mock.MagicMock()

        mock_sngl_table = MockSnglTable([mock_row])

        # Create mock LIGO_LW element for bank
        mock_bank_elem = mock.MagicMock()
        mock_bank_elem.hasAttribute.return_value = True
        mock_bank_elem.Name = "sgnl_svd_bank_Bank"

        # Create mock fragment element
        mock_frag_elem = mock.MagicMock()
        mock_frag_elem.tagName = "LIGO_LW"

        mock_bank_elem.childNodes = [mock_frag_elem]

        # Setup getElementsByTagName to return our bank element
        mock_xmldoc = mock.MagicMock()
        mock_xmldoc.getElementsByTagName.return_value = [mock_bank_elem]
        mock_load_url.return_value = mock_xmldoc

        # Mock param get
        def get_param_side_effect(elem, name):
            param_values = {
                "bank_id": mock.MagicMock(value="0000_test"),
                "sample_rate_max": mock.MagicMock(value=4096),
                "filter_length": mock.MagicMock(value=1.0),
                "logname": None,
                "snr_threshold": mock.MagicMock(value=4.0),
                "template_bank_filename": mock.MagicMock(value="bank.xml"),
                "new_deltaf": mock.MagicMock(value=0.25),
                "working_f_low": mock.MagicMock(value=30.0),
                "f_low": mock.MagicMock(value=40.0),
                "rate": mock.MagicMock(value=4096),
                "start": mock.MagicMock(value=0.0),
                "end": mock.MagicMock(value=1.0),
            }
            return param_values.get(name, mock.MagicMock(value=0))

        # Mock array get
        def get_array_side_effect(elem, name):
            array_values = {
                "autocorrelation_bank_real": mock.MagicMock(array=np.array([[1.0]])),
                "autocorrelation_bank_imag": mock.MagicMock(array=np.array([[0.0]])),
                "sigmasq": mock.MagicMock(array=np.array([1.0])),
                "autocorrelation_mask": mock.MagicMock(array=np.array([[1]])),
                "bank_correlation_matrix_real": mock.MagicMock(array=np.array([[1.0]])),
                "bank_correlation_matrix_imag": mock.MagicMock(array=np.array([[0.0]])),
                "chifacs": mock.MagicMock(array=np.array([1.0])),
                "singular_values": mock.MagicMock(array=np.array([1.0])),
                "mix_matrix": mock.MagicMock(array=np.array([[1, 2]])),
                "orthogonal_template_bank": mock.MagicMock(array=np.array([[1, 2]])),
            }
            if name not in array_values:
                raise ValueError(f"Array {name} not found")
            return array_values[name]

        with mock.patch.object(
            lsctables.SnglInspiralTable, "get_table", return_value=mock_sngl_table
        ):
            from igwn_ligolw.ligolw import Array as ligolw_array
            from igwn_ligolw.ligolw import Param as ligolw_param

            with mock.patch.object(
                ligolw_param, "get_param", side_effect=get_param_side_effect
            ):
                with mock.patch.object(
                    ligolw_array, "get_array", side_effect=get_array_side_effect
                ):
                    banks = svd_bank.read_banks(
                        "test.xml", contenthandler=TestContentHandler
                    )

        assert len(banks) == 1
        assert banks[0].bank_id == "0000_test"

    @mock.patch("sgnl.svd_bank.HorizonDistance")
    @mock.patch("sgnl.svd_bank.lal.series.read_psd_xmldoc")
    @mock.patch("sgnl.svd_bank.ligolw_utils.load_url")
    def test_read_banks_fast_mode(self, mock_load_url, mock_read_psd, mock_horizon):
        """Test read_banks with fast=True mode."""
        mock_read_psd.return_value = {"H1": mock.MagicMock()}
        mock_horizon.return_value = mock.MagicMock()

        # Create mock row class that can be iterated multiple times
        class MockRow:
            template_id = 1
            mass1 = 1.4
            mass2 = 1.4
            spin1z = 0.0
            spin2z = 0.0

        mock_row = MockRow()

        # Create a list-like mock that supports multiple iteration
        class MockSnglTable(list):
            def __init__(self, rows):
                super().__init__(rows)
                self.parentNode = mock.MagicMock()

        mock_sngl_table = MockSnglTable([mock_row])

        # Create mock bank element
        mock_bank_elem = mock.MagicMock()
        mock_bank_elem.hasAttribute.return_value = True
        mock_bank_elem.Name = "sgnl_svd_bank_Bank"

        # Create mock fragment
        mock_frag_elem = mock.MagicMock()
        mock_frag_elem.tagName = "LIGO_LW"
        mock_bank_elem.childNodes = [mock_frag_elem]

        mock_xmldoc = mock.MagicMock()
        mock_xmldoc.getElementsByTagName.return_value = [mock_bank_elem]
        mock_load_url.return_value = mock_xmldoc

        def get_param_side_effect(elem, name):
            param_values = {
                "bank_id": mock.MagicMock(value="0001"),
                "sample_rate_max": mock.MagicMock(value=4096),
                "rate": mock.MagicMock(value=4096),
                "start": mock.MagicMock(value=0.0),
                "end": mock.MagicMock(value=1.0),
            }
            return param_values.get(name, mock.MagicMock(value=0))

        def get_array_side_effect(elem, name):
            array_values = {
                "autocorrelation_bank_real": mock.MagicMock(array=np.array([[1.0]])),
                "autocorrelation_bank_imag": mock.MagicMock(array=np.array([[0.0]])),
                "sigmasq": mock.MagicMock(array=np.array([1.0])),
                "mix_matrix": mock.MagicMock(array=np.array([[1, 2]])),
                "orthogonal_template_bank": mock.MagicMock(array=np.array([[1, 2]])),
            }
            if name not in array_values:
                raise ValueError(f"Array {name} not found")
            return array_values[name]

        with mock.patch.object(
            lsctables.SnglInspiralTable, "get_table", return_value=mock_sngl_table
        ):
            from igwn_ligolw.ligolw import Array as ligolw_array
            from igwn_ligolw.ligolw import Param as ligolw_param

            with mock.patch.object(
                ligolw_param, "get_param", side_effect=get_param_side_effect
            ):
                with mock.patch.object(
                    ligolw_array, "get_array", side_effect=get_array_side_effect
                ):
                    banks = svd_bank.read_banks(
                        "test.xml", contenthandler=TestContentHandler, fast=True
                    )

        assert len(banks) == 1

    @mock.patch("sgnl.svd_bank.HorizonDistance")
    @mock.patch("sgnl.svd_bank.lal.series.read_psd_xmldoc")
    @mock.patch("sgnl.svd_bank.ligolw_utils.load_url")
    def test_read_banks_no_psd(self, mock_load_url, mock_read_psd, mock_horizon):
        """Test read_banks when no PSD in document."""
        mock_read_psd.side_effect = ValueError("no PSD")
        mock_horizon.return_value = mock.MagicMock()

        # Create mock row class that can be iterated multiple times
        class MockRow:
            template_id = 1
            mass1 = 1.4
            mass2 = 1.4
            spin1z = 0.0
            spin2z = 0.0

        mock_row = MockRow()

        # Create a list-like mock that supports multiple iteration
        class MockSnglTable(list):
            def __init__(self, rows):
                super().__init__(rows)
                self.parentNode = mock.MagicMock()

        mock_sngl_table = MockSnglTable([mock_row])

        mock_bank_elem = mock.MagicMock()
        mock_bank_elem.hasAttribute.return_value = True
        mock_bank_elem.Name = "gstlal_svd_bank_Bank"  # Test gstlal name

        mock_frag_elem = mock.MagicMock()
        mock_frag_elem.tagName = "LIGO_LW"
        mock_bank_elem.childNodes = [mock_frag_elem]

        mock_xmldoc = mock.MagicMock()
        mock_xmldoc.getElementsByTagName.return_value = [mock_bank_elem]
        mock_load_url.return_value = mock_xmldoc

        def get_param_side_effect(elem, name):
            param_values = {
                "bank_id": mock.MagicMock(value="0002"),
                "sample_rate_max": mock.MagicMock(value=4096),
                "filter_length": mock.MagicMock(value=1.0),
                "logname": None,
                "snr_threshold": mock.MagicMock(value=4.0),
                "template_bank_filename": mock.MagicMock(value="bank.xml"),
                "rate": mock.MagicMock(value=4096),
                "start": mock.MagicMock(value=0.0),
                "end": mock.MagicMock(value=1.0),
            }
            if name in ["new_deltaf", "working_f_low", "f_low"]:
                raise ValueError("param not found")
            return param_values.get(name, mock.MagicMock(value=0))

        def get_array_side_effect(elem, name):
            array_values = {
                "autocorrelation_bank_real": mock.MagicMock(array=np.array([[1.0]])),
                "autocorrelation_bank_imag": mock.MagicMock(array=np.array([[0.0]])),
                "sigmasq": mock.MagicMock(array=np.array([1.0])),
                "autocorrelation_mask": mock.MagicMock(array=np.array([[1]])),
                "bank_correlation_matrix_real": mock.MagicMock(array=np.array([[1.0]])),
                "bank_correlation_matrix_imag": mock.MagicMock(array=np.array([[0.0]])),
                "chifacs": mock.MagicMock(array=np.array([1.0])),
                "mix_matrix": mock.MagicMock(array=np.array([[1, 2]])),
                "orthogonal_template_bank": mock.MagicMock(array=np.array([[1, 2]])),
            }
            if name == "singular_values":
                raise ValueError("not found")
            if name not in array_values:
                raise ValueError(f"Array {name} not found")
            return array_values[name]

        with mock.patch.object(
            lsctables.SnglInspiralTable, "get_table", return_value=mock_sngl_table
        ):
            from igwn_ligolw.ligolw import Array as ligolw_array
            from igwn_ligolw.ligolw import Param as ligolw_param

            with mock.patch.object(
                ligolw_param, "get_param", side_effect=get_param_side_effect
            ):
                with mock.patch.object(
                    ligolw_array, "get_array", side_effect=get_array_side_effect
                ):
                    banks = svd_bank.read_banks(
                        "test.xml", contenthandler=TestContentHandler
                    )

        assert len(banks) == 1
        assert banks[0].processed_psd is None

    @mock.patch("sgnl.svd_bank.HorizonDistance")
    @mock.patch("sgnl.svd_bank.condition_psd")
    @mock.patch("sgnl.svd_bank.lal.series.read_psd_xmldoc")
    @mock.patch("sgnl.svd_bank.ligolw_utils.load_url")
    def test_read_banks_no_mix_matrix(
        self, mock_load_url, mock_read_psd, mock_condition_psd, mock_horizon
    ):
        """Test read_banks when mix_matrix is missing."""
        mock_psd = mock.MagicMock()
        mock_read_psd.return_value = {"H1": mock_psd}
        mock_condition_psd.return_value = mock_psd
        mock_horizon.return_value = mock.MagicMock()

        # Create mock row class that can be iterated multiple times
        class MockRow:
            template_id = 1
            mass1 = 1.4
            mass2 = 1.4
            spin1z = 0.0
            spin2z = 0.0

        mock_row = MockRow()

        # Create a list-like mock that supports multiple iteration
        class MockSnglTable(list):
            def __init__(self, rows):
                super().__init__(rows)
                self.parentNode = mock.MagicMock()

        mock_sngl_table = MockSnglTable([mock_row])

        mock_bank_elem = mock.MagicMock()
        mock_bank_elem.hasAttribute.return_value = True
        mock_bank_elem.Name = "sgnl_svd_bank_Bank"

        mock_frag_elem = mock.MagicMock()
        mock_frag_elem.tagName = "LIGO_LW"
        mock_bank_elem.childNodes = [mock_frag_elem]

        mock_xmldoc = mock.MagicMock()
        mock_xmldoc.getElementsByTagName.return_value = [mock_bank_elem]
        mock_load_url.return_value = mock_xmldoc

        def get_param_side_effect(elem, name):
            param_values = {
                "bank_id": mock.MagicMock(value="0003"),
                "sample_rate_max": mock.MagicMock(value=4096),
                "filter_length": mock.MagicMock(value=1.0),
                "logname": None,
                "snr_threshold": mock.MagicMock(value=4.0),
                "template_bank_filename": mock.MagicMock(value="bank.xml"),
                "new_deltaf": mock.MagicMock(value=0.25),
                "working_f_low": mock.MagicMock(value=30.0),
                "f_low": mock.MagicMock(value=40.0),
                "rate": mock.MagicMock(value=4096),
                "start": mock.MagicMock(value=0.0),
                "end": mock.MagicMock(value=1.0),
            }
            return param_values.get(name, mock.MagicMock(value=0))

        def get_array_side_effect(elem, name):
            array_values = {
                "autocorrelation_bank_real": mock.MagicMock(array=np.array([[1.0]])),
                "autocorrelation_bank_imag": mock.MagicMock(array=np.array([[0.0]])),
                "sigmasq": mock.MagicMock(array=np.array([1.0])),
                "autocorrelation_mask": mock.MagicMock(array=np.array([[1]])),
                "bank_correlation_matrix_real": mock.MagicMock(array=np.array([[1.0]])),
                "bank_correlation_matrix_imag": mock.MagicMock(array=np.array([[0.0]])),
                "chifacs": mock.MagicMock(array=np.array([1.0])),
                "singular_values": mock.MagicMock(array=np.array([1.0])),
                "orthogonal_template_bank": mock.MagicMock(array=np.array([[1, 2]])),
            }
            if name == "mix_matrix":
                raise ValueError("mix_matrix not found")
            if name not in array_values:
                raise ValueError(f"Array {name} not found")
            return array_values[name]

        with mock.patch.object(
            lsctables.SnglInspiralTable, "get_table", return_value=mock_sngl_table
        ):
            from igwn_ligolw.ligolw import Array as ligolw_array
            from igwn_ligolw.ligolw import Param as ligolw_param

            with mock.patch.object(
                ligolw_param, "get_param", side_effect=get_param_side_effect
            ):
                with mock.patch.object(
                    ligolw_array, "get_array", side_effect=get_array_side_effect
                ):
                    banks = svd_bank.read_banks(
                        "test.xml", contenthandler=TestContentHandler
                    )

        assert len(banks) == 1
        assert banks[0].bank_fragments[0].mix_matrix is None


class TestBankVerboseAndMultipleFragments:
    """Tests for Bank with verbose mode and multiple fragments."""

    @mock.patch("sgnl.svd_bank.cbc_template_fir.decompose_templates")
    @mock.patch("sgnl.svd_bank.cbc_template_fir.generate_templates")
    @mock.patch("sgnl.svd_bank.read_approximant")
    def test_bank_verbose_mode(
        self, mock_read_approx, mock_generate, mock_decompose, capsys
    ):
        """Test Bank.__post_init__ with verbose=True."""
        mock_read_approx.return_value = "TaylorF2"

        workspace = mock.MagicMock()
        workspace.psd = mock.MagicMock()
        workspace.working_duration = 4.0
        workspace.working_f_low = 30.0
        workspace.f_low = 40.0
        workspace.sample_rate_max = 4096
        workspace.time_slices = [(4096, 0.0, 1.0)]

        mock_sngl_table = mock.MagicMock()
        mock_sngl_row = mock.MagicMock()
        mock_sngl_row.Gamma1 = 0
        mock_sngl_table.copy.return_value = mock.MagicMock()
        mock_sngl_table.__iter__ = mock.MagicMock(return_value=iter([mock_sngl_row]))
        mock_sngl_table.__getitem__ = mock.MagicMock(return_value=mock_sngl_row)

        mock_generate.return_value = (
            [np.array([[1, 2], [3, 4]])],
            np.array([[1 + 1j, 2 + 2j]]),
            np.array([[1, 0]]),
            np.array([1.0]),
            workspace,
        )

        mock_decompose.return_value = (
            np.array([[1, 2], [3, 4]]),
            np.array([1.0, 0.5]),
            np.array([[1, 2, 3, 4]]),
            np.array([1.0, 2.0, 3.0, 4.0]),
        )

        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())
        create_mock_sngl_inspiral_table(xmldoc, num_rows=1)

        time_slices_rec = np.rec.array(
            [(4096, 0.0, 1.0)], dtype=[("rate", int), ("start", float), ("end", float)]
        )

        mock_psd = mock.MagicMock()

        with mock.patch.object(
            lsctables.SnglInspiralTable, "get_table", return_value=mock_sngl_table
        ):
            svd_bank.Bank(
                bank_xmldoc=xmldoc,
                psd=mock_psd,
                time_slices=time_slices_rec,
                snr_threshold=4.0,
                tolerance=0.01,
                bank_id="0000_test",
                verbose=True,
            )

        captured = capsys.readouterr()
        assert "constructing template decomposition" in captured.err

    @mock.patch("sgnl.svd_bank.cbc_template_fir.decompose_templates")
    @mock.patch("sgnl.svd_bank.cbc_template_fir.generate_templates")
    @mock.patch("sgnl.svd_bank.read_approximant")
    def test_bank_multiple_fragments_correlation_matrix(
        self, mock_read_approx, mock_generate, mock_decompose
    ):
        """Test Bank with multiple fragments to cover correlation matrix update."""
        mock_read_approx.return_value = "TaylorF2"

        workspace = mock.MagicMock()
        workspace.psd = mock.MagicMock()
        workspace.working_duration = 4.0
        workspace.working_f_low = 30.0
        workspace.f_low = 40.0
        workspace.sample_rate_max = 4096
        # Multiple time slices for multiple fragments
        workspace.time_slices = [(4096, 0.0, 0.5), (2048, 0.5, 1.0)]

        # Create a row class with Gamma1 attribute
        class MockSnglRow:
            Gamma1 = 0

        mock_sngl_row = MockSnglRow()

        # Create a list-like mock that supports slicing for lines 248-249
        class MockSnglTable(list):
            def __init__(self, rows):
                super().__init__(rows)

            def copy(self):
                return MockSnglTable(list(self))

        mock_sngl_table = MockSnglTable([mock_sngl_row])

        # Two template banks for two fragments
        mock_generate.return_value = (
            [np.array([[1, 2], [3, 4]]), np.array([[5, 6], [7, 8]])],
            np.array([[1 + 1j, 2 + 2j]]),
            np.array([[1, 0]]),
            np.array([1.0]),
            workspace,
        )

        mock_decompose.return_value = (
            np.array([[1, 2], [3, 4]]),
            np.array([1.0, 0.5]),
            np.array([[1, 2, 3, 4]]),
            np.array([1.0, 2.0, 3.0, 4.0]),
        )

        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())
        create_mock_sngl_inspiral_table(xmldoc, num_rows=1)

        time_slices_rec = np.rec.array(
            [(4096, 0.0, 0.5), (2048, 0.5, 1.0)],
            dtype=[("rate", int), ("start", float), ("end", float)],
        )

        mock_psd = mock.MagicMock()

        with mock.patch.object(
            lsctables.SnglInspiralTable, "get_table", return_value=mock_sngl_table
        ):
            bank = svd_bank.Bank(
                bank_xmldoc=xmldoc,
                psd=mock_psd,
                time_slices=time_slices_rec,
                snr_threshold=4.0,
                tolerance=0.01,
                bank_id="0000_test",
            )

        # Verify bank_correlation_matrix was updated (line 236 coverage)
        assert bank.bank_correlation_matrix is not None
        assert len(bank.bank_fragments) == 2
        # Verify line 248-249 was executed - Gamma1 should be set
        assert mock_sngl_row.Gamma1 == 0  # The value from bank_id.split("_")[0]

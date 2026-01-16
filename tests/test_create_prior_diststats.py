"""Unit tests for sgnl.bin.create_prior_diststats module."""

import sys
from unittest import mock

import pytest

from sgnl.bin import create_prior_diststats


class MockSnglInspiralRow:
    """Mock row for sngl_inspiral_table."""

    def __init__(self, template_id, gamma2=1.0, gamma3=1.0, gamma4=1.0, gamma5=1.0):
        self.template_id = template_id
        self.Gamma2 = gamma2
        self.Gamma3 = gamma3
        self.Gamma4 = gamma4
        self.Gamma5 = gamma5
        # Combo chisq columns (Gamma6-9)
        self.Gamma6 = gamma2
        self.Gamma7 = gamma3
        self.Gamma8 = gamma4
        self.Gamma9 = gamma5


class MockSVDBank:
    """Mock SVD bank object."""

    def __init__(self, template_ids=None, horizon_factors=None):
        if template_ids is None:
            template_ids = [1, 2, 3]
        self.sngl_inspiral_table = [MockSnglInspiralRow(tid) for tid in template_ids]
        if horizon_factors is None:
            self.horizon_factors = {tid: 1.0 for tid in template_ids}
        else:
            self.horizon_factors = horizon_factors


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_parse_basic_args(self, tmp_path):
        """Test parsing with basic required arguments."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        mock_bank = MockSVDBank()

        with mock.patch(
            "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
        ) as mock_read:
            mock_read.return_value = [mock_bank]

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--instrument",
                "H1",
                "--instrument",
                "L1",
                "--output-likelihood-file",
                str(output_file),
            ]

            with mock.patch.object(sys, "argv", test_args):
                options, params, template_ids, horizon_factors, lambda_eta = (
                    create_prior_diststats.parse_command_line()
                )

        assert options.coincidence_threshold == 0.005
        assert options.min_instruments == 2
        assert set(options.instrument) == {"H1", "L1"}
        assert str(output_file) in options.output_likelihood_file
        assert len(template_ids) == 3

    def test_parse_with_all_options(self, tmp_path):
        """Test parsing with all optional arguments."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"
        mass_model = tmp_path / "mass_model.hdf5"
        dtdphi = tmp_path / "dtdphi.hdf5"
        idq = tmp_path / "idq.hdf5"
        rankingstatpdf = tmp_path / "rankingstatpdf.xml.gz"

        mock_bank = MockSVDBank()

        with mock.patch(
            "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
        ) as mock_read:
            mock_read.return_value = [mock_bank]

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--instrument",
                "H1",
                "--instrument",
                "L1",
                "--output-likelihood-file",
                str(output_file),
                "--coincidence-threshold",
                "0.01",
                "--min-instruments",
                "1",
                "--mass-model-file",
                str(mass_model),
                "--dtdphi-file",
                str(dtdphi),
                "--idq-file",
                str(idq),
                "--mismatch-min",
                "0.002",
                "--mismatch-max",
                "0.4",
                "--write-empty-rankingstatpdf",
                str(rankingstatpdf),
                "--search",
                "ew",
                "-v",
            ]

            with mock.patch.object(sys, "argv", test_args):
                options, params, template_ids, horizon_factors, lambda_eta = (
                    create_prior_diststats.parse_command_line()
                )

        assert options.coincidence_threshold == 0.01
        assert options.min_instruments == 1
        assert options.mass_model_file == str(mass_model)
        assert options.dtdphi_file == str(dtdphi)
        assert options.idq_file == str(idq)
        assert options.mismatch_min == 0.002
        assert options.mismatch_max == 0.4
        assert options.search == "ew"
        assert options.verbose is True

    def test_parse_with_seed_likelihood(self, tmp_path):
        """Test parsing with seed likelihood file."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"
        seed_file = tmp_path / "seed.xml.gz"

        mock_bank = MockSVDBank()

        with mock.patch(
            "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
        ) as mock_read:
            mock_read.return_value = [mock_bank]

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--seed-likelihood",
                str(seed_file),
                "--output-likelihood-file",
                str(output_file),
                # These should be ignored with seed
                "--instrument",
                "H1",
                "--coincidence-threshold",
                "0.01",
            ]

            with mock.patch.object(sys, "argv", test_args):
                with pytest.warns(UserWarning, match="--seed-likelihood given"):
                    options, params, template_ids, horizon_factors, lambda_eta = (
                        create_prior_diststats.parse_command_line()
                    )

        # These should be set to None when seed is provided
        assert options.coincidence_threshold is None
        assert options.min_instruments is None
        assert options.instrument is None
        assert options.seed_likelihood == str(seed_file)

    def test_parse_missing_instrument_without_seed(self, tmp_path):
        """Test that missing instrument raises ValueError without seed."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        mock_bank = MockSVDBank()

        with mock.patch(
            "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
        ) as mock_read:
            mock_read.return_value = [mock_bank]

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--output-likelihood-file",
                str(output_file),
            ]

            with mock.patch.object(sys, "argv", test_args):
                with pytest.raises(ValueError, match="must specify at least one"):
                    create_prior_diststats.parse_command_line()

    def test_parse_min_instruments_less_than_one(self, tmp_path):
        """Test that min_instruments < 1 raises ValueError."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        mock_bank = MockSVDBank()

        with mock.patch(
            "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
        ) as mock_read:
            mock_read.return_value = [mock_bank]

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--instrument",
                "H1",
                "--min-instruments",
                "0",
                "--output-likelihood-file",
                str(output_file),
            ]

            with mock.patch.object(sys, "argv", test_args):
                with pytest.raises(ValueError, match="--min-instruments must be >= 1"):
                    create_prior_diststats.parse_command_line()

    def test_parse_min_instruments_greater_than_instruments(self, tmp_path):
        """Test that min_instruments > len(instruments) raises ValueError."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        mock_bank = MockSVDBank()

        with mock.patch(
            "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
        ) as mock_read:
            mock_read.return_value = [mock_bank]

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--instrument",
                "H1",
                "--min-instruments",
                "3",
                "--output-likelihood-file",
                str(output_file),
            ]

            with mock.patch.object(sys, "argv", test_args):
                with pytest.raises(ValueError, match="greater than the number"):
                    create_prior_diststats.parse_command_line()

    def test_parse_invalid_svd_file_extension(self, tmp_path):
        """Test that invalid SVD file extension raises ValueError."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.hdf5"  # Not xml
        output_file = tmp_path / "output.xml.gz"

        test_args = [
            "sgnl-create-prior-diststats",
            "--svd-file",
            str(svd_file),
            "--instrument",
            "H1",
            "--output-likelihood-file",
            str(output_file),
        ]

        with mock.patch.object(sys, "argv", test_args):
            with pytest.raises(ValueError, match="svd file cannot be read"):
                create_prior_diststats.parse_command_line()

    def test_parse_svd_file_xml_extension(self, tmp_path):
        """Test parsing with .xml extension (not .xml.gz)."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml"
        output_file = tmp_path / "output.xml.gz"

        mock_bank = MockSVDBank()

        with mock.patch(
            "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
        ) as mock_read:
            mock_read.return_value = [mock_bank]

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--instrument",
                "H1",
                "--min-instruments",
                "1",
                "--output-likelihood-file",
                str(output_file),
            ]

            with mock.patch.object(sys, "argv", test_args):
                options, params, template_ids, horizon_factors, lambda_eta = (
                    create_prior_diststats.parse_command_line()
                )

        assert len(template_ids) == 3

    def test_parse_zero_gamma_columns_raises_error(self, tmp_path):
        """Test that all-zero Gamma columns raise ValueError."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        # Create bank with all-zero Gamma values
        mock_bank = MockSVDBank()
        for row in mock_bank.sngl_inspiral_table:
            row.Gamma2 = 0
            row.Gamma3 = 0
            row.Gamma4 = 0
            row.Gamma5 = 0

        with mock.patch(
            "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
        ) as mock_read:
            mock_read.return_value = [mock_bank]

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--instrument",
                "H1",
                "--min-instruments",
                "1",
                "--output-likelihood-file",
                str(output_file),
            ]

            with mock.patch.object(sys, "argv", test_args):
                with pytest.raises(ValueError, match="is all zero"):
                    create_prior_diststats.parse_command_line()

    def test_parse_multiple_svd_files(self, tmp_path):
        """Test parsing with multiple SVD files."""
        svd_file1 = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        svd_file2 = tmp_path / "L1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        mock_bank1 = MockSVDBank(template_ids=[1, 2])
        mock_bank2 = MockSVDBank(template_ids=[3, 4])

        with mock.patch(
            "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
        ) as mock_read:
            # Return different banks for different files
            mock_read.side_effect = [[mock_bank1], [mock_bank2]]

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file1),
                "--svd-file",
                str(svd_file2),
                "--instrument",
                "H1",
                "--instrument",
                "L1",
                "--output-likelihood-file",
                str(output_file),
            ]

            with mock.patch.object(sys, "argv", test_args):
                options, params, template_ids, horizon_factors, lambda_eta = (
                    create_prior_diststats.parse_command_line()
                )

        # Should have unique template IDs from both banks
        assert len(template_ids) == 4
        assert set(template_ids) == {1, 2, 3, 4}


class TestMain:
    """Tests for main function."""

    def _create_mock_rankingstat(self):
        """Create a mock RankingStat object."""
        mock_stat = mock.MagicMock()
        mock_stat.terms = {"P_of_SNR_chisq": mock.MagicMock()}
        return mock_stat

    def _create_mock_config(self):
        """Create a mock analysis config."""
        return {
            "default": {
                "chi2_over_snr2_min": 0.0,
                "chi2_over_snr2_max": 100.0,
                "chi_bin_min": 0.0,
                "chi_bin_max": 10.0,
                "chi_bin_num": 100,
            },
            "ew": {
                "chi2_over_snr2_min": 0.0,
                "chi2_over_snr2_max": 50.0,
                "chi_bin_min": 0.0,
                "chi_bin_max": 5.0,
                "chi_bin_num": 50,
            },
        }

    def test_main_creates_likelihood(self, tmp_path):
        """Test that main correctly creates likelihood file."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        mock_bank = MockSVDBank()
        mock_stat = self._create_mock_rankingstat()
        mock_config = self._create_mock_config()

        with (
            mock.patch(
                "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
            ) as mock_read,
            mock.patch(
                "sgnl.bin.create_prior_diststats.get_analysis_config"
            ) as mock_get_config,
            mock.patch(
                "sgnl.bin.create_prior_diststats.likelihood_ratio.LnLikelihoodRatio"
            ) as mock_lr_class,
        ):
            mock_read.return_value = [mock_bank]
            mock_get_config.return_value = mock_config
            mock_lr_class.return_value = mock_stat

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--instrument",
                "H1",
                "--instrument",
                "L1",
                "--output-likelihood-file",
                str(output_file),
            ]

            with mock.patch.object(sys, "argv", test_args):
                create_prior_diststats.main()

        # Verify LnLikelihoodRatio was created
        mock_lr_class.assert_called_once()

        # Verify signal model was added
        mock_stat.terms[
            "P_of_SNR_chisq"
        ].add_snr_chisq_signal_model.assert_called_once()

        # Verify save was called
        mock_stat.save.assert_called_once()

    def test_main_with_ew_search(self, tmp_path):
        """Test main with 'ew' search option."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        mock_bank = MockSVDBank()
        mock_stat = self._create_mock_rankingstat()
        mock_config = self._create_mock_config()

        with (
            mock.patch(
                "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
            ) as mock_read,
            mock.patch(
                "sgnl.bin.create_prior_diststats.get_analysis_config"
            ) as mock_get_config,
            mock.patch(
                "sgnl.bin.create_prior_diststats.likelihood_ratio.LnLikelihoodRatio"
            ) as mock_lr_class,
        ):
            mock_read.return_value = [mock_bank]
            mock_get_config.return_value = mock_config
            mock_lr_class.return_value = mock_stat

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--instrument",
                "H1",
                "--min-instruments",
                "1",
                "--output-likelihood-file",
                str(output_file),
                "--search",
                "ew",
            ]

            with mock.patch.object(sys, "argv", test_args):
                create_prior_diststats.main()

        # Verify LnLikelihoodRatio was called with ew config values
        call_kwargs = mock_lr_class.call_args[1]
        assert call_kwargs["chi2_over_snr2_max"] == 50.0  # ew config value

    def test_main_with_seed_likelihood(self, tmp_path):
        """Test main with seed likelihood file."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"
        seed_file = tmp_path / "seed.xml.gz"

        mock_bank = MockSVDBank()
        mock_stat = self._create_mock_rankingstat()

        with (
            mock.patch(
                "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
            ) as mock_read,
            mock.patch(
                "sgnl.bin.create_prior_diststats.likelihood_ratio.LnLikelihoodRatio"
            ) as mock_lr_class,
        ):
            mock_read.return_value = [mock_bank]
            mock_lr_class.load.return_value = mock_stat

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--seed-likelihood",
                str(seed_file),
                "--output-likelihood-file",
                str(output_file),
            ]

            with mock.patch.object(sys, "argv", test_args):
                with pytest.warns(UserWarning):
                    create_prior_diststats.main()

        # Verify load was called instead of constructor
        mock_lr_class.load.assert_called_once()

    def test_main_with_seed_and_model_files(self, tmp_path):
        """Test main with seed and updated model files."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"
        seed_file = tmp_path / "seed.xml.gz"
        mass_model = tmp_path / "mass_model.hdf5"
        dtdphi = tmp_path / "dtdphi.hdf5"
        idq = tmp_path / "idq.hdf5"

        mock_bank = MockSVDBank()
        mock_stat = self._create_mock_rankingstat()

        with (
            mock.patch(
                "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
            ) as mock_read,
            mock.patch(
                "sgnl.bin.create_prior_diststats.likelihood_ratio.LnLikelihoodRatio"
            ) as mock_lr_class,
        ):
            mock_read.return_value = [mock_bank]
            mock_lr_class.load.return_value = mock_stat

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--seed-likelihood",
                str(seed_file),
                "--mass-model-file",
                str(mass_model),
                "--dtdphi-file",
                str(dtdphi),
                "--idq-file",
                str(idq),
                "--output-likelihood-file",
                str(output_file),
            ]

            with mock.patch.object(sys, "argv", test_args):
                with pytest.warns(UserWarning):
                    create_prior_diststats.main()

        # Verify model files were set on the loaded rankingstat
        assert mock_stat.population_model_file == str(mass_model)
        assert mock_stat.dtdphi_file == str(dtdphi)
        assert mock_stat.idq_file == str(idq)

    def test_main_writes_empty_rankingstatpdf(self, tmp_path):
        """Test main with write-empty-rankingstatpdf option."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"
        rankingstatpdf_file = tmp_path / "rankingstatpdf.xml.gz"

        mock_bank = MockSVDBank()
        mock_stat = self._create_mock_rankingstat()
        mock_config = self._create_mock_config()
        mock_pdf = mock.MagicMock()

        with (
            mock.patch(
                "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
            ) as mock_read,
            mock.patch(
                "sgnl.bin.create_prior_diststats.get_analysis_config"
            ) as mock_get_config,
            mock.patch(
                "sgnl.bin.create_prior_diststats.likelihood_ratio.LnLikelihoodRatio"
            ) as mock_lr_class,
            mock.patch(
                "sgnl.bin.create_prior_diststats.far.RankingStatPDF"
            ) as mock_pdf_class,
        ):
            mock_read.return_value = [mock_bank]
            mock_get_config.return_value = mock_config
            mock_lr_class.return_value = mock_stat
            mock_pdf_class.return_value = mock_pdf

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--instrument",
                "H1",
                "--min-instruments",
                "1",
                "--output-likelihood-file",
                str(output_file),
                "--write-empty-rankingstatpdf",
                str(rankingstatpdf_file),
            ]

            with mock.patch.object(sys, "argv", test_args):
                create_prior_diststats.main()

        # Verify RankingStatPDF was created and saved
        mock_pdf_class.assert_called_once_with(mock_stat, nsamples=0)
        mock_pdf.save.assert_called_once()

    def test_main_multiple_output_files(self, tmp_path):
        """Test main with multiple output likelihood files."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file1 = tmp_path / "output1.xml.gz"
        output_file2 = tmp_path / "output2.xml.gz"

        mock_bank = MockSVDBank()
        mock_stat = self._create_mock_rankingstat()
        mock_config = self._create_mock_config()

        with (
            mock.patch(
                "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
            ) as mock_read,
            mock.patch(
                "sgnl.bin.create_prior_diststats.get_analysis_config"
            ) as mock_get_config,
            mock.patch(
                "sgnl.bin.create_prior_diststats.likelihood_ratio.LnLikelihoodRatio"
            ) as mock_lr_class,
        ):
            mock_read.return_value = [mock_bank]
            mock_get_config.return_value = mock_config
            mock_lr_class.return_value = mock_stat

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--instrument",
                "H1",
                "--min-instruments",
                "1",
                "--output-likelihood-file",
                str(output_file1),
                "--output-likelihood-file",
                str(output_file2),
            ]

            with mock.patch.object(sys, "argv", test_args):
                create_prior_diststats.main()

        # Verify save was called twice (once for each output file)
        assert mock_stat.save.call_count == 2

    def test_main_with_verbose(self, tmp_path):
        """Test main with verbose flag."""
        svd_file = tmp_path / "H1-SVD_BANK-0-100.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        mock_bank = MockSVDBank()
        mock_stat = self._create_mock_rankingstat()
        mock_config = self._create_mock_config()

        with (
            mock.patch(
                "sgnl.bin.create_prior_diststats.svd_bank.read_banks"
            ) as mock_read,
            mock.patch(
                "sgnl.bin.create_prior_diststats.get_analysis_config"
            ) as mock_get_config,
            mock.patch(
                "sgnl.bin.create_prior_diststats.likelihood_ratio.LnLikelihoodRatio"
            ) as mock_lr_class,
        ):
            mock_read.return_value = [mock_bank]
            mock_get_config.return_value = mock_config
            mock_lr_class.return_value = mock_stat

            test_args = [
                "sgnl-create-prior-diststats",
                "--svd-file",
                str(svd_file),
                "--instrument",
                "H1",
                "--min-instruments",
                "1",
                "--output-likelihood-file",
                str(output_file),
                "-v",
            ]

            with mock.patch.object(sys, "argv", test_args):
                create_prior_diststats.main()

        # Verify verbose was passed to save
        save_call_kwargs = mock_stat.save.call_args[1]
        assert save_call_kwargs["verbose"] is True

"""Unit tests for sgnl.bin.assign_far module."""

import pathlib
import sys
from unittest import mock

import pytest

from sgnl import sgnlio
from sgnl.bin import assign_far

PATH_SCHEMA = pathlib.Path(__file__).parent.parent / "config" / "cbc_db.yaml"


@pytest.fixture
def input_database_with_events(tmp_path):
    """Create a temporary input database with events for testing."""
    db = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=":memory:")

    # Insert initial process row
    db.default_cursor.execute(
        """
        INSERT INTO process (program, start_time, end_time, ifos,
                             is_online, node, username, unix_procid)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        ("initial_program", 0, 0, "H1,L1", 0, "test_node", "test_user", 12345),
    )

    # Insert a filter row
    db.default_cursor.execute(
        """
        INSERT INTO filter (_filter_id, bank_id, subbank_id, end_time_delta,
                           mass1, mass2, spin1x, spin1y, spin1z,
                           spin2x, spin2y, spin2z, template_duration)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (1, 0, 0, 0, 1.4, 1.4, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1000000000),
    )

    # Insert trigger and event using the stillsuit interface
    # Time in nanoseconds
    event_time = 1000000000 * 10**9

    db.insert_event(
        {
            "trigger": [
                {
                    "_filter_id": 1,
                    "epoch_start": event_time - 10**9,
                    "epoch_end": event_time + 10**9,
                    "ifo": "H1",
                    "time": event_time,
                    "snr": 10.0,
                    "chisq": 1.0,
                    "phase": 0.0,
                    "chisq_weighted_snr": 10.0,
                },
            ],
            "event": {
                "time": event_time,
                "network_snr": 10.0,
                "network_chisq_weighted_snr": 10.0,
                "likelihood": 5.0,  # Some likelihood value
                "false_alarm_probability": None,
                "combined_far": None,
            },
        },
        ignore_sim=True,
    )

    db.flush()

    input_path = tmp_path / "input.db"
    db.to_file(str(input_path))

    return input_path


@pytest.fixture
def input_database_already_processed(tmp_path):
    """Create a database that has already been processed by assign_far."""
    db = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=":memory:")

    # Insert process row with the program name already set
    db.default_cursor.execute(
        """
        INSERT INTO process (program, start_time, end_time, ifos,
                             is_online, node, username, unix_procid)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        ("sgnl-assign-far", 0, 0, "H1,L1", 0, "test_node", "test_user", 12345),
    )

    # Insert a filter row
    db.default_cursor.execute(
        """
        INSERT INTO filter (_filter_id, bank_id, subbank_id, end_time_delta,
                           mass1, mass2, spin1x, spin1y, spin1z,
                           spin2x, spin2y, spin2z, template_duration)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (1, 0, 0, 0, 1.4, 1.4, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1000000000),
    )

    db.flush()

    input_path = tmp_path / "already_processed.db"
    db.to_file(str(input_path))

    return input_path


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_parse_required_args(self, tmp_path):
        """Test parsing with required arguments."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"
        ranking_pdf = tmp_path / "ranking.xml.gz"

        test_args = [
            "sgnl-assign-far",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            "--input-rankingstatpdf-file",
            str(ranking_pdf),
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, process_params = assign_far.parse_command_line()

        assert options.input_database_file == [str(input_db)]
        assert options.output_database_file == [str(output_db)]
        assert options.input_rankingstatpdf_file == str(ranking_pdf)
        assert options.force is False
        assert options.verbose is False

    def test_parse_multiple_databases(self, tmp_path):
        """Test parsing with multiple input/output databases."""
        input_db1 = tmp_path / "input1.db"
        input_db1.touch()
        input_db2 = tmp_path / "input2.db"
        input_db2.touch()
        output_db1 = tmp_path / "output1.db"
        output_db2 = tmp_path / "output2.db"
        ranking_pdf = tmp_path / "ranking.xml.gz"

        test_args = [
            "sgnl-assign-far",
            "-i",
            str(input_db1),
            str(input_db2),
            "-o",
            str(output_db1),
            str(output_db2),
            "--input-rankingstatpdf-file",
            str(ranking_pdf),
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_far.parse_command_line()

        assert options.input_database_file == [str(input_db1), str(input_db2)]
        assert options.output_database_file == [str(output_db1), str(output_db2)]

    def test_parse_with_input_cache(self, tmp_path):
        """Test parsing with input database cache file."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"
        ranking_pdf = tmp_path / "ranking.xml.gz"

        # Create cache file
        cache_file = tmp_path / "input.cache"
        cache_db = tmp_path / "cached_input.db"
        cache_db.touch()
        # LAL cache format: observatory description start duration path
        cache_file.write_text(f"H H1_TEST 1000000000 1000 file://localhost{cache_db}\n")

        test_args = [
            "sgnl-assign-far",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            str(tmp_path / "cached_output.db"),
            "--input-database-cache",
            str(cache_file),
            "--input-rankingstatpdf-file",
            str(ranking_pdf),
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_far.parse_command_line()

        # Should have both direct and cache inputs flattened
        assert str(input_db) in options.input_database_file
        assert str(cache_db) in options.input_database_file
        assert len(options.input_database_file) == 2

    def test_parse_with_output_cache(self, tmp_path):
        """Test parsing with output database cache file."""
        input_db1 = tmp_path / "input1.db"
        input_db1.touch()
        input_db2 = tmp_path / "input2.db"
        input_db2.touch()
        output_db = tmp_path / "output.db"
        ranking_pdf = tmp_path / "ranking.xml.gz"

        # Create output cache file
        cache_file = tmp_path / "output.cache"
        cached_output = tmp_path / "cached_output.db"
        cache_file.write_text(
            f"H H1_TEST 1000000000 1000 file://localhost{cached_output}\n"
        )

        test_args = [
            "sgnl-assign-far",
            "-i",
            str(input_db1),
            str(input_db2),
            "-o",
            str(output_db),
            "--output-database-cache",
            str(cache_file),
            "--input-rankingstatpdf-file",
            str(ranking_pdf),
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_far.parse_command_line()

        # Should have both direct and cache outputs flattened
        assert str(output_db) in options.output_database_file
        assert str(cached_output) in options.output_database_file
        assert len(options.output_database_file) == 2

    def test_parse_with_verbose_and_force(self, tmp_path):
        """Test parsing with verbose and force flags."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"
        ranking_pdf = tmp_path / "ranking.xml.gz"

        test_args = [
            "sgnl-assign-far",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            "--input-rankingstatpdf-file",
            str(ranking_pdf),
            "-v",
            "-f",
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_far.parse_command_line()

        assert options.verbose is True
        assert options.force is True

    def test_parse_with_config_schema_and_tmp_space(self, tmp_path):
        """Test parsing with config schema and tmp space options."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"
        ranking_pdf = tmp_path / "ranking.xml.gz"
        schema = tmp_path / "schema.yaml"

        test_args = [
            "sgnl-assign-far",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            "--input-rankingstatpdf-file",
            str(ranking_pdf),
            "-s",
            str(schema),
            "--tmp-space",
            str(tmp_path / "tmp_space"),
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_far.parse_command_line()

        assert options.config_schema == str(schema)
        assert options.tmp_space == str(tmp_path / "tmp_space")

    def test_parse_missing_rankingstatpdf(self, tmp_path):
        """Test that missing ranking stat pdf raises ValueError."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"

        test_args = [
            "sgnl-assign-far",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
        ]

        with mock.patch.object(sys, "argv", test_args):
            with pytest.raises(
                ValueError, match="must set --input-rankingstatpdf-file"
            ):
                assign_far.parse_command_line()

    def test_parse_missing_input_database(self, tmp_path):
        """Test that missing input database raises ValueError."""
        ranking_pdf = tmp_path / "ranking.xml.gz"

        test_args = [
            "sgnl-assign-far",
            "--input-rankingstatpdf-file",
            str(ranking_pdf),
        ]

        with mock.patch.object(sys, "argv", test_args):
            with pytest.raises(
                ValueError, match="must provide at least one database file"
            ):
                assign_far.parse_command_line()

    def test_parse_output_exists_raises_error(self, tmp_path):
        """Test that existing output file raises ValueError."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"
        output_db.touch()  # Create existing output
        ranking_pdf = tmp_path / "ranking.xml.gz"

        test_args = [
            "sgnl-assign-far",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            "--input-rankingstatpdf-file",
            str(ranking_pdf),
        ]

        with mock.patch.object(sys, "argv", test_args):
            with pytest.raises(ValueError, match="already exists"):
                assign_far.parse_command_line()

    def test_parse_mismatched_database_counts(self, tmp_path):
        """Test that mismatched input/output counts raises ValueError."""
        input_db1 = tmp_path / "input1.db"
        input_db1.touch()
        input_db2 = tmp_path / "input2.db"
        input_db2.touch()
        output_db = tmp_path / "output.db"
        ranking_pdf = tmp_path / "ranking.xml.gz"

        test_args = [
            "sgnl-assign-far",
            "-i",
            str(input_db1),
            str(input_db2),
            "-o",
            str(output_db),
            "--input-rankingstatpdf-file",
            str(ranking_pdf),
        ]

        with mock.patch.object(sys, "argv", test_args):
            with pytest.raises(ValueError, match="number of each given database"):
                assign_far.parse_command_line()


class TestMain:
    """Tests for main function."""

    def _create_mock_fapfar(self):
        """Create a mock FAPFAR object that returns predictable FAR values."""
        mock_fapfar = mock.MagicMock()
        # Return a FAR of 1e-6 for any likelihood
        mock_fapfar.far_from_rank.return_value = 1e-6
        return mock_fapfar

    def _create_mock_rankingstatpdf(self):
        """Create a mock RankingStatPDF object."""
        mock_pdf = mock.MagicMock()
        # Make the zero_lag check pass (not all zeros)
        mock_pdf.zero_lag_lr_lnpdf.array.__eq__ = mock.MagicMock(
            return_value=mock.MagicMock(all=mock.MagicMock(return_value=False))
        )
        return mock_pdf

    def test_main_assigns_far(self, input_database_with_events, tmp_path):
        """Test that main correctly assigns FAR to events."""
        output_db = tmp_path / "output.db"

        mock_pdf = self._create_mock_rankingstatpdf()
        mock_fapfar = self._create_mock_fapfar()

        with (
            mock.patch("sgnl.bin.assign_far.far.RankingStatPDF") as mock_pdf_class,
            mock.patch("sgnl.bin.assign_far.far.FAPFAR") as mock_fapfar_class,
        ):
            mock_pdf_class.load.return_value = mock_pdf
            mock_fapfar_class.return_value = mock_fapfar

            test_args = [
                "sgnl-assign-far",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_database_with_events),
                "-o",
                str(output_db),
                "--input-rankingstatpdf-file",
                "fake_ranking.xml.gz",
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_far.main()

        # Verify output exists
        assert output_db.exists()

        # Load output and check FAR was assigned
        outdb = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=str(output_db))
        events = list(outdb.default_cursor.execute("SELECT * FROM event;"))
        assert len(events) == 1
        event = dict(events[0])
        # FAR should be the value we mocked (1e-6)
        assert event["combined_far"] == pytest.approx(1e-6)

        # Verify far_from_rank was called
        mock_fapfar.far_from_rank.assert_called()

    def test_main_with_verbose(self, input_database_with_events, tmp_path, capsys):
        """Test main with verbose output."""
        output_db = tmp_path / "output.db"

        mock_pdf = self._create_mock_rankingstatpdf()
        mock_fapfar = self._create_mock_fapfar()

        with (
            mock.patch("sgnl.bin.assign_far.far.RankingStatPDF") as mock_pdf_class,
            mock.patch("sgnl.bin.assign_far.far.FAPFAR") as mock_fapfar_class,
        ):
            mock_pdf_class.load.return_value = mock_pdf
            mock_fapfar_class.return_value = mock_fapfar

            test_args = [
                "sgnl-assign-far",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_database_with_events),
                "-o",
                str(output_db),
                "--input-rankingstatpdf-file",
                "fake_ranking.xml.gz",
                "-v",
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_far.main()

        captured = capsys.readouterr()
        assert "assigning FARs" in captured.err
        assert "FAR assignment complete" in captured.err
        assert "Done" in captured.err

    def test_main_skips_already_processed(
        self, input_database_already_processed, tmp_path, capsys
    ):
        """Test that main skips databases already processed."""
        output_db = tmp_path / "output.db"

        mock_pdf = self._create_mock_rankingstatpdf()
        mock_fapfar = self._create_mock_fapfar()

        with (
            mock.patch("sgnl.bin.assign_far.far.RankingStatPDF") as mock_pdf_class,
            mock.patch("sgnl.bin.assign_far.far.FAPFAR") as mock_fapfar_class,
        ):
            mock_pdf_class.load.return_value = mock_pdf
            mock_fapfar_class.return_value = mock_fapfar

            test_args = [
                "sgnl-assign-far",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_database_already_processed),
                "-o",
                str(output_db),
                "--input-rankingstatpdf-file",
                "fake_ranking.xml.gz",
                "-v",
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_far.main()

        captured = capsys.readouterr()
        assert "already processed, skipping" in captured.err
        # Output file should NOT be created because it was skipped
        assert not output_db.exists()

    def test_main_force_reprocesses(self, tmp_path):
        """Test that --force flag reprocesses already processed databases."""
        # Create a database that's already been processed
        db = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=":memory:")
        db.default_cursor.execute(
            """
            INSERT INTO process (program, start_time, end_time, ifos,
                                 is_online, node, username, unix_procid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sgnl-assign-far", 0, 0, "H1,L1", 0, "test_node", "test_user", 12345),
        )
        db.default_cursor.execute(
            """
            INSERT INTO filter (_filter_id, bank_id, subbank_id, end_time_delta,
                               mass1, mass2, spin1x, spin1y, spin1z,
                               spin2x, spin2y, spin2z, template_duration)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (1, 0, 0, 0, 1.4, 1.4, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1000000000),
        )
        db.insert_event(
            {
                "trigger": [
                    {
                        "_filter_id": 1,
                        "epoch_start": 0,
                        "epoch_end": 2 * 10**9,
                        "ifo": "H1",
                        "time": 10**9,
                        "snr": 10.0,
                        "chisq": 1.0,
                        "phase": 0.0,
                        "chisq_weighted_snr": 10.0,
                    },
                ],
                "event": {
                    "time": 10**9,
                    "network_snr": 10.0,
                    "network_chisq_weighted_snr": 10.0,
                    "likelihood": 5.0,
                    "false_alarm_probability": None,
                    "combined_far": None,
                },
            },
            ignore_sim=True,
        )
        db.flush()
        input_path = tmp_path / "input_already_processed.db"
        db.to_file(str(input_path))

        output_db = tmp_path / "output.db"

        mock_pdf = self._create_mock_rankingstatpdf()
        mock_fapfar = self._create_mock_fapfar()

        with (
            mock.patch("sgnl.bin.assign_far.far.RankingStatPDF") as mock_pdf_class,
            mock.patch("sgnl.bin.assign_far.far.FAPFAR") as mock_fapfar_class,
        ):
            mock_pdf_class.load.return_value = mock_pdf
            mock_fapfar_class.return_value = mock_fapfar

            test_args = [
                "sgnl-assign-far",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_path),
                "-o",
                str(output_db),
                "--input-rankingstatpdf-file",
                "fake_ranking.xml.gz",
                "-f",  # Force flag
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_far.main()

        # With force, output should be created
        assert output_db.exists()

    def test_main_updates_process_params(self, input_database_with_events, tmp_path):
        """Test that main records process parameters."""
        output_db = tmp_path / "output.db"

        mock_pdf = self._create_mock_rankingstatpdf()
        mock_fapfar = self._create_mock_fapfar()

        with (
            mock.patch("sgnl.bin.assign_far.far.RankingStatPDF") as mock_pdf_class,
            mock.patch("sgnl.bin.assign_far.far.FAPFAR") as mock_fapfar_class,
        ):
            mock_pdf_class.load.return_value = mock_pdf
            mock_fapfar_class.return_value = mock_fapfar

            test_args = [
                "sgnl-assign-far",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_database_with_events),
                "-o",
                str(output_db),
                "--input-rankingstatpdf-file",
                "fake_ranking.xml.gz",
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_far.main()

        # Check process params
        outdb = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=str(output_db))
        params = {
            dict(row)["param"]: dict(row)["value"]
            for row in outdb.default_cursor.execute("SELECT * FROM process_params;")
        }

        assert "input_rankingstatpdf_file" in params
        assert "input_database_file" in params

    def test_main_zero_lag_histogram_error(self, input_database_with_events, tmp_path):
        """Test that main raises error when zero_lag histogram is all zeros."""
        output_db = tmp_path / "output.db"

        # Create a mock RankingStatPDF with zero histogram
        with mock.patch("sgnl.bin.assign_far.far.RankingStatPDF") as mock_pdf_class:
            mock_pdf = mock.MagicMock()
            mock_pdf.zero_lag_lr_lnpdf.array.__eq__ = mock.MagicMock(
                return_value=mock.MagicMock(all=mock.MagicMock(return_value=True))
            )
            mock_pdf_class.load.return_value = mock_pdf

            test_args = [
                "sgnl-assign-far",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_database_with_events),
                "-o",
                str(output_db),
                "--input-rankingstatpdf-file",
                "fake_ranking.xml.gz",
            ]

            with mock.patch.object(sys, "argv", test_args):
                with pytest.raises(ValueError, match="zerolag histogram is not stored"):
                    assign_far.main()

    def test_main_multiple_databases(self, tmp_path):
        """Test main with multiple input/output databases."""
        # Create two input databases
        db1 = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=":memory:")
        db1.default_cursor.execute(
            """
            INSERT INTO process (program, start_time, end_time, ifos,
                                 is_online, node, username, unix_procid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("initial_program", 0, 0, "H1,L1", 0, "test_node", "test_user", 12345),
        )
        db1.default_cursor.execute(
            """
            INSERT INTO filter (_filter_id, bank_id, subbank_id, end_time_delta,
                               mass1, mass2, spin1x, spin1y, spin1z,
                               spin2x, spin2y, spin2z, template_duration)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (1, 0, 0, 0, 1.4, 1.4, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1000000000),
        )
        db1.insert_event(
            {
                "trigger": [
                    {
                        "_filter_id": 1,
                        "epoch_start": 0,
                        "epoch_end": 2 * 10**9,
                        "ifo": "H1",
                        "time": 10**9,
                        "snr": 10.0,
                        "chisq": 1.0,
                        "phase": 0.0,
                        "chisq_weighted_snr": 10.0,
                    },
                ],
                "event": {
                    "time": 10**9,
                    "network_snr": 10.0,
                    "network_chisq_weighted_snr": 10.0,
                    "likelihood": 5.0,
                    "false_alarm_probability": None,
                    "combined_far": None,
                },
            },
            ignore_sim=True,
        )
        db1.flush()
        input1 = tmp_path / "input1.db"
        db1.to_file(str(input1))

        db2 = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=":memory:")
        db2.default_cursor.execute(
            """
            INSERT INTO process (program, start_time, end_time, ifos,
                                 is_online, node, username, unix_procid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("initial_program", 0, 0, "H1,L1", 0, "test_node", "test_user", 12345),
        )
        db2.default_cursor.execute(
            """
            INSERT INTO filter (_filter_id, bank_id, subbank_id, end_time_delta,
                               mass1, mass2, spin1x, spin1y, spin1z,
                               spin2x, spin2y, spin2z, template_duration)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (1, 0, 0, 0, 1.4, 1.4, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1000000000),
        )
        db2.insert_event(
            {
                "trigger": [
                    {
                        "_filter_id": 1,
                        "epoch_start": 0,
                        "epoch_end": 2 * 10**9,
                        "ifo": "L1",
                        "time": 2 * 10**9,
                        "snr": 8.0,
                        "chisq": 1.0,
                        "phase": 0.0,
                        "chisq_weighted_snr": 8.0,
                    },
                ],
                "event": {
                    "time": 2 * 10**9,
                    "network_snr": 8.0,
                    "network_chisq_weighted_snr": 8.0,
                    "likelihood": 3.0,
                    "false_alarm_probability": None,
                    "combined_far": None,
                },
            },
            ignore_sim=True,
        )
        db2.flush()
        input2 = tmp_path / "input2.db"
        db2.to_file(str(input2))

        output1 = tmp_path / "output1.db"
        output2 = tmp_path / "output2.db"

        mock_pdf = self._create_mock_rankingstatpdf()
        mock_fapfar = self._create_mock_fapfar()

        with (
            mock.patch("sgnl.bin.assign_far.far.RankingStatPDF") as mock_pdf_class,
            mock.patch("sgnl.bin.assign_far.far.FAPFAR") as mock_fapfar_class,
        ):
            mock_pdf_class.load.return_value = mock_pdf
            mock_fapfar_class.return_value = mock_fapfar

            test_args = [
                "sgnl-assign-far",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input1),
                str(input2),
                "-o",
                str(output1),
                str(output2),
                "--input-rankingstatpdf-file",
                "fake_ranking.xml.gz",
                "-v",
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_far.main()

        # Both outputs should exist and have FARs assigned
        assert output1.exists()
        assert output2.exists()

        outdb1 = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=str(output1))
        events1 = list(outdb1.default_cursor.execute("SELECT * FROM event;"))
        assert dict(events1[0])["combined_far"] == pytest.approx(1e-6)

        outdb2 = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=str(output2))
        events2 = list(outdb2.default_cursor.execute("SELECT * FROM event;"))
        assert dict(events2[0])["combined_far"] == pytest.approx(1e-6)

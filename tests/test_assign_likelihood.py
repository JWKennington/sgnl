"""Unit tests for sgnl.bin.assign_likelihood module."""

import pathlib
import sys
from unittest import mock

import pytest
from igwn_segments import segment, segmentlist, segmentlistdict

from sgnl import sgnlio
from sgnl.bin import assign_likelihood

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
                "likelihood": 0.0,
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
    """Create a database that has already been processed by assign_likelihood."""
    db = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=":memory:")

    # Insert process row with the program name already set
    db.default_cursor.execute(
        """
        INSERT INTO process (program, start_time, end_time, ifos,
                             is_online, node, username, unix_procid)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        ("sgnl-assign-likelihood", 0, 0, "H1,L1", 0, "test_node", "test_user", 12345),
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
        likelihood_file = tmp_path / "likelihood.xml.gz"

        test_args = [
            "sgnl-assign-likelihood",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            "-l",
            str(likelihood_file),
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, process_params = assign_likelihood.parse_command_line()

        assert options.input_database_file == [str(input_db)]
        assert options.output_database_file == [str(output_db)]
        assert options.input_likelihood_files == [str(likelihood_file)]
        assert options.force is False
        assert options.verbose is False
        assert options.gstlal is False
        assert options.add_zerolag_to_background is False

    def test_parse_multiple_databases(self, tmp_path):
        """Test parsing with multiple input/output databases."""
        input_db1 = tmp_path / "input1.db"
        input_db1.touch()
        input_db2 = tmp_path / "input2.db"
        input_db2.touch()
        output_db1 = tmp_path / "output1.db"
        output_db2 = tmp_path / "output2.db"
        likelihood_file = tmp_path / "likelihood.xml.gz"

        test_args = [
            "sgnl-assign-likelihood",
            "-i",
            str(input_db1),
            str(input_db2),
            "-o",
            str(output_db1),
            str(output_db2),
            "-l",
            str(likelihood_file),
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_likelihood.parse_command_line()

        assert options.input_database_file == [str(input_db1), str(input_db2)]
        assert options.output_database_file == [str(output_db1), str(output_db2)]

    def test_parse_multiple_likelihood_files(self, tmp_path):
        """Test parsing with multiple likelihood files."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"
        likelihood1 = tmp_path / "likelihood1.xml.gz"
        likelihood2 = tmp_path / "likelihood2.xml.gz"

        test_args = [
            "sgnl-assign-likelihood",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            "-l",
            str(likelihood1),
            str(likelihood2),
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_likelihood.parse_command_line()

        assert str(likelihood1) in options.input_likelihood_files
        assert str(likelihood2) in options.input_likelihood_files

    def test_parse_with_input_cache(self, tmp_path):
        """Test parsing with input database cache file."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"
        likelihood_file = tmp_path / "likelihood.xml.gz"

        # Create cache file
        cache_file = tmp_path / "input.cache"
        cache_db = tmp_path / "cached_input.db"
        cache_db.touch()
        cache_file.write_text(f"H H1_TEST 1000000000 1000 file://localhost{cache_db}\n")

        test_args = [
            "sgnl-assign-likelihood",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            str(tmp_path / "cached_output.db"),
            "--input-database-cache",
            str(cache_file),
            "-l",
            str(likelihood_file),
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_likelihood.parse_command_line()

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
        likelihood_file = tmp_path / "likelihood.xml.gz"

        # Create output cache file
        cache_file = tmp_path / "output.cache"
        cached_output = tmp_path / "cached_output.db"
        cache_file.write_text(
            f"H H1_TEST 1000000000 1000 file://localhost{cached_output}\n"
        )

        test_args = [
            "sgnl-assign-likelihood",
            "-i",
            str(input_db1),
            str(input_db2),
            "-o",
            str(output_db),
            "--output-database-cache",
            str(cache_file),
            "-l",
            str(likelihood_file),
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_likelihood.parse_command_line()

        assert str(output_db) in options.output_database_file
        assert str(cached_output) in options.output_database_file
        assert len(options.output_database_file) == 2

    def test_parse_with_likelihood_cache(self, tmp_path):
        """Test parsing with likelihood cache file."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"

        # Create likelihood cache file
        cache_file = tmp_path / "likelihood.cache"
        cached_likelihood = tmp_path / "cached_likelihood.xml.gz"
        cache_file.write_text(
            f"H H1_TEST 1000000000 1000 file://localhost{cached_likelihood}\n"
        )

        test_args = [
            "sgnl-assign-likelihood",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            "--input-likelihood-cache",
            str(cache_file),
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_likelihood.parse_command_line()

        assert f"file://localhost{cached_likelihood}" in options.input_likelihood_files

    def test_parse_with_all_flags(self, tmp_path):
        """Test parsing with verbose, force, gstlal, and add-zerolag flags."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"
        likelihood_file = tmp_path / "likelihood.xml.gz"

        test_args = [
            "sgnl-assign-likelihood",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            "-l",
            str(likelihood_file),
            "-v",
            "-f",
            "--gstlal",
            "--add-zerolag-to-background",
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_likelihood.parse_command_line()

        assert options.verbose is True
        assert options.force is True
        assert options.gstlal is True
        assert options.add_zerolag_to_background is True

    def test_parse_with_config_and_tmp_space(self, tmp_path):
        """Test parsing with config schema and tmp space options."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"
        likelihood_file = tmp_path / "likelihood.xml.gz"
        schema = tmp_path / "schema.yaml"

        test_args = [
            "sgnl-assign-likelihood",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            "-l",
            str(likelihood_file),
            "-s",
            str(schema),
            "-t",
            str(tmp_path / "tmp_space"),
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_likelihood.parse_command_line()

        assert options.config_schema == str(schema)
        assert options.tmp_space == str(tmp_path / "tmp_space")

    def test_parse_with_vetoes_name(self, tmp_path):
        """Test parsing with vetoes name option."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"
        likelihood_file = tmp_path / "likelihood.xml.gz"

        test_args = [
            "sgnl-assign-likelihood",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            "-l",
            str(likelihood_file),
            "--vetoes-name",
            "vetoes",
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_likelihood.parse_command_line()

        assert options.vetoes_name == "vetoes"

    def test_parse_with_verbose_level(self, tmp_path):
        """Test parsing with verbose level option."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"
        likelihood_file = tmp_path / "likelihood.xml.gz"

        test_args = [
            "sgnl-assign-likelihood",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            "-l",
            str(likelihood_file),
            "--verbose-level",
            "DEBUG",
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = assign_likelihood.parse_command_line()

        assert options.verbose_level == "DEBUG"

    def test_parse_missing_likelihood(self, tmp_path):
        """Test that missing likelihood file raises ValueError."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"

        test_args = [
            "sgnl-assign-likelihood",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
        ]

        with mock.patch.object(sys, "argv", test_args):
            with pytest.raises(ValueError, match="no likelihood URLs specified"):
                assign_likelihood.parse_command_line()

    def test_parse_output_exists_raises_error(self, tmp_path):
        """Test that existing output file raises ValueError."""
        input_db = tmp_path / "input.db"
        input_db.touch()
        output_db = tmp_path / "output.db"
        output_db.touch()  # Create existing output
        likelihood_file = tmp_path / "likelihood.xml.gz"

        test_args = [
            "sgnl-assign-likelihood",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            "-l",
            str(likelihood_file),
        ]

        with mock.patch.object(sys, "argv", test_args):
            with pytest.raises(ValueError, match="already exists"):
                assign_likelihood.parse_command_line()

    def test_parse_mismatched_database_counts(self, tmp_path):
        """Test that mismatched input/output counts raises ValueError."""
        input_db1 = tmp_path / "input1.db"
        input_db1.touch()
        input_db2 = tmp_path / "input2.db"
        input_db2.touch()
        output_db = tmp_path / "output.db"
        likelihood_file = tmp_path / "likelihood.xml.gz"

        test_args = [
            "sgnl-assign-likelihood",
            "-i",
            str(input_db1),
            str(input_db2),
            "-o",
            str(output_db),
            "-l",
            str(likelihood_file),
        ]

        with mock.patch.object(sys, "argv", test_args):
            with pytest.raises(ValueError, match="number of each given databases"):
                assign_likelihood.parse_command_line()


class TestTriggerVetoFunc:
    """Tests for trigger_veto_func function."""

    def test_trigger_vetoed(self):
        """Test that a trigger in veto segment is vetoed."""
        trigger = {"ifo": "H1", "time": 1000000500}
        vetoseglists = segmentlistdict()
        vetoseglists["H1"] = segmentlist([segment(1000000000, 1000001000)])

        result = assign_likelihood.trigger_veto_func(trigger, vetoseglists)
        assert result is True

    def test_trigger_not_vetoed(self):
        """Test that a trigger outside veto segment is not vetoed."""
        trigger = {"ifo": "H1", "time": 1000002000}
        vetoseglists = segmentlistdict()
        vetoseglists["H1"] = segmentlist([segment(1000000000, 1000001000)])

        result = assign_likelihood.trigger_veto_func(trigger, vetoseglists)
        assert result is False

    def test_trigger_ifo_not_in_vetoes(self):
        """Test that a trigger with ifo not in vetoes is not vetoed."""
        trigger = {"ifo": "L1", "time": 1000000500}
        vetoseglists = segmentlistdict()
        vetoseglists["H1"] = segmentlist([segment(1000000000, 1000001000)])

        result = assign_likelihood.trigger_veto_func(trigger, vetoseglists)
        assert result is False

    def test_trigger_empty_vetoes(self):
        """Test that a trigger with empty vetoes is not vetoed."""
        trigger = {"ifo": "H1", "time": 1000000500}
        vetoseglists = segmentlistdict()

        result = assign_likelihood.trigger_veto_func(trigger, vetoseglists)
        assert result is False


class TestMain:
    """Tests for main function."""

    def _create_mock_rankingstat(self):
        """Create a mock RankingStat object."""
        mock_stat = mock.MagicMock()
        mock_stat.instruments = ["H1", "L1"]
        mock_stat.ln_lr_from_triggers.return_value = 5.0
        return mock_stat

    def test_main_assigns_likelihood(self, input_database_with_events, tmp_path):
        """Test that main correctly assigns likelihood to events."""
        output_db = tmp_path / "output.db"

        mock_stat = self._create_mock_rankingstat()

        with mock.patch(
            "sgnl.bin.assign_likelihood.far.marginalize_pdf_urls"
        ) as mock_marginalize:
            mock_marginalize.return_value = mock_stat

            test_args = [
                "sgnl-assign-likelihood",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_database_with_events),
                "-o",
                str(output_db),
                "-l",
                "fake_likelihood.xml.gz",
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_likelihood.main()

        # Verify output exists
        assert output_db.exists()

        # Load output and check likelihood was assigned
        outdb = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=str(output_db))
        events = list(outdb.default_cursor.execute("SELECT * FROM event;"))
        assert len(events) == 1
        event = dict(events[0])
        # Likelihood should be the value we mocked (5.0)
        assert event["likelihood"] == pytest.approx(5.0)

        # Verify ln_lr_from_triggers was called
        mock_stat.ln_lr_from_triggers.assert_called()

    def test_main_with_verbose(self, input_database_with_events, tmp_path, capsys):
        """Test main with verbose output."""
        output_db = tmp_path / "output.db"

        mock_stat = self._create_mock_rankingstat()

        with mock.patch(
            "sgnl.bin.assign_likelihood.far.marginalize_pdf_urls"
        ) as mock_marginalize:
            mock_marginalize.return_value = mock_stat

            test_args = [
                "sgnl-assign-likelihood",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_database_with_events),
                "-o",
                str(output_db),
                "-l",
                "fake_likelihood.xml.gz",
                "-v",
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_likelihood.main()

        captured = capsys.readouterr()
        assert "1/1:" in captured.err
        assert "likelihood assignment complete" in captured.err
        assert "Done" in captured.err

    def test_main_with_debug_verbose_level(
        self, input_database_with_events, tmp_path, capsys
    ):
        """Test main with DEBUG verbose level."""
        output_db = tmp_path / "output.db"

        mock_stat = self._create_mock_rankingstat()

        with mock.patch(
            "sgnl.bin.assign_likelihood.far.marginalize_pdf_urls"
        ) as mock_marginalize:
            mock_marginalize.return_value = mock_stat

            test_args = [
                "sgnl-assign-likelihood",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_database_with_events),
                "-o",
                str(output_db),
                "-l",
                "fake_likelihood.xml.gz",
                "--verbose-level",
                "DEBUG",
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_likelihood.main()

        captured = capsys.readouterr()
        assert "dumping" in captured.err or output_db.exists()

    def test_main_skips_already_processed(
        self, input_database_already_processed, tmp_path, capsys
    ):
        """Test that main skips databases already processed."""
        output_db = tmp_path / "output.db"

        mock_stat = self._create_mock_rankingstat()

        with mock.patch(
            "sgnl.bin.assign_likelihood.far.marginalize_pdf_urls"
        ) as mock_marginalize:
            mock_marginalize.return_value = mock_stat

            test_args = [
                "sgnl-assign-likelihood",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_database_already_processed),
                "-o",
                str(output_db),
                "-l",
                "fake_likelihood.xml.gz",
                "-v",
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_likelihood.main()

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
            (
                "sgnl-assign-likelihood",
                0,
                0,
                "H1,L1",
                0,
                "test_node",
                "test_user",
                12345,
            ),
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
                    "likelihood": 0.0,
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

        mock_stat = self._create_mock_rankingstat()

        with mock.patch(
            "sgnl.bin.assign_likelihood.far.marginalize_pdf_urls"
        ) as mock_marginalize:
            mock_marginalize.return_value = mock_stat

            test_args = [
                "sgnl-assign-likelihood",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_path),
                "-o",
                str(output_db),
                "-l",
                "fake_likelihood.xml.gz",
                "-f",  # Force flag
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_likelihood.main()

        # With force, output should be created
        assert output_db.exists()

    def test_main_with_add_zerolag_to_background(
        self, input_database_with_events, tmp_path
    ):
        """Test main with add-zerolag-to-background flag."""
        output_db = tmp_path / "output.db"

        mock_stat = self._create_mock_rankingstat()

        with mock.patch(
            "sgnl.bin.assign_likelihood.far.marginalize_pdf_urls"
        ) as mock_marginalize:
            mock_marginalize.return_value = mock_stat

            test_args = [
                "sgnl-assign-likelihood",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_database_with_events),
                "-o",
                str(output_db),
                "-l",
                "fake_likelihood.xml.gz",
                "--add-zerolag-to-background",
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_likelihood.main()

        # Verify finish was called with add_zerolag=True
        mock_stat.finish.assert_called_once_with(add_zerolag=True)

    def test_main_vetoes_name_raises_error(self, input_database_with_events, tmp_path):
        """Test that vetoes-name option raises NotImplementedError."""
        output_db = tmp_path / "output.db"

        mock_stat = self._create_mock_rankingstat()

        with mock.patch(
            "sgnl.bin.assign_likelihood.far.marginalize_pdf_urls"
        ) as mock_marginalize:
            mock_marginalize.return_value = mock_stat

            test_args = [
                "sgnl-assign-likelihood",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_database_with_events),
                "-o",
                str(output_db),
                "-l",
                "fake_likelihood.xml.gz",
                "--vetoes-name",
                "vetoes",
            ]

            with mock.patch.object(sys, "argv", test_args):
                with pytest.raises(ValueError, match="vetoing feature has not"):
                    assign_likelihood.main()

    def test_main_failed_database_load(self, tmp_path, capsys):
        """Test handling of failed database loads."""
        # Create a corrupt/invalid database file
        bad_db = tmp_path / "bad.db"
        bad_db.write_text("not a valid database")
        output_db = tmp_path / "output.db"

        mock_stat = self._create_mock_rankingstat()

        with mock.patch(
            "sgnl.bin.assign_likelihood.far.marginalize_pdf_urls"
        ) as mock_marginalize:
            mock_marginalize.return_value = mock_stat

            test_args = [
                "sgnl-assign-likelihood",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(bad_db),
                "-o",
                str(output_db),
                "-l",
                "fake_likelihood.xml.gz",
                "-v",
            ]

            with mock.patch.object(sys, "argv", test_args):
                with pytest.raises(ValueError, match="could not be processed"):
                    assign_likelihood.main()

        captured = capsys.readouterr()
        assert "failed to load" in captured.err

    def test_main_updates_process_params(self, input_database_with_events, tmp_path):
        """Test that main records process parameters."""
        output_db = tmp_path / "output.db"

        mock_stat = self._create_mock_rankingstat()

        with mock.patch(
            "sgnl.bin.assign_likelihood.far.marginalize_pdf_urls"
        ) as mock_marginalize:
            mock_marginalize.return_value = mock_stat

            test_args = [
                "sgnl-assign-likelihood",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_database_with_events),
                "-o",
                str(output_db),
                "-l",
                "fake_likelihood.xml.gz",
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_likelihood.main()

        # Check process params
        outdb = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=str(output_db))
        params = {
            dict(row)["param"]: dict(row)["value"]
            for row in outdb.default_cursor.execute("SELECT * FROM process_params;")
        }

        assert "input_likelihood_file" in params
        assert "input_database_file" in params

    def test_main_with_vetoed_triggers(self, tmp_path):
        """Test that vetoed triggers are skipped."""
        # Create a database with events
        db = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=":memory:")
        db.default_cursor.execute(
            """
            INSERT INTO process (program, start_time, end_time, ifos,
                                 is_online, node, username, unix_procid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("initial_program", 0, 0, "H1,L1", 0, "test_node", "test_user", 12345),
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
                    "likelihood": 0.0,
                    "false_alarm_probability": None,
                    "combined_far": None,
                },
            },
            ignore_sim=True,
        )
        db.flush()
        input_path = tmp_path / "input.db"
        db.to_file(str(input_path))

        output_db = tmp_path / "output.db"

        mock_stat = self._create_mock_rankingstat()

        # Mock trigger_veto_func to always return True (veto all triggers)
        with (
            mock.patch(
                "sgnl.bin.assign_likelihood.far.marginalize_pdf_urls"
            ) as mock_marginalize,
            mock.patch(
                "sgnl.bin.assign_likelihood.trigger_veto_func", return_value=True
            ),
        ):
            mock_marginalize.return_value = mock_stat

            test_args = [
                "sgnl-assign-likelihood",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input_path),
                "-o",
                str(output_db),
                "-l",
                "fake_likelihood.xml.gz",
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_likelihood.main()

        # Verify output exists but likelihood was not updated (event was vetoed)
        assert output_db.exists()
        outdb = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=str(output_db))
        events = list(outdb.default_cursor.execute("SELECT * FROM event;"))
        event = dict(events[0])
        # Likelihood should still be 0.0 (not updated because trigger was vetoed)
        assert event["likelihood"] == pytest.approx(0.0)
        # ln_lr_from_triggers should not have been called
        mock_stat.ln_lr_from_triggers.assert_not_called()

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
                    "likelihood": 0.0,
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
                    "likelihood": 0.0,
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

        mock_stat = self._create_mock_rankingstat()

        with mock.patch(
            "sgnl.bin.assign_likelihood.far.marginalize_pdf_urls"
        ) as mock_marginalize:
            mock_marginalize.return_value = mock_stat

            test_args = [
                "sgnl-assign-likelihood",
                "-s",
                str(PATH_SCHEMA),
                "-i",
                str(input1),
                str(input2),
                "-o",
                str(output1),
                str(output2),
                "-l",
                "fake_likelihood.xml.gz",
                "-v",
            ]

            with mock.patch.object(sys, "argv", test_args):
                assign_likelihood.main()

        # Both outputs should exist and have likelihoods assigned
        assert output1.exists()
        assert output2.exists()

        outdb1 = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=str(output1))
        events1 = list(outdb1.default_cursor.execute("SELECT * FROM event;"))
        assert dict(events1[0])["likelihood"] == pytest.approx(5.0)

        outdb2 = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=str(output2))
        events2 = list(outdb2.default_cursor.execute("SELECT * FROM event;"))
        assert dict(events2[0])["likelihood"] == pytest.approx(5.0)

"""Unit tests for sgnl.bin.add_segments module."""

import pathlib
import sys
from unittest import mock

import pytest
from igwn_ligolw import ligolw, lsctables
from igwn_ligolw import utils as ligolw_utils
from igwn_ligolw.utils import segments as ligolw_segments
from igwn_segments import segment, segmentlist, segmentlistdict

from sgnl import sgnlio
from sgnl.bin import add_segments

PATH_SCHEMA = pathlib.Path(__file__).parent.parent / "config" / "cbc_db.yaml"


@pytest.fixture
def segments_file(tmp_path):
    """Create a temporary segments XML file for testing."""
    # Create document
    doc = ligolw.Document()
    doc.appendChild(ligolw.LIGO_LW())

    # Create process table and a process row
    proc_table = lsctables.ProcessTable.new()
    doc.childNodes[0].appendChild(proc_table)
    proc = proc_table.RowType()
    proc.process_id = proc_table.get_next_id()
    proc.program = "test"
    proc.version = "1.0"
    proc.cvs_repository = ""
    proc.cvs_entry_time = 0
    proc.comment = ""
    proc.is_online = 0
    proc.node = "test"
    proc.username = "test"
    proc.unix_procid = 0
    proc.start_time = 0
    proc.end_time = 0
    proc.jobid = 0
    proc.domain = "test"
    proc.ifos = "H1,L1"
    proc_table.append(proc)

    # Create segment tables using LigolwSegments
    segs = ligolw_segments.LigolwSegments(doc, proc)

    # Create segments
    # H1: 1000000000 to 1000001000
    # L1: 1000000500 to 1000001500
    segdict = segmentlistdict()
    segdict["H1"] = segmentlist([segment(1000000000, 1000001000)])
    segdict["L1"] = segmentlist([segment(1000000500, 1000001500)])

    # Insert into document
    segs.insert_from_segmentlistdict(segdict, "test_segments")

    # Finalize
    segs.finalize()

    # Write the document to a file
    segments_path = tmp_path / "test_segments.xml"
    ligolw_utils.write_filename(doc, str(segments_path))

    return segments_path


@pytest.fixture
def input_database(tmp_path):
    """Create a temporary input database for testing."""
    # Create an in-memory database and save it to file
    db = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=":memory:")

    # Insert initial process row (required by the schema)
    db.default_cursor.execute(
        """
        INSERT INTO process (program, start_time, end_time, ifos,
                             is_online, node, username, unix_procid)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        ("initial_program", 0, 0, "H1,L1", 0, "test_node", "test_user", 12345),
    )
    db.flush()

    input_path = tmp_path / "input.db"
    db.to_file(str(input_path))

    return input_path


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_parse_command_line_required_args(self, tmp_path):
        """Test parsing with all required arguments."""
        input_db = tmp_path / "input.db"
        output_db = tmp_path / "output.db"
        segments = tmp_path / "segments.xml"

        test_args = [
            "sgnl-add-segments",
            "-i",
            str(input_db),
            "-o",
            str(output_db),
            "--segments-file",
            str(segments),
            "--segments-name",
            "test_segments",
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, process_params = add_segments.parse_command_line()

        assert options.input_database_file == str(input_db)
        assert options.output_database_file == str(output_db)
        assert options.segments_file == str(segments)
        assert options.segments_name == "test_segments"
        assert options.verbose is False
        assert options.config_schema is None

        # process_params should be a dict copy of options
        assert process_params["input_database_file"] == str(input_db)
        assert process_params["segments_name"] == "test_segments"

    def test_parse_command_line_with_verbose(self, tmp_path):
        """Test parsing with verbose flag."""
        test_args = [
            "sgnl-add-segments",
            "-i",
            "input.db",
            "-o",
            "output.db",
            "--segments-file",
            "segments.xml",
            "--segments-name",
            "test_segments",
            "-v",
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = add_segments.parse_command_line()

        assert options.verbose is True

    def test_parse_command_line_with_config_schema(self, tmp_path):
        """Test parsing with config schema option."""
        test_args = [
            "sgnl-add-segments",
            "-i",
            "input.db",
            "-o",
            "output.db",
            "--segments-file",
            "segments.xml",
            "--segments-name",
            "test_segments",
            "-s",
            "schema.yaml",
        ]

        with mock.patch.object(sys, "argv", test_args):
            options, _ = add_segments.parse_command_line()

        assert options.config_schema == "schema.yaml"

    def test_parse_command_line_missing_required(self):
        """Test that missing required arguments raises SystemExit."""
        test_args = ["sgnl-add-segments", "-i", "input.db"]

        with mock.patch.object(sys, "argv", test_args):
            with pytest.raises(SystemExit):
                add_segments.parse_command_line()


class TestInitConfigRow:
    """Tests for init_config_row function."""

    def test_init_config_row_basic(self):
        """Test creating a config row from table schema."""
        table = {
            "columns": [
                {"name": "col1", "type": "INTEGER"},
                {"name": "col2", "type": "TEXT"},
                {"name": "col3", "type": "REAL"},
            ]
        }

        result = add_segments.init_config_row(None, table)

        assert result == {"col1": None, "col2": None, "col3": None}

    def test_init_config_row_excludes_dunder_columns(self):
        """Test that columns starting with __ are excluded."""
        table = {
            "columns": [
                {"name": "__primary_id", "type": "INTEGER"},
                {"name": "col1", "type": "INTEGER"},
                {"name": "__internal", "type": "TEXT"},
                {"name": "col2", "type": "TEXT"},
            ]
        }

        result = add_segments.init_config_row(None, table)

        assert "__primary_id" not in result
        assert "__internal" not in result
        assert result == {"col1": None, "col2": None}

    def test_init_config_row_with_extra(self):
        """Test creating a config row with extra values."""
        table = {
            "columns": [
                {"name": "col1", "type": "INTEGER"},
                {"name": "col2", "type": "TEXT"},
            ]
        }
        extra = {"col1": 42, "extra_col": "extra_value"}

        result = add_segments.init_config_row(None, table, extra=extra)

        assert result["col1"] == 42
        assert result["col2"] is None
        assert result["extra_col"] == "extra_value"

    def test_init_config_row_empty_table(self):
        """Test with an empty columns list."""
        table = {"columns": []}

        result = add_segments.init_config_row(None, table)

        assert result == {}


class TestMain:
    """Tests for main function."""

    def test_main_adds_segments(self, input_database, segments_file, tmp_path):
        """Test that main correctly adds segments to database."""
        output_db = tmp_path / "output.db"

        test_args = [
            "sgnl-add-segments",
            "-s",
            str(PATH_SCHEMA),
            "-i",
            str(input_database),
            "-o",
            str(output_db),
            "--segments-file",
            str(segments_file),
            "--segments-name",
            "test_segments",
        ]

        with mock.patch.object(sys, "argv", test_args):
            add_segments.main()

        # Verify the output database exists
        assert output_db.exists()

        # Load the output database and verify segments were added
        outdb = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=str(output_db))

        # Check that segments were inserted
        segments = list(
            outdb.default_cursor.execute("SELECT * FROM segment ORDER BY ifo;")
        )
        assert len(segments) == 2

        # Check H1 segment
        h1_seg = dict(segments[0])
        assert h1_seg["ifo"] == "H1"
        assert h1_seg["name"] == "test_segments"
        # Times should be in nanoseconds
        assert h1_seg["start_time"] == 1000000000 * 10**9
        assert h1_seg["end_time"] == 1000001000 * 10**9

        # Check L1 segment
        l1_seg = dict(segments[1])
        assert l1_seg["ifo"] == "L1"
        assert l1_seg["name"] == "test_segments"
        assert l1_seg["start_time"] == 1000000500 * 10**9
        assert l1_seg["end_time"] == 1000001500 * 10**9

    def test_main_updates_process_table(self, input_database, segments_file, tmp_path):
        """Test that main updates the process table correctly."""
        output_db = tmp_path / "output.db"

        test_args = [
            "sgnl-add-segments",
            "-s",
            str(PATH_SCHEMA),
            "-i",
            str(input_database),
            "-o",
            str(output_db),
            "--segments-file",
            str(segments_file),
            "--segments-name",
            "test_segments",
        ]

        with mock.patch.object(sys, "argv", test_args):
            add_segments.main()

        # Load the output database and verify process was updated
        outdb = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=str(output_db))

        # Check that process table was updated with new program name
        process_rows = list(outdb.default_cursor.execute("SELECT * FROM process;"))
        assert len(process_rows) == 1
        process_row = dict(process_rows[0])
        assert process_row["program"] == "sgnl-add-segments"

    def test_main_inserts_process_params(self, input_database, segments_file, tmp_path):
        """Test that main inserts process parameters correctly."""
        output_db = tmp_path / "output.db"

        test_args = [
            "sgnl-add-segments",
            "-s",
            str(PATH_SCHEMA),
            "-i",
            str(input_database),
            "-o",
            str(output_db),
            "--segments-file",
            str(segments_file),
            "--segments-name",
            "test_segments",
            "-v",
        ]

        with mock.patch.object(sys, "argv", test_args):
            add_segments.main()

        # Load the output database and verify process_params
        outdb = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=str(output_db))

        params = {
            dict(row)["param"]: dict(row)["value"]
            for row in outdb.default_cursor.execute("SELECT * FROM process_params;")
        }

        # Should have all command line options as params
        assert "input_database_file" in params
        assert params["input_database_file"] == str(input_database)
        assert "output_database_file" in params
        assert params["output_database_file"] == str(output_db)
        assert "segments_file" in params
        assert params["segments_name"] == "test_segments"
        assert params["verbose"] == "True"

    def test_main_without_config_schema(self, segments_file, tmp_path):
        """Test main when database already has schema embedded."""
        # Create input database and save it
        input_db = sgnlio.SgnlDB(config=str(PATH_SCHEMA), dbname=":memory:")
        input_db.default_cursor.execute(
            """
            INSERT INTO process (program, start_time, end_time, ifos,
                                 is_online, node, username, unix_procid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("initial_program", 0, 0, "H1,L1", 0, "test_node", "test_user", 12345),
        )
        input_db.flush()

        input_path = tmp_path / "input_with_schema.db"
        input_db.to_file(str(input_path))

        output_db = tmp_path / "output.db"

        # Run without -s option - should use embedded schema
        test_args = [
            "sgnl-add-segments",
            "-i",
            str(input_path),
            "-o",
            str(output_db),
            "--segments-file",
            str(segments_file),
            "--segments-name",
            "test_segments",
        ]

        with mock.patch.object(sys, "argv", test_args):
            add_segments.main()

        # Verify segments were added
        assert output_db.exists()
        outdb = sgnlio.SgnlDB(dbname=str(output_db))
        segments = list(outdb.default_cursor.execute("SELECT * FROM segment;"))
        assert len(segments) == 2

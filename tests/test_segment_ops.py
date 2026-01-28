"""Tests for sgnl.bin.segment_ops CLI"""

import sys
from unittest import mock

import igwn_segments as segments
import pytest

from sgnl.bin import segment_ops


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_single_input_file(self):
        """Test parsing with single input file."""
        with mock.patch.object(sys, "argv", ["segment_ops", "input.xml"]):
            args = segment_ops.parse_command_line()

        assert args.inputs == ["input.xml"]
        assert args.output == "segments_out.xml.gz"
        assert args.segment_name == "datasegments"

    def test_multiple_input_files(self):
        """Test parsing with multiple input files."""
        with mock.patch.object(
            sys, "argv", ["segment_ops", "input1.xml", "input2.xml", "input3.xml"]
        ):
            args = segment_ops.parse_command_line()

        assert args.inputs == ["input1.xml", "input2.xml", "input3.xml"]

    def test_custom_output_file(self):
        """Test parsing with custom output file."""
        with mock.patch.object(
            sys, "argv", ["segment_ops", "input.xml", "-o", "custom_output.xml.gz"]
        ):
            args = segment_ops.parse_command_line()

        assert args.output == "custom_output.xml.gz"

    def test_custom_segment_name(self):
        """Test parsing with custom segment name."""
        with mock.patch.object(
            sys, "argv", ["segment_ops", "input.xml", "-n", "custom_segments"]
        ):
            args = segment_ops.parse_command_line()

        assert args.segment_name == "custom_segments"

    def test_union_operation(self):
        """Test parsing with union operation."""
        with mock.patch.object(
            sys,
            "argv",
            ["segment_ops", "input1.xml", "input2.xml", "--operation", "union"],
        ):
            args = segment_ops.parse_command_line()

        assert args.operation == "union"

    def test_intersection_operation(self):
        """Test parsing with intersection operation."""
        with mock.patch.object(
            sys,
            "argv",
            ["segment_ops", "input1.xml", "input2.xml", "--operation", "intersection"],
        ):
            args = segment_ops.parse_command_line()

        assert args.operation == "intersection"

    def test_diff_operation(self):
        """Test parsing with diff operation."""
        with mock.patch.object(
            sys,
            "argv",
            ["segment_ops", "input1.xml", "input2.xml", "--operation", "diff"],
        ):
            args = segment_ops.parse_command_line()

        assert args.operation == "diff"

    def test_gps_bounds(self):
        """Test parsing with GPS bounds."""
        with mock.patch.object(
            sys,
            "argv",
            [
                "segment_ops",
                "input.xml",
                "--gps-start",
                "1000000000",
                "--gps-end",
                "1000010000",
            ],
        ):
            args = segment_ops.parse_command_line()

        assert args.gps_start == 1000000000.0
        assert args.gps_end == 1000010000.0

    def test_min_length(self):
        """Test parsing with min-length."""
        with mock.patch.object(
            sys, "argv", ["segment_ops", "input.xml", "--min-length", "512"]
        ):
            args = segment_ops.parse_command_line()

        assert args.min_length == 512.0

    def test_trim(self):
        """Test parsing with trim."""
        with mock.patch.object(
            sys, "argv", ["segment_ops", "input.xml", "--trim", "100"]
        ):
            args = segment_ops.parse_command_line()

        assert args.trim == 100.0

    def test_verbose_flag(self):
        """Test parsing with verbose flag."""
        with mock.patch.object(sys, "argv", ["segment_ops", "input.xml", "-v"]):
            args = segment_ops.parse_command_line()

        assert args.verbose is True

    def test_diff_requires_two_files(self):
        """Test that diff operation requires exactly two files."""
        with mock.patch.object(
            sys, "argv", ["segment_ops", "input.xml", "--operation", "diff"]
        ):
            with pytest.raises(SystemExit):
                segment_ops.parse_command_line()

    def test_diff_with_three_files_fails(self):
        """Test that diff operation with three files fails."""
        with mock.patch.object(
            sys,
            "argv",
            [
                "segment_ops",
                "input1.xml",
                "input2.xml",
                "input3.xml",
                "--operation",
                "diff",
            ],
        ):
            with pytest.raises(SystemExit):
                segment_ops.parse_command_line()

    def test_union_requires_two_files(self):
        """Test that union operation requires at least two files."""
        with mock.patch.object(
            sys, "argv", ["segment_ops", "input.xml", "--operation", "union"]
        ):
            with pytest.raises(SystemExit):
                segment_ops.parse_command_line()

    def test_intersection_requires_two_files(self):
        """Test that intersection operation requires at least two files."""
        with mock.patch.object(
            sys, "argv", ["segment_ops", "input.xml", "--operation", "intersection"]
        ):
            with pytest.raises(SystemExit):
                segment_ops.parse_command_line()


class TestMain:
    """Tests for main function."""

    def test_main_single_file(self, tmp_path):
        """Test main with single input file."""
        # Create input segment file
        input_segs = segments.segmentlistdict()
        input_segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        input_file = tmp_path / "input.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        from sgnl.dags.segments import write_segments

        write_segments(input_segs, output=str(input_file))

        with mock.patch.object(
            sys, "argv", ["segment_ops", str(input_file), "-o", str(output_file)]
        ):
            segment_ops.main()

        assert output_file.exists()

    def test_main_with_gps_bounds(self, tmp_path):
        """Test main with GPS bounding."""
        input_segs = segments.segmentlistdict()
        input_segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        input_file = tmp_path / "input.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        from sgnl.dags.segments import load_segment_file, write_segments

        write_segments(input_segs, output=str(input_file))

        with mock.patch.object(
            sys,
            "argv",
            [
                "segment_ops",
                str(input_file),
                "-o",
                str(output_file),
                "--gps-start",
                "100",
                "--gps-end",
                "500",
            ],
        ):
            segment_ops.main()

        result = load_segment_file(str(output_file))
        assert result["H1"][0] == segments.segment(100, 500)

    def test_main_with_min_length(self, tmp_path):
        """Test main with minimum length filtering."""
        input_segs = segments.segmentlistdict()
        input_segs["H1"] = segments.segmentlist(
            [
                segments.segment(0, 100),  # too short
                segments.segment(500, 1500),  # long enough
            ]
        )

        input_file = tmp_path / "input.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        from sgnl.dags.segments import load_segment_file, write_segments

        write_segments(input_segs, output=str(input_file))

        with mock.patch.object(
            sys,
            "argv",
            [
                "segment_ops",
                str(input_file),
                "-o",
                str(output_file),
                "--min-length",
                "512",
            ],
        ):
            segment_ops.main()

        result = load_segment_file(str(output_file))
        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(500, 1500)

    def test_main_with_trim(self, tmp_path):
        """Test main with segment contraction."""
        input_segs = segments.segmentlistdict()
        input_segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        input_file = tmp_path / "input.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        from sgnl.dags.segments import load_segment_file, write_segments

        write_segments(input_segs, output=str(input_file))

        with mock.patch.object(
            sys,
            "argv",
            [
                "segment_ops",
                str(input_file),
                "-o",
                str(output_file),
                "--trim",
                "50",
            ],
        ):
            segment_ops.main()

        result = load_segment_file(str(output_file))
        assert result["H1"][0] == segments.segment(50, 950)

    def test_main_union_operation(self, tmp_path):
        """Test main with union operation."""
        segs1 = segments.segmentlistdict()
        segs1["H1"] = segments.segmentlist([segments.segment(0, 500)])

        segs2 = segments.segmentlistdict()
        segs2["H1"] = segments.segmentlist([segments.segment(400, 1000)])

        input_file1 = tmp_path / "input1.xml.gz"
        input_file2 = tmp_path / "input2.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        from sgnl.dags.segments import load_segment_file, write_segments

        write_segments(segs1, output=str(input_file1))
        write_segments(segs2, output=str(input_file2))

        with mock.patch.object(
            sys,
            "argv",
            [
                "segment_ops",
                str(input_file1),
                str(input_file2),
                "-o",
                str(output_file),
                "--operation",
                "union",
            ],
        ):
            segment_ops.main()

        result = load_segment_file(str(output_file))
        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(0, 1000)

    def test_main_intersection_operation(self, tmp_path):
        """Test main with intersection operation."""
        segs1 = segments.segmentlistdict()
        segs1["H1"] = segments.segmentlist([segments.segment(0, 700)])

        segs2 = segments.segmentlistdict()
        segs2["H1"] = segments.segmentlist([segments.segment(300, 1000)])

        input_file1 = tmp_path / "input1.xml.gz"
        input_file2 = tmp_path / "input2.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        from sgnl.dags.segments import load_segment_file, write_segments

        write_segments(segs1, output=str(input_file1))
        write_segments(segs2, output=str(input_file2))

        with mock.patch.object(
            sys,
            "argv",
            [
                "segment_ops",
                str(input_file1),
                str(input_file2),
                "-o",
                str(output_file),
                "--operation",
                "intersection",
            ],
        ):
            segment_ops.main()

        result = load_segment_file(str(output_file))
        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(300, 700)

    def test_main_diff_operation(self, tmp_path):
        """Test main with diff operation."""
        segs1 = segments.segmentlistdict()
        segs1["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        segs2 = segments.segmentlistdict()
        segs2["H1"] = segments.segmentlist([segments.segment(300, 700)])

        input_file1 = tmp_path / "input1.xml.gz"
        input_file2 = tmp_path / "input2.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        from sgnl.dags.segments import load_segment_file, write_segments

        write_segments(segs1, output=str(input_file1))
        write_segments(segs2, output=str(input_file2))

        with mock.patch.object(
            sys,
            "argv",
            [
                "segment_ops",
                str(input_file1),
                str(input_file2),
                "-o",
                str(output_file),
                "--operation",
                "diff",
            ],
        ):
            segment_ops.main()

        result = load_segment_file(str(output_file))
        assert len(result["H1"]) == 2
        assert result["H1"][0] == segments.segment(0, 300)
        assert result["H1"][1] == segments.segment(700, 1000)

    def test_main_verbose(self, tmp_path, capsys):
        """Test main with verbose output."""
        input_segs = segments.segmentlistdict()
        input_segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        input_file = tmp_path / "input.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        from sgnl.dags.segments import write_segments

        write_segments(input_segs, output=str(input_file))

        with mock.patch.object(
            sys,
            "argv",
            ["segment_ops", str(input_file), "-o", str(output_file), "-v"],
        ):
            segment_ops.main()

        captured = capsys.readouterr()
        assert "Loading segments" in captured.out
        assert "Saving output" in captured.out

    def test_main_combined_operations(self, tmp_path):
        """Test main with multiple operations combined."""
        input_segs = segments.segmentlistdict()
        input_segs["H1"] = segments.segmentlist(
            [
                segments.segment(0, 100),  # will be filtered
                segments.segment(1000, 3000),  # will be bounded and trimmed
            ]
        )

        input_file = tmp_path / "input.xml.gz"
        output_file = tmp_path / "output.xml.gz"

        from sgnl.dags.segments import load_segment_file, write_segments

        write_segments(input_segs, output=str(input_file))

        with mock.patch.object(
            sys,
            "argv",
            [
                "segment_ops",
                str(input_file),
                "-o",
                str(output_file),
                "--gps-start",
                "500",
                "--gps-end",
                "2500",
                "--min-length",
                "512",
                "--trim",
                "100",
            ],
        ):
            segment_ops.main()

        result = load_segment_file(str(output_file))
        # After bounding: [1000, 2500]
        # After filtering (>= 512): [1000, 2500]
        # After trimming by 100: [1100, 2400]
        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(1100, 2400)

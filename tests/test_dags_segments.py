"""Tests for sgnl.dags.segments"""

from unittest import mock

import igwn_segments as segments
import pytest
from lal import LIGOTimeGPS

from sgnl.dags import segments as seg_module


class TestQueryDqsegdbSegments:
    """Tests for query_dqsegdb_segments function."""

    def test_single_instrument_str_flag(self):
        """Test with single instrument and string flag."""
        mock_result = {"active": segments.segmentlist([segments.segment(100, 200)])}

        with mock.patch("dqsegdb2.query.query_segments", return_value=mock_result):
            result = seg_module.query_dqsegdb_segments(
                instruments="H1",
                start=100,
                end=200,
                flags="H1:DMT-SCIENCE:1",
            )

        assert "H1" in result
        assert len(result["H1"]) == 1

    def test_multiple_instruments_with_dict_flags(self):
        """Test with multiple instruments and dict flags."""
        mock_result = {"active": segments.segmentlist([segments.segment(100, 200)])}

        with mock.patch("dqsegdb2.query.query_segments", return_value=mock_result):
            result = seg_module.query_dqsegdb_segments(
                instruments=["H1", "L1"],
                start=100,
                end=200,
                flags={"H1": "H1:DMT-SCIENCE:1", "L1": "L1:DMT-SCIENCE:1"},
            )

        assert "H1" in result
        assert "L1" in result

    def test_str_flag_with_list_instruments_raises(self):
        """Test that string flag with list instruments raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            seg_module.query_dqsegdb_segments(
                instruments=["H1", "L1"],
                start=100,
                end=200,
                flags="H1:DMT-SCIENCE:1",
            )

        assert "instruments must also be type str" in str(exc_info.value)

    def test_with_ligo_time_gps(self):
        """Test with LIGOTimeGPS arguments."""
        mock_result = {"active": segments.segmentlist([segments.segment(100, 200)])}

        with mock.patch("dqsegdb2.query.query_segments", return_value=mock_result):
            result = seg_module.query_dqsegdb_segments(
                instruments="H1",
                start=LIGOTimeGPS(100),
                end=LIGOTimeGPS(200),
                flags="H1:DMT-SCIENCE:1",
            )

        assert "H1" in result

    def test_custom_server(self):
        """Test with custom server URL."""
        mock_result = {"active": segments.segmentlist([segments.segment(100, 200)])}

        with mock.patch(
            "dqsegdb2.query.query_segments", return_value=mock_result
        ) as mock_query:
            seg_module.query_dqsegdb_segments(
                instruments="H1",
                start=100,
                end=200,
                flags="H1:DMT-SCIENCE:1",
                server="https://custom.server.org",
            )

        mock_query.assert_called_once_with(
            "H1:DMT-SCIENCE:1",
            100,
            200,
            host="https://custom.server.org",
            coalesce=True,
        )


class TestQueryDqsegdbVetoSegments:
    """Tests for query_dqsegdb_veto_segments function."""

    @pytest.fixture
    def mock_veto_table(self):
        """Create a mock veto table."""
        mock_veto1 = mock.MagicMock()
        mock_veto1.ifo = "H1"
        mock_veto1.name = "TEST_VETO"
        mock_veto1.version = 1
        mock_veto1.category = 1

        mock_veto2 = mock.MagicMock()
        mock_veto2.ifo = "H1"
        mock_veto2.name = "TEST_VETO2"
        mock_veto2.version = 1
        mock_veto2.category = 2

        mock_veto3 = mock.MagicMock()
        mock_veto3.ifo = "L1"
        mock_veto3.name = "TEST_VETO"
        mock_veto3.version = 1
        mock_veto3.category = 1

        return [mock_veto1, mock_veto2, mock_veto3]

    def test_cat1_cumulative(self, mock_veto_table):
        """Test CAT1 with cumulative vetoes."""
        mock_xmldoc = mock.MagicMock()
        mock_result = {"active": segments.segmentlist([segments.segment(100, 150)])}

        with (
            mock.patch(
                "sgnl.dags.segments.ligolw_utils.load_filename",
                return_value=mock_xmldoc,
            ),
            mock.patch(
                "sgnl.dags.segments.lsctables.VetoDefTable.get_table",
                return_value=mock_veto_table.copy(),
            ),
            mock.patch("dqsegdb2.query.query_segments", return_value=mock_result),
        ):
            result = seg_module.query_dqsegdb_veto_segments(
                instruments=["H1", "L1"],
                start=100,
                end=200,
                veto_definer_file="veto_definer.xml",
                category="CAT1",
                cumulative=True,
            )

        assert "H1" in result
        assert "L1" in result

    def test_cat2_cumulative(self, mock_veto_table):
        """Test CAT2 with cumulative vetoes."""
        mock_xmldoc = mock.MagicMock()
        mock_result = {"active": segments.segmentlist([segments.segment(100, 150)])}

        with (
            mock.patch(
                "sgnl.dags.segments.ligolw_utils.load_filename",
                return_value=mock_xmldoc,
            ),
            mock.patch(
                "sgnl.dags.segments.lsctables.VetoDefTable.get_table",
                return_value=mock_veto_table.copy(),
            ),
            mock.patch("dqsegdb2.query.query_segments", return_value=mock_result),
        ):
            result = seg_module.query_dqsegdb_veto_segments(
                instruments="H1",
                start=100,
                end=200,
                veto_definer_file="veto_definer.xml",
                category="CAT2",
                cumulative=True,
            )

        assert "H1" in result

    def test_cat3_non_cumulative(self, mock_veto_table):
        """Test CAT3 with non-cumulative vetoes."""
        mock_veto_cat3 = mock.MagicMock()
        mock_veto_cat3.ifo = "H1"
        mock_veto_cat3.name = "TEST_VETO_CAT3"
        mock_veto_cat3.version = 1
        mock_veto_cat3.category = 3

        mock_xmldoc = mock.MagicMock()
        mock_result = {"active": segments.segmentlist([segments.segment(100, 150)])}

        veto_table = mock_veto_table.copy()
        veto_table.append(mock_veto_cat3)

        with (
            mock.patch(
                "sgnl.dags.segments.ligolw_utils.load_filename",
                return_value=mock_xmldoc,
            ),
            mock.patch(
                "sgnl.dags.segments.lsctables.VetoDefTable.get_table",
                return_value=veto_table,
            ),
            mock.patch("dqsegdb2.query.query_segments", return_value=mock_result),
        ):
            result = seg_module.query_dqsegdb_veto_segments(
                instruments="H1",
                start=100,
                end=200,
                veto_definer_file="veto_definer.xml",
                category="CAT3",
                cumulative=False,
            )

        assert "H1" in result

    def test_invalid_category_raises(self):
        """Test that invalid category raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            seg_module.query_dqsegdb_veto_segments(
                instruments="H1",
                start=100,
                end=200,
                veto_definer_file="veto_definer.xml",
                category="CAT4",
            )

        assert "not valid category" in str(exc_info.value)


class TestQueryGwoscSegments:
    """Tests for query_gwosc_segments function."""

    def test_single_instrument(self):
        """Test with single instrument."""
        mock_response = mock.MagicMock()
        mock_response.read.return_value = b"0 100\n100 200\n"

        with mock.patch("urllib.request.urlopen", return_value=mock_response):
            result = seg_module.query_gwosc_segments(
                instruments="H1",
                start=1126051217,  # O1 start time
                end=1126051317,
            )

        assert "H1" in result

    def test_multiple_instruments_string(self):
        """Test with multiple instruments as string."""
        mock_response = mock.MagicMock()
        mock_response.read.return_value = b"0 100\n"

        with mock.patch("urllib.request.urlopen", return_value=mock_response):
            result = seg_module.query_gwosc_segments(
                instruments="H1L1",
                start=1126051217,
                end=1126051317,
            )

        assert "H1" in result
        assert "L1" in result

    def test_no_verify_certs(self):
        """Test with verify_certs=False."""
        mock_response = mock.MagicMock()
        mock_response.read.return_value = b"0 100\n"

        with mock.patch("urllib.request.urlopen", return_value=mock_response):
            result = seg_module.query_gwosc_segments(
                instruments="H1",
                start=1126051217,
                end=1126051317,
                verify_certs=False,
            )

        assert "H1" in result


class TestQueryGwoscVetoSegments:
    """Tests for query_gwosc_veto_segments function."""

    def test_cat1_cumulative(self):
        """Test CAT1 with cumulative vetoes."""
        mock_response = mock.MagicMock()
        # Return full span as "science" segments
        mock_response.read.return_value = b"1126051217 1126051317\n"

        with mock.patch("urllib.request.urlopen", return_value=mock_response):
            result = seg_module.query_gwosc_veto_segments(
                instruments="H1",
                start=1126051217,
                end=1126051317,
                category="CAT1",
                cumulative=True,
            )

        assert "H1" in result

    def test_cat2_non_cumulative(self):
        """Test CAT2 with non-cumulative vetoes."""
        mock_response = mock.MagicMock()
        mock_response.read.return_value = b"1126051217 1126051317\n"

        with mock.patch("urllib.request.urlopen", return_value=mock_response):
            result = seg_module.query_gwosc_veto_segments(
                instruments=["H1", "L1"],
                start=1126051217,
                end=1126051317,
                category="CAT2",
                cumulative=False,
            )

        assert "H1" in result
        assert "L1" in result

    def test_cat3_cumulative(self):
        """Test CAT3 with cumulative vetoes."""
        mock_response = mock.MagicMock()
        mock_response.read.return_value = b"1126051217 1126051317\n"

        with mock.patch("urllib.request.urlopen", return_value=mock_response):
            result = seg_module.query_gwosc_veto_segments(
                instruments="H1",
                start=1126051217,
                end=1126051317,
                category="CAT3",
                cumulative=True,
            )

        assert "H1" in result

    def test_invalid_category_raises(self):
        """Test that invalid category raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            seg_module.query_gwosc_veto_segments(
                instruments="H1",
                start=1126051217,
                end=1126051317,
                category="CAT4",
            )

        assert "not valid category" in str(exc_info.value)

    def test_no_verify_certs(self):
        """Test with verify_certs=False."""
        mock_response = mock.MagicMock()
        mock_response.read.return_value = b"1126051217 1126051317\n"

        with mock.patch("urllib.request.urlopen", return_value=mock_response):
            result = seg_module.query_gwosc_veto_segments(
                instruments="H1",
                start=1126051217,
                end=1126051317,
                category="CAT1",
                verify_certs=False,
            )

        assert "H1" in result


class TestWriteSegments:
    """Tests for write_segments function."""

    def test_write_segments(self, tmp_path):
        """Test writing segments to file."""
        seglistdict = segments.segmentlistdict()
        seglistdict["H1"] = segments.segmentlist([segments.segment(100, 200)])
        seglistdict["L1"] = segments.segmentlist([segments.segment(100, 200)])

        output_file = str(tmp_path / "segments.xml.gz")

        seg_module.write_segments(
            seglistdict,
            output=output_file,
            segment_name="test_segments",
            process_name="test-process",
            verbose=True,
        )

        # File should exist
        assert (tmp_path / "segments.xml.gz").exists()

    def test_write_segments_empty_raises(self):
        """Test that empty segments raises ValueError."""
        seglistdict = segments.segmentlistdict()
        seglistdict["H1"] = segments.segmentlist()

        with pytest.raises(ValueError) as exc_info:
            seg_module.write_segments(seglistdict)

        assert "No segments found" in str(exc_info.value)


class TestAnalysisSegments:
    """Tests for analysis_segments function."""

    def test_basic_analysis_segments(self):
        """Test basic analysis segment generation."""
        allsegs = segments.segmentlistdict()
        allsegs["H1"] = segments.segmentlist([segments.segment(0, 100000)])
        allsegs["L1"] = segments.segmentlist([segments.segment(0, 100000)])

        boundary_seg = segments.segment(0, 100000)

        result = seg_module.analysis_segments(
            ifos=["H1", "L1"],
            allsegs=allsegs,
            boundary_seg=boundary_seg,
            start_pad=0,
            overlap=0,
            min_instruments=1,
        )

        # Should have segments for different ifo combinations
        assert len(result) > 0

    def test_with_overlap(self):
        """Test analysis segments with overlap."""
        allsegs = segments.segmentlistdict()
        allsegs["H1"] = segments.segmentlist([segments.segment(0, 100000)])

        boundary_seg = segments.segment(0, 100000)

        result = seg_module.analysis_segments(
            ifos=["H1"],
            allsegs=allsegs,
            boundary_seg=boundary_seg,
            overlap=100,
            min_instruments=1,
        )

        assert len(result) > 0

    def test_min_instruments_filter(self):
        """Test that min_instruments filters correctly."""
        allsegs = segments.segmentlistdict()
        allsegs["H1"] = segments.segmentlist([segments.segment(0, 100000)])
        allsegs["L1"] = segments.segmentlist([segments.segment(50000, 100000)])

        boundary_seg = segments.segment(0, 100000)

        result = seg_module.analysis_segments(
            ifos=["H1", "L1"],
            allsegs=allsegs,
            boundary_seg=boundary_seg,
            min_instruments=2,
        )

        # Only double coincidence segments should be present
        for key in result.keys():
            assert len(key) >= 2

    def test_empty_result_segments_deleted(self):
        """Test that empty result segments are deleted from dict."""
        allsegs = segments.segmentlistdict()
        allsegs["H1"] = segments.segmentlist()
        allsegs["L1"] = segments.segmentlist()

        boundary_seg = segments.segment(0, 1000)

        result = seg_module.analysis_segments(
            ifos=["H1", "L1"],
            allsegs=allsegs,
            boundary_seg=boundary_seg,
            min_instruments=1,
        )

        assert len(result) == 0


class TestSplitSegmentsByLock:
    """Tests for split_segments_by_lock function."""

    def test_basic_split(self):
        """Test basic segment splitting by lock."""
        seglistdicts = segments.segmentlistdict()
        seglistdicts["H1"] = segments.segmentlist([segments.segment(0, 1000000)])
        seglistdicts["L1"] = segments.segmentlist([segments.segment(0, 1000000)])

        boundary_seg = segments.segment(0, 1000000)

        result = seg_module.split_segments_by_lock(
            ifos=["H1", "L1"],
            seglistdicts=seglistdicts,
            boundary_seg=boundary_seg,
            max_time=100000,
        )

        assert len(result) >= 1

    def test_three_ifos_updates_doublesegs(self):
        """Test with three IFOs to hit the doublesegs update branch."""
        seglistdicts = segments.segmentlistdict()
        seglistdicts["H1"] = segments.segmentlist([segments.segment(0, 1000000)])
        seglistdicts["L1"] = segments.segmentlist([segments.segment(0, 1000000)])
        seglistdicts["V1"] = segments.segmentlist([segments.segment(0, 1000000)])

        boundary_seg = segments.segment(0, 1000000)

        result = seg_module.split_segments_by_lock(
            ifos=["H1", "L1", "V1"],
            seglistdicts=seglistdicts,
            boundary_seg=boundary_seg,
            max_time=100000,
        )

        assert len(result) >= 1

    def test_merge_short_last_segment(self):
        """Test that short last segment is merged."""
        seglistdicts = segments.segmentlistdict()
        # Create segments that will result in short last segment
        seglistdicts["H1"] = segments.segmentlist(
            [
                segments.segment(0, 700000),
                segments.segment(700100, 750000),
            ]
        )
        seglistdicts["L1"] = segments.segmentlist(
            [
                segments.segment(0, 700000),
                segments.segment(700100, 750000),
            ]
        )

        boundary_seg = segments.segment(0, 750000)

        result = seg_module.split_segments_by_lock(
            ifos=["H1", "L1"],
            seglistdicts=seglistdicts,
            boundary_seg=boundary_seg,
            max_time=100000,
        )

        assert len(result) >= 1


class TestSplitSegments:
    """Tests for split_segments function."""

    def test_split_single_segment(self):
        """Test splitting a segmentlist with one segment."""
        seglist = segments.segmentlist([segments.segment(0, 10000)])

        result = seg_module.split_segments(seglist, maxextent=2000, overlap=100)

        assert len(result) > 1

    def test_split_multiple_segments(self):
        """Test splitting a segmentlist with multiple segments."""
        seglist = segments.segmentlist(
            [segments.segment(0, 5000), segments.segment(6000, 11000)]
        )

        result = seg_module.split_segments(seglist, maxextent=2000, overlap=0)

        assert len(result) > 2


class TestSplitSegment:
    """Tests for split_segment function."""

    def test_segment_smaller_than_maxextent(self):
        """Test segment smaller than maxextent is not split."""
        seg = segments.segment(0, 1000)

        result = seg_module.split_segment(seg, maxextent=2000, overlap=0)

        assert len(result) == 1
        assert result[0] == seg

    def test_segment_larger_than_maxextent(self):
        """Test segment larger than maxextent is split."""
        seg = segments.segment(0, 10000)

        result = seg_module.split_segment(seg, maxextent=2000, overlap=0)

        assert len(result) > 1

    def test_with_overlap(self):
        """Test splitting with overlap."""
        seg = segments.segment(0, 10000)

        result = seg_module.split_segment(seg, maxextent=2000, overlap=100)

        # Check that segments overlap
        for i in range(len(result) - 1):
            assert result[i][1] > result[i + 1][0]

    def test_negative_maxextent_raises(self):
        """Test that negative maxextent raises ValueError."""
        seg = segments.segment(0, 1000)

        with pytest.raises(ValueError) as exc_info:
            seg_module.split_segment(seg, maxextent=-100, overlap=0)

        assert "maxextent must be positive" in str(exc_info.value)

    def test_zero_maxextent_raises(self):
        """Test that zero maxextent raises ValueError."""
        seg = segments.segment(0, 1000)

        with pytest.raises(ValueError) as exc_info:
            seg_module.split_segment(seg, maxextent=0, overlap=0)

        assert "maxextent must be positive" in str(exc_info.value)


class TestGwoscSegmentUrl:
    """Tests for _gwosc_segment_url function."""

    def test_o1_url(self):
        """Test URL generation for O1."""
        url = seg_module._gwosc_segment_url(1126051217, 1126051317, "H1_DATA")

        assert "O1" in url
        assert "H1_DATA" in url

    def test_o2_url(self):
        """Test URL generation for O2."""
        url = seg_module._gwosc_segment_url(1164556817, 1164556917, "H1_DATA")

        assert "O2_16KHZ_R1" in url

    def test_o3a_url(self):
        """Test URL generation for O3a."""
        url = seg_module._gwosc_segment_url(1238166018, 1238166118, "H1_DATA")

        assert "O3a_16KHZ_R1" in url

    def test_o3b_url(self):
        """Test URL generation for O3b."""
        url = seg_module._gwosc_segment_url(1256655618, 1256655718, "H1_DATA")

        assert "O3b_16KHZ_R1" in url

    def test_o4a_url(self):
        """Test URL generation for O4a."""
        url = seg_module._gwosc_segment_url(1368975618, 1368975718, "H1_DATA")

        assert "O4a_16KHZ_R1" in url

    def test_invalid_gps_raises(self):
        """Test that invalid GPS time raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            seg_module._gwosc_segment_url(1000000000, 1000000100, "H1_DATA")

        assert "GPS times requested not in GWOSC" in str(exc_info.value)

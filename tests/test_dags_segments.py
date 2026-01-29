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
        mock_result = {"active": segments.segmentlist([segments.segment(100, 700)])}

        with mock.patch("dqsegdb2.query.query_segments", return_value=mock_result):
            result = seg_module.query_dqsegdb_segments(
                instruments="H1",
                start=100,
                end=700,
                flags="H1:DMT-SCIENCE:1",
            )

        assert "H1" in result
        assert len(result["H1"]) == 1

    def test_multiple_instruments_with_dict_flags(self):
        """Test with multiple instruments and dict flags."""
        mock_result = {"active": segments.segmentlist([segments.segment(100, 700)])}

        with mock.patch("dqsegdb2.query.query_segments", return_value=mock_result):
            result = seg_module.query_dqsegdb_segments(
                instruments=["H1", "L1"],
                start=100,
                end=700,
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
                end=700,
                flags="H1:DMT-SCIENCE:1",
            )

        assert "instruments must also be type str" in str(exc_info.value)

    def test_with_ligo_time_gps(self):
        """Test with LIGOTimeGPS arguments."""
        mock_result = {"active": segments.segmentlist([segments.segment(100, 700)])}

        with mock.patch("dqsegdb2.query.query_segments", return_value=mock_result):
            result = seg_module.query_dqsegdb_segments(
                instruments="H1",
                start=LIGOTimeGPS(100),
                end=LIGOTimeGPS(700),
                flags="H1:DMT-SCIENCE:1",
            )

        assert "H1" in result

    def test_custom_server(self):
        """Test with custom server URL."""
        mock_result = {"active": segments.segmentlist([segments.segment(100, 700)])}

        with mock.patch(
            "dqsegdb2.query.query_segments", return_value=mock_result
        ) as mock_query:
            seg_module.query_dqsegdb_segments(
                instruments="H1",
                start=100,
                end=700,
                flags="H1:DMT-SCIENCE:1",
                server="https://custom.server.org",
            )

        mock_query.assert_called_once_with(
            "H1:DMT-SCIENCE:1",
            100,
            700,
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


class TestFilterShortSegments:
    """Tests for filter_short_segments function."""

    def test_filters_short_segments(self):
        """Test that segments shorter than min_duration are removed."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist(
            [
                segments.segment(0, 100),  # too short
                segments.segment(200, 800),  # 600s, long enough
                segments.segment(1000, 1100),  # too short
            ]
        )

        result = seg_module.filter_short_segments(segs, min_duration=512)

        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(200, 800)

    def test_default_min_duration(self):
        """Test default min_duration of 512 seconds."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist(
            [
                segments.segment(0, 511),  # just under 512
                segments.segment(1000, 1512),  # exactly 512
            ]
        )

        result = seg_module.filter_short_segments(segs)

        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(1000, 1512)

    def test_multiple_ifos(self):
        """Test filtering with multiple IFOs."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])
        segs["L1"] = segments.segmentlist([segments.segment(0, 100)])

        result = seg_module.filter_short_segments(segs, min_duration=500)

        assert len(result["H1"]) == 1
        assert len(result["L1"]) == 0

    def test_empty_result(self):
        """Test that empty segmentlist is returned when all filtered."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist([segments.segment(0, 100)])

        result = seg_module.filter_short_segments(segs, min_duration=500)

        assert "H1" in result
        assert len(result["H1"]) == 0


class TestBoundSegments:
    """Tests for bound_segments function."""

    def test_bound_both_ends(self):
        """Test bounding segments at both start and end."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        result = seg_module.bound_segments(segs, start=100, end=900)

        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(100, 900)

    def test_bound_start_only(self):
        """Test bounding at start only."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        result = seg_module.bound_segments(segs, start=500, end=None)

        assert result["H1"][0] == segments.segment(500, 1000)

    def test_bound_end_only(self):
        """Test bounding at end only."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        result = seg_module.bound_segments(segs, start=None, end=500)

        assert result["H1"][0] == segments.segment(0, 500)

    def test_no_bounds_returns_original(self):
        """Test that no bounds returns original segments."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        result = seg_module.bound_segments(segs, start=None, end=None)

        assert result is segs

    def test_multiple_segments(self):
        """Test bounding multiple segments."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist(
            [
                segments.segment(0, 500),
                segments.segment(600, 1000),
            ]
        )

        result = seg_module.bound_segments(segs, start=100, end=800)

        assert len(result["H1"]) == 2
        assert result["H1"][0] == segments.segment(100, 500)
        assert result["H1"][1] == segments.segment(600, 800)

    def test_segment_outside_bounds_removed(self):
        """Test that segments outside bounds are removed."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist(
            [
                segments.segment(0, 100),  # before bounds
                segments.segment(500, 600),  # inside bounds
                segments.segment(900, 1000),  # after bounds
            ]
        )

        result = seg_module.bound_segments(segs, start=400, end=700)

        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(500, 600)

    def test_multiple_ifos(self):
        """Test bounding with multiple IFOs."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])
        segs["L1"] = segments.segmentlist([segments.segment(200, 800)])

        result = seg_module.bound_segments(segs, start=100, end=900)

        assert result["H1"][0] == segments.segment(100, 900)
        assert result["L1"][0] == segments.segment(200, 800)


class TestContractSegments:
    """Tests for contract_segments function."""

    def test_contract_segments(self):
        """Test basic segment contraction."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        result = seg_module.contract_segments(segs, trim=100)

        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(100, 900)

    def test_contract_removes_short_segments(self):
        """Test that segments too short for contraction are removed."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist(
            [
                segments.segment(0, 100),  # too short (100 < 2*100)
                segments.segment(500, 1500),  # long enough
            ]
        )

        result = seg_module.contract_segments(segs, trim=100)

        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(600, 1400)

    def test_zero_trim_returns_original(self):
        """Test that zero trim returns original segments."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        result = seg_module.contract_segments(segs, trim=0)

        assert result is segs

    def test_negative_trim_returns_original(self):
        """Test that negative trim returns original segments."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        result = seg_module.contract_segments(segs, trim=-50)

        assert result is segs

    def test_multiple_ifos(self):
        """Test contraction with multiple IFOs."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])
        segs["L1"] = segments.segmentlist([segments.segment(0, 500)])

        result = seg_module.contract_segments(segs, trim=50)

        assert result["H1"][0] == segments.segment(50, 950)
        assert result["L1"][0] == segments.segment(50, 450)


class TestUnionSegmentlistdicts:
    """Tests for union_segmentlistdicts function."""

    def test_union_basic(self):
        """Test basic union of two segmentlistdicts."""
        a = segments.segmentlistdict()
        a["H1"] = segments.segmentlist([segments.segment(0, 500)])

        b = segments.segmentlistdict()
        b["H1"] = segments.segmentlist([segments.segment(400, 1000)])

        result = seg_module.union_segmentlistdicts(a, b)

        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(0, 1000)

    def test_union_disjoint_segments(self):
        """Test union of disjoint segments."""
        a = segments.segmentlistdict()
        a["H1"] = segments.segmentlist([segments.segment(0, 100)])

        b = segments.segmentlistdict()
        b["H1"] = segments.segmentlist([segments.segment(200, 300)])

        result = seg_module.union_segmentlistdicts(a, b)

        assert len(result["H1"]) == 2

    def test_union_different_ifos(self):
        """Test union with different IFOs."""
        a = segments.segmentlistdict()
        a["H1"] = segments.segmentlist([segments.segment(0, 100)])

        b = segments.segmentlistdict()
        b["L1"] = segments.segmentlist([segments.segment(0, 100)])

        result = seg_module.union_segmentlistdicts(a, b)

        assert "H1" in result
        assert "L1" in result

    def test_union_missing_key_treated_as_empty(self):
        """Test that missing keys are treated as empty segmentlists."""
        a = segments.segmentlistdict()
        a["H1"] = segments.segmentlist([segments.segment(0, 100)])
        a["L1"] = segments.segmentlist([segments.segment(0, 100)])

        b = segments.segmentlistdict()
        b["H1"] = segments.segmentlist([segments.segment(50, 150)])

        result = seg_module.union_segmentlistdicts(a, b)

        assert result["L1"] == a["L1"]


class TestIntersectionSegmentlistdicts:
    """Tests for intersection_segmentlistdicts function."""

    def test_intersection_basic(self):
        """Test basic intersection of two segmentlistdicts."""
        a = segments.segmentlistdict()
        a["H1"] = segments.segmentlist([segments.segment(0, 500)])

        b = segments.segmentlistdict()
        b["H1"] = segments.segmentlist([segments.segment(300, 1000)])

        result = seg_module.intersection_segmentlistdicts(a, b)

        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(300, 500)

    def test_intersection_disjoint_segments(self):
        """Test intersection of disjoint segments."""
        a = segments.segmentlistdict()
        a["H1"] = segments.segmentlist([segments.segment(0, 100)])

        b = segments.segmentlistdict()
        b["H1"] = segments.segmentlist([segments.segment(200, 300)])

        result = seg_module.intersection_segmentlistdicts(a, b)

        assert len(result["H1"]) == 0

    def test_intersection_only_common_keys(self):
        """Test that intersection only includes common keys."""
        a = segments.segmentlistdict()
        a["H1"] = segments.segmentlist([segments.segment(0, 100)])
        a["L1"] = segments.segmentlist([segments.segment(0, 100)])

        b = segments.segmentlistdict()
        b["H1"] = segments.segmentlist([segments.segment(0, 100)])
        b["V1"] = segments.segmentlist([segments.segment(0, 100)])

        result = seg_module.intersection_segmentlistdicts(a, b)

        assert "H1" in result
        assert "L1" not in result
        assert "V1" not in result


class TestDiffSegmentlistdicts:
    """Tests for diff_segmentlistdicts function."""

    def test_diff_basic(self):
        """Test basic difference of two segmentlistdicts."""
        a = segments.segmentlistdict()
        a["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        b = segments.segmentlistdict()
        b["H1"] = segments.segmentlist([segments.segment(300, 700)])

        result = seg_module.diff_segmentlistdicts(a, b)

        assert len(result["H1"]) == 2
        assert result["H1"][0] == segments.segment(0, 300)
        assert result["H1"][1] == segments.segment(700, 1000)

    def test_diff_missing_key_treated_as_empty(self):
        """Test that missing keys in b are treated as empty."""
        a = segments.segmentlistdict()
        a["H1"] = segments.segmentlist([segments.segment(0, 1000)])
        a["L1"] = segments.segmentlist([segments.segment(0, 1000)])

        b = segments.segmentlistdict()
        b["H1"] = segments.segmentlist([segments.segment(300, 700)])

        result = seg_module.diff_segmentlistdicts(a, b)

        # L1 should be unchanged since it's not in b
        assert result["L1"] == a["L1"]

    def test_diff_complete_subtraction(self):
        """Test difference where b completely covers a."""
        a = segments.segmentlistdict()
        a["H1"] = segments.segmentlist([segments.segment(100, 500)])

        b = segments.segmentlistdict()
        b["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        result = seg_module.diff_segmentlistdicts(a, b)

        assert len(result["H1"]) == 0


class TestCombineSegmentlistdicts:
    """Tests for combine_segmentlistdicts function."""

    def test_combine_union(self):
        """Test combining with union operation."""
        segdicts = [
            segments.segmentlistdict(
                {"H1": segments.segmentlist([segments.segment(0, 100)])}
            ),
            segments.segmentlistdict(
                {"H1": segments.segmentlist([segments.segment(50, 150)])}
            ),
            segments.segmentlistdict(
                {"H1": segments.segmentlist([segments.segment(100, 200)])}
            ),
        ]

        result = seg_module.combine_segmentlistdicts(segdicts, "union")

        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(0, 200)

    def test_combine_intersection(self):
        """Test combining with intersection operation."""
        segdicts = [
            segments.segmentlistdict(
                {"H1": segments.segmentlist([segments.segment(0, 500)])}
            ),
            segments.segmentlistdict(
                {"H1": segments.segmentlist([segments.segment(100, 400)])}
            ),
            segments.segmentlistdict(
                {"H1": segments.segmentlist([segments.segment(200, 300)])}
            ),
        ]

        result = seg_module.combine_segmentlistdicts(segdicts, "intersection")

        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(200, 300)

    def test_combine_diff(self):
        """Test combining with diff operation."""
        segdicts = [
            segments.segmentlistdict(
                {"H1": segments.segmentlist([segments.segment(0, 1000)])}
            ),
            segments.segmentlistdict(
                {"H1": segments.segmentlist([segments.segment(200, 400)])}
            ),
        ]

        result = seg_module.combine_segmentlistdicts(segdicts, "diff")

        assert len(result["H1"]) == 2

    def test_combine_invalid_operation_raises(self):
        """Test that invalid operation raises ValueError."""
        segdicts = [
            segments.segmentlistdict(),
            segments.segmentlistdict(),
        ]

        with pytest.raises(ValueError) as exc_info:
            seg_module.combine_segmentlistdicts(segdicts, "invalid")

        assert "must be 'union', 'intersection', or 'diff'" in str(exc_info.value)

    def test_combine_less_than_two_dicts_raises(self):
        """Test that fewer than two dicts raises ValueError."""
        segdicts = [segments.segmentlistdict()]

        with pytest.raises(ValueError) as exc_info:
            seg_module.combine_segmentlistdicts(segdicts, "union")

        assert "at least two" in str(exc_info.value)


class TestProcessSegments:
    """Tests for process_segments function."""

    def test_process_all_operations(self):
        """Test processing with all operations."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist(
            [
                segments.segment(0, 100),  # will be filtered by min_length
                segments.segment(1000, 3000),  # will be bounded and contracted
            ]
        )

        result = seg_module.process_segments(
            segs,
            gps_start=500,
            gps_end=2500,
            min_length=512,
            trim=100,
        )

        # After bounding: [1000, 2500]
        # After filtering (>= 512): [1000, 2500]
        # After contracting by 100: [1100, 2400]
        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(1100, 2400)

    def test_process_no_operations(self):
        """Test processing with no operations."""
        segs = segments.segmentlistdict()
        segs["H1"] = segments.segmentlist([segments.segment(0, 1000)])

        result = seg_module.process_segments(segs)

        assert result["H1"] == segs["H1"]


class TestLoadSegmentFile:
    """Tests for load_segment_file function."""

    def test_load_segment_file(self, tmp_path):
        """Test loading segments from file."""
        # First write a segment file
        seglistdict = segments.segmentlistdict()
        seglistdict["H1"] = segments.segmentlist([segments.segment(100, 200)])
        seglistdict["L1"] = segments.segmentlist([segments.segment(100, 200)])

        output_file = str(tmp_path / "segments.xml.gz")
        seg_module.write_segments(seglistdict, output=output_file)

        # Now load it back
        result = seg_module.load_segment_file(output_file)

        assert "H1" in result
        assert "L1" in result
        assert len(result["H1"]) == 1
        assert result["H1"][0] == segments.segment(100, 200)


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

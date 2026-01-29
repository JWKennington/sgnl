"""Tests for sgnl.dags.util"""

import sys
from unittest import mock

import pytest

# Mock ezdag before importing util
mock_ezdag = mock.MagicMock()
sys.modules["ezdag"] = mock_ezdag

from igwn_segments import segment, segmentlist, segmentlistdict  # noqa: E402
from lal.utils import CacheEntry  # noqa: E402

from sgnl.dags import util  # noqa: E402


class TestDotDict:
    """Tests for DotDict class."""

    def test_dot_notation_get(self):
        """Test getting values with dot notation."""
        d = util.DotDict({"foo": 1, "bar": 2})
        assert d.foo == 1
        assert d.bar == 2

    def test_dot_notation_set(self):
        """Test setting values with dot notation."""
        d = util.DotDict()
        d.foo = 1
        assert d["foo"] == 1

    def test_dot_notation_del(self):
        """Test deleting values with dot notation."""
        d = util.DotDict({"foo": 1})
        del d.foo
        assert "foo" not in d

    def test_nested_dict_conversion(self):
        """Test that nested dicts are converted to DotDict."""
        d = util.DotDict({"outer": {"inner": 1}})
        assert isinstance(d["outer"], util.DotDict)
        assert d.outer.inner == 1


class TestReplaceHyphens:
    """Tests for replace_hyphens function."""

    def test_replace_hyphens(self):
        """Test replacing hyphens with underscores."""
        result = util.replace_hyphens({"foo-bar": 1, "baz-qux": 2})
        assert "foo_bar" in result
        assert "baz_qux" in result

    def test_replace_hyphens_nested(self):
        """Test replacing hyphens in nested dicts."""
        result = util.replace_hyphens({"outer-key": {"inner-key": 1}})
        assert "outer_key" in result
        assert "inner_key" in result["outer_key"]

    def test_replace_hyphens_reverse(self):
        """Test reverse replacement (underscores to hyphens)."""
        result = util.replace_hyphens({"foo_bar": 1}, reverse=True)
        assert "foo-bar" in result


class TestRecursiveUpdate:
    """Tests for recursive_update function."""

    def test_simple_update(self):
        """Test simple dictionary update."""
        d = {"a": 1, "b": 2}
        u = {"b": 3, "c": 4}
        result = util.recursive_update(d, u)
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_nested_update(self):
        """Test nested dictionary update."""
        d = {"a": {"x": 1, "y": 2}}
        u = {"a": {"y": 3, "z": 4}}
        result = util.recursive_update(d, u)
        assert result == {"a": {"x": 1, "y": 3, "z": 4}}


class TestToIfoList:
    """Tests for to_ifo_list function."""

    def test_single_ifo(self):
        """Test converting single IFO."""
        result = util.to_ifo_list("H1")
        assert result == ["H1"]

    def test_multiple_ifos(self):
        """Test converting multiple IFOs."""
        result = util.to_ifo_list("H1L1V1")
        assert result == ["H1", "L1", "V1"]


class TestToIfoCombos:
    """Tests for to_ifo_combos function."""

    def test_single_ifo(self):
        """Test combinations with single IFO."""
        result = util.to_ifo_combos(["H1"])
        assert frozenset(["H1"]) in result

    def test_two_ifos(self):
        """Test combinations with two IFOs."""
        result = util.to_ifo_combos(["H1", "L1"])
        assert frozenset(["H1"]) in result
        assert frozenset(["L1"]) in result
        assert frozenset(["H1", "L1"]) in result

    def test_min_ifos(self):
        """Test min_ifos parameter."""
        result = util.to_ifo_combos(["H1", "L1", "V1"], min_ifos=2)
        assert frozenset(["H1"]) not in result
        assert frozenset(["H1", "L1"]) in result


class TestFormatIfoArgs:
    """Tests for format_ifo_args function."""

    def test_single_ifo_string(self):
        """Test with single IFO as string."""
        result = util.format_ifo_args("H1", {"H1": "CHANNEL"})
        assert result == ["H1=CHANNEL"]

    def test_multiple_ifos(self):
        """Test with multiple IFOs."""
        result = util.format_ifo_args(
            ["H1", "L1"], {"H1": "CHANNEL_H1", "L1": "CHANNEL_L1"}
        )
        assert "H1=CHANNEL_H1" in result
        assert "L1=CHANNEL_L1" in result


class TestLoadSvdOptions:
    """Tests for load_svd_options function."""

    def test_load_svd_options_basic(self, tmp_path):
        """Test loading basic SVD options."""
        option_file = tmp_path / "options.json"
        option_file.write_text('{"bins": {"0000": {"foo": 1}}}')

        svd_config = util.DotDict({})
        svd_bins, svd_stats = util.load_svd_options(str(option_file), svd_config)

        assert "0000" in svd_bins
        assert svd_stats.bins["0000"]["foo"] == 1

    def test_load_svd_options_with_sub_banks(self, tmp_path):
        """Test loading SVD options with sub_banks."""
        option_file = tmp_path / "options.json"
        option_file.write_text('{"bins": {"0000": {"foo": 1}}}')

        svd_config = util.DotDict(
            {
                "param1": "value1",
                "sub_banks": {"bank1": {"param2": "value2"}},
            }
        )
        svd_bins, svd_stats = util.load_svd_options(str(option_file), svd_config)

        assert "bank1" in svd_config.sub_banks
        assert svd_config.sub_banks["bank1"]["param1"] == "value1"
        assert svd_config.sub_banks["bank1"]["param2"] == "value2"


class TestFlatten:
    """Tests for flatten function."""

    def test_flatten_nested(self):
        """Test flattening nested lists."""
        result = util.flatten([[1, 2], [3, 4]])
        assert result == [1, 2, 3, 4]

    def test_flatten_empty(self):
        """Test flattening empty list."""
        result = util.flatten([])
        assert result == []


class TestMchirpRangeToBins:
    """Tests for mchirp_range_to_bins function."""

    def test_overlapping_bins(self):
        """Test finding overlapping SVD bins."""
        svd_metadata = {
            "bins": {
                "0000": {"min_mchirp": 1.0, "max_mchirp": 5.0},
                "0001": {"min_mchirp": 5.0, "max_mchirp": 10.0},
                "0002": {"min_mchirp": 10.0, "max_mchirp": 20.0},
            }
        }
        result = util.mchirp_range_to_bins(3.0, 7.0, svd_metadata)
        assert "0000" in result
        assert "0001" in result
        assert "0002" not in result


class TestCondorScratchSpace:
    """Tests for condor_scratch_space function."""

    def test_returns_env_var(self):
        """Test that function returns expected env variable name."""
        result = util.condor_scratch_space()
        assert result == "_CONDOR_SCRATCH_DIR"


class TestDataCache:
    """Tests for DataCache class."""

    @pytest.fixture
    def sample_cache(self):
        """Create a sample DataCache."""
        entries = [
            CacheEntry.from_T050017("H1-SGNL_TRIGGERS-1000000000-1000.sqlite.gz"),
            CacheEntry.from_T050017("L1-SGNL_TRIGGERS-1000000000-1000.sqlite.gz"),
        ]
        return util.DataCache(util.DataType.TRIGGERS, entries)

    def test_files_property(self, sample_cache):
        """Test files property."""
        files = sample_cache.files
        assert len(files) == 2
        assert all("TRIGGERS" in f for f in files)

    def test_urls_property(self, sample_cache):
        """Test urls property."""
        urls = sample_cache.urls
        assert len(urls) == 2

    def test_urls_osdf_fix(self):
        """Test that OSDF URLs are fixed properly."""
        # Create entry with osdf URL
        entry = mock.MagicMock()
        entry.url = "osdf:/path/to/file"
        cache = util.DataCache(util.DataType.TRIGGERS, [entry])
        urls = cache.urls
        assert urls[0] == "osdf:///path/to/file"

    def test_urls_igwn_osdf_fix(self):
        """Test that igwn+osdf URLs are fixed properly."""
        entry = mock.MagicMock()
        entry.url = "igwn+osdf:/path/to/file"
        cache = util.DataCache(util.DataType.TRIGGERS, [entry])
        urls = cache.urls
        assert urls[0] == "igwn+osdf:///path/to/file"

    def test_len(self, sample_cache):
        """Test __len__ method."""
        assert len(sample_cache) == 2

    def test_add(self, sample_cache):
        """Test __add__ method."""
        combined = sample_cache + sample_cache
        assert len(combined) == 4

    def test_add_different_types_raises(self, sample_cache):
        """Test that adding different types raises assertion."""
        other = util.DataCache(util.DataType.REFERENCE_PSD, [])
        with pytest.raises(AssertionError):
            sample_cache + other

    def test_chunked(self, sample_cache):
        """Test chunked method."""
        chunks = list(sample_cache.chunked(1))
        assert len(chunks) == 2
        assert all(len(c) == 1 for c in chunks)

    def test_groupby_ifo(self, sample_cache):
        """Test groupby ifo."""
        groups = sample_cache.groupby("ifo")
        assert "H1" in groups
        assert "L1" in groups

    def test_groupby_instrument(self, sample_cache):
        """Test groupby instrument."""
        groups = sample_cache.groupby("instrument")
        assert "H1" in groups

    def test_groupby_observatory(self, sample_cache):
        """Test groupby observatory."""
        groups = sample_cache.groupby("observatory")
        assert "H1" in groups

    def test_groupby_segment(self, sample_cache):
        """Test groupby segment."""
        groups = sample_cache.groupby("segment")
        assert len(groups) >= 1

    def test_groupby_time(self, sample_cache):
        """Test groupby time."""
        groups = sample_cache.groupby("time")
        assert len(groups) >= 1

    def test_groupby_svd_bin(self):
        """Test groupby svd_bin."""
        entries = [
            CacheEntry.from_T050017("H1-0000_SGNL_TRIGGERS-1000000000-1000.sqlite.gz"),
            CacheEntry.from_T050017("H1-0001_SGNL_TRIGGERS-1000000000-1000.sqlite.gz"),
        ]
        cache = util.DataCache(util.DataType.TRIGGERS, entries)
        groups = cache.groupby("bin")
        assert "0000" in groups
        assert "0001" in groups

    def test_groupby_subtype(self):
        """Test groupby subtype."""
        entries = [
            CacheEntry.from_T050017("H1-SGNL_TRIGGERS_FOO-1000000000-1000.sqlite.gz"),
        ]
        cache = util.DataCache(util.DataType.TRIGGERS, entries)
        groups = cache.groupby("subtype")
        assert "FOO" in groups

    def test_groupby_directory(self, sample_cache):
        """Test groupby directory."""
        groups = sample_cache.groupby("directory")
        assert len(groups) >= 1

    def test_groupby_invalid_raises(self, sample_cache):
        """Test that invalid groupby raises ValueError."""
        with pytest.raises(ValueError):
            sample_cache.groupby("invalid_group")

    def test_groupby_keyfunc_with_string(self, sample_cache):
        """Test _groupby_keyfunc with string argument (line 214)."""
        # Directly call _groupby_keyfunc with a string instead of tuple
        keyfunc = sample_cache._groupby_keyfunc("ifo")
        # Should work without error
        entry = sample_cache.cache[0]
        result = keyfunc(entry)
        assert result == "H1"

    def test_groupby_multiple(self, sample_cache):
        """Test groupby with multiple groups."""
        groups = sample_cache.groupby("ifo", "segment")
        assert len(groups) >= 1
        # Keys should be tuples
        for key in groups.keys():
            assert isinstance(key, tuple)

    def test_groupby_bins(self):
        """Test groupby_bins method."""
        entries = [
            CacheEntry.from_T050017("H1-SGNL_TRIGGERS-1000000000-500.sqlite.gz"),
            CacheEntry.from_T050017("H1-SGNL_TRIGGERS-1000000500-500.sqlite.gz"),
        ]
        cache = util.DataCache(util.DataType.TRIGGERS, entries)
        bins = segmentlist([segment(1000000000, 1000000600)])
        groups = cache.groupby_bins("time", bins)
        assert len(groups) >= 1

    def test_groupby_bins_invalid_type_raises(self, sample_cache):
        """Test that invalid bin_type raises assertion."""
        with pytest.raises(AssertionError):
            sample_cache.groupby_bins("invalid", [])

    def test_copy(self, sample_cache):
        """Test copy method."""
        copied = sample_cache.copy()
        assert len(copied) == len(sample_cache)

    def test_generate(self, tmp_path):
        """Test generate method."""
        cache = util.DataCache.generate(
            util.DataType.TRIGGERS,
            ifos=["H1"],
            time_bins=segment(1000000000, 1000001000),
            root=str(tmp_path),
        )
        assert len(cache) == 1

    def test_generate_with_svd_bins(self, tmp_path):
        """Test generate with svd_bins list."""
        cache = util.DataCache.generate(
            util.DataType.TRIGGERS,
            ifos="H1",
            time_bins=segment(1000000000, 1000001000),
            svd_bins=["0000", "0001"],
            root=str(tmp_path),
        )
        assert len(cache) == 2

    def test_generate_with_svd_bins_string(self, tmp_path):
        """Test generate with svd_bins as string (line 267)."""
        cache = util.DataCache.generate(
            util.DataType.TRIGGERS,
            ifos="H1",
            time_bins=segment(1000000000, 1000001000),
            svd_bins="0000",  # String, not list
            root=str(tmp_path),
        )
        assert len(cache) == 1

    def test_generate_with_segmentlist(self, tmp_path):
        """Test generate with segmentlist time_bins."""
        cache = util.DataCache.generate(
            util.DataType.TRIGGERS,
            ifos=frozenset(["H1"]),
            time_bins=segmentlist([segment(1000000000, 1000001000)]),
            root=str(tmp_path),
        )
        assert len(cache) == 1

    def test_generate_with_segmentlistdict(self, tmp_path):
        """Test generate with segmentlistdict time_bins."""
        time_bins = segmentlistdict(
            {"H1": segmentlist([segment(1000000000, 1000001000)])}
        )
        cache = util.DataCache.generate(
            util.DataType.TRIGGERS,
            ifos=["H1"],
            time_bins=time_bins,
            root=str(tmp_path),
        )
        assert len(cache) == 1

    def test_generate_no_time_bins(self, tmp_path):
        """Test generate without time_bins."""
        cache = util.DataCache.generate(
            util.DataType.TRIGGERS,
            ifos=["H1"],
            root=str(tmp_path),
        )
        assert len(cache) == 1

    def test_generate_with_subtype(self, tmp_path):
        """Test generate with subtype."""
        cache = util.DataCache.generate(
            util.DataType.TRIGGERS,
            ifos=["H1"],
            time_bins=segment(1000000000, 1000001000),
            subtype="TEST",
            root=str(tmp_path),
        )
        assert len(cache) == 1
        assert "TEST" in cache.files[0]

    def test_find(self, tmp_path):
        """Test find method."""
        # Create a file
        triggers_dir = tmp_path / "triggers" / "10000"
        triggers_dir.mkdir(parents=True)
        (triggers_dir / "H1-SGNL_TRIGGERS-1000000000-1000.sqlite.gz").touch()

        cache = util.DataCache.find(
            util.DataType.TRIGGERS,
            root=str(tmp_path),
        )
        assert len(cache) == 1

    def test_find_with_svd_bins(self, tmp_path):
        """Test find with svd_bins."""
        triggers_dir = tmp_path / "triggers" / "10000"
        triggers_dir.mkdir(parents=True)
        (triggers_dir / "H1-0000_SGNL_TRIGGERS-1000000000-1000.sqlite.gz").touch()

        cache = util.DataCache.find(
            util.DataType.TRIGGERS,
            root=str(tmp_path),
            svd_bins="0000",
        )
        assert len(cache) == 1

    def test_find_with_segments(self, tmp_path):
        """Test find with segment filtering."""
        triggers_dir = tmp_path / "triggers" / "10000"
        triggers_dir.mkdir(parents=True)
        (triggers_dir / "H1-SGNL_TRIGGERS-1000000000-1000.sqlite.gz").touch()

        cache = util.DataCache.find(
            util.DataType.TRIGGERS,
            root=str(tmp_path),
            segments=segmentlist([segment(1000000000, 1000002000)]),
        )
        assert len(cache) == 1

    def test_find_no_gps_dir(self, tmp_path):
        """Test find without GPS directory."""
        triggers_dir = tmp_path / "triggers"
        triggers_dir.mkdir(parents=True)
        (triggers_dir / "H1-SGNL_TRIGGERS-1000000000-1000.sqlite.gz").touch()

        cache = util.DataCache.find(
            util.DataType.TRIGGERS,
            root=str(tmp_path),
        )
        assert len(cache) == 1

    def test_from_files(self):
        """Test from_files method."""
        cache = util.DataCache.from_files(
            util.DataType.TRIGGERS,
            "H1-SGNL_TRIGGERS-1000000000-1000.sqlite.gz",
        )
        assert len(cache) == 1

    def test_from_files_list(self):
        """Test from_files with list."""
        cache = util.DataCache.from_files(
            util.DataType.TRIGGERS,
            [
                "H1-SGNL_TRIGGERS-1000000000-1000.sqlite.gz",
                "L1-SGNL_TRIGGERS-1000000000-1000.sqlite.gz",
            ],
        )
        assert len(cache) == 2


class TestDataFileMixin:
    """Tests for DataFileMixin class."""

    def test_description_basic(self):
        """Test basic description generation."""
        desc = util.DataType.TRIGGERS.description()
        assert "TRIGGERS" in desc
        assert "SGNL" in desc

    def test_description_with_svd_bin(self):
        """Test description with svd_bin."""
        desc = util.DataType.TRIGGERS.description(svd_bin="0000")
        assert "0000" in desc

    def test_description_with_subtype(self):
        """Test description with subtype."""
        desc = util.DataType.TRIGGERS.description(subtype="test")
        assert "TEST" in desc

    def test_filename(self):
        """Test filename generation."""
        filename = util.DataType.TRIGGERS.filename(
            "H1", span=segment(1000000000, 1000001000)
        )
        assert "H1" in filename
        assert "TRIGGERS" in filename
        assert "1000000000" in filename

    def test_filename_no_span(self):
        """Test filename without span."""
        filename = util.DataType.TRIGGERS.filename("H1")
        assert "H1" in filename
        assert "0-0" in filename

    def test_filename_custom_extension(self):
        """Test filename with custom extension."""
        filename = util.DataType.TRIGGERS.filename(
            "H1", span=segment(0, 1000), extension=".h5"
        )
        assert filename.endswith(".h5")

    def test_file_pattern(self):
        """Test file_pattern generation."""
        pattern = util.DataType.TRIGGERS.file_pattern()
        assert "*" in pattern
        assert "TRIGGERS" in pattern

    def test_file_pattern_with_svd_bin(self):
        """Test file_pattern with svd_bin."""
        pattern = util.DataType.TRIGGERS.file_pattern(svd_bin="0000")
        assert "0000" in pattern

    def test_directory(self):
        """Test directory generation."""
        directory = util.DataType.TRIGGERS.directory()
        assert directory == "triggers"

    def test_directory_with_root(self):
        """Test directory with root."""
        directory = util.DataType.TRIGGERS.directory(root="/data")
        assert directory == "/data/triggers"

    def test_directory_with_start(self):
        """Test directory with start time."""
        directory = util.DataType.TRIGGERS.directory(start=1234567890)
        assert "12345" in directory


class TestDataType:
    """Tests for DataType enum."""

    def test_str(self):
        """Test __str__ method."""
        assert str(util.DataType.TRIGGERS) == "TRIGGERS"

    def test_extension(self):
        """Test extension attribute."""
        assert util.DataType.TRIGGERS.extension == "sqlite.gz"
        assert util.DataType.REFERENCE_PSD.extension == "xml.gz"


class TestGpsDirectory:
    """Tests for gps_directory function."""

    def test_gps_directory(self):
        """Test GPS directory generation."""
        result = util.gps_directory(1234567890)
        assert result == "12345"


class TestT050017Filename:
    """Tests for T050017_filename function."""

    def test_basic(self):
        """Test basic filename generation."""
        result = util.T050017_filename(
            "H1", "TEST", segment(1000000000, 1000001000), "xml.gz"
        )
        assert result == "H1-TEST-1000000000-1000.xml.gz"

    def test_with_frozenset_instruments(self):
        """Test with frozenset instruments."""
        result = util.T050017_filename(
            frozenset(["H1", "L1"]), "TEST", segment(1000000000, 1000001000), "xml.gz"
        )
        assert "H1L1" in result or "L1H1" in result

    def test_with_path(self):
        """Test with path."""
        result = util.T050017_filename(
            "H1", "TEST", segment(1000000000, 1000001000), "xml.gz", path="/data"
        )
        assert result.startswith("/data/")

    def test_strips_leading_dot(self):
        """Test that leading dot is stripped from extension."""
        result = util.T050017_filename("H1", "TEST", segment(0, 1000), ".xml.gz")
        assert not result.endswith("..xml.gz")

    def test_overflow_duration(self):
        """Test with overflow duration."""
        result = util.T050017_filename("H1", "TEST", segment(0, float("inf")), "xml.gz")
        assert "2000000000" in result


class TestEstimateOsdfFrameGB:
    """Tests for estimate_osdf_frame_GB function."""

    def test_hl_frames(self):
        """Test estimating H/L frame sizes."""
        uri_list = [
            "osdf:///igwn/ligo/frame1.gwf",
            "osdf:///igwn/ligo/frame2.gwf",
        ]
        result = util.estimate_osdf_frame_GB(uri_list)
        assert result == 3.2  # 2 * 1.6 GB

    def test_v_frames(self):
        """Test estimating Virgo frame sizes."""
        uri_list = [
            "osdf:///igwn/virgo/frame1.gwf",
        ]
        result = util.estimate_osdf_frame_GB(uri_list)
        assert result == 0.11

    def test_inj_frames(self):
        """Test estimating injection frame sizes."""
        uri_list = [
            "osdf:///igwn/shared/frame1.gwf",
        ]
        result = util.estimate_osdf_frame_GB(uri_list)
        assert result == 0.15

    def test_gwosc_frames(self):
        """Test estimating GWOSC frame sizes."""
        uri_list = [
            "osdf:///gwdata/O4a/frame1.gwf",
        ]
        result = util.estimate_osdf_frame_GB(uri_list)
        assert result == 0.5

    def test_mixed_frames(self):
        """Test with mixed frame types."""
        uri_list = [
            "osdf:///igwn/ligo/frame1.gwf",
            "osdf:///igwn/virgo/frame1.gwf",
            "osdf:///igwn/shared/inj.gwf",
        ]
        result = util.estimate_osdf_frame_GB(uri_list)
        assert result == pytest.approx(1.86)  # 1.6 + 0.11 + 0.15

    def test_unknown_raises(self):
        """Test that unknown URIs raise ValueError."""
        uri_list = ["osdf:///unknown/frame.gwf"]
        with pytest.raises(ValueError):
            util.estimate_osdf_frame_GB(uri_list)


class TestOsdfFlattenFrameCache:
    """Tests for osdf_flatten_frame_cache function."""

    def test_flatten_osdf_cache(self, tmp_path):
        """Test flattening OSDF cache."""
        cache_file = tmp_path / "cache.lcf"
        cache_file.write_text("H H1_HOFT 1000000000 1000 osdf:///igwn/ligo/frame.gwf\n")

        has_osdf, output_file = util.osdf_flatten_frame_cache(
            str(cache_file), new_frame_dir="/srv"
        )

        assert has_osdf is True
        assert "flat_" in output_file

    def test_no_osdf_frames(self, tmp_path):
        """Test with no OSDF frames."""
        cache_file = tmp_path / "cache.lcf"
        cache_file.write_text(
            "H H1_HOFT 1000000000 1000 file://localhost/data/frame.gwf\n"
        )

        has_osdf, output_file = util.osdf_flatten_frame_cache(str(cache_file))

        assert has_osdf is False
        assert output_file == str(cache_file)

    def test_custom_output_name(self, tmp_path):
        """Test with custom output name."""
        cache_file = tmp_path / "cache.lcf"
        cache_file.write_text("H H1_HOFT 1000000000 1000 osdf:///igwn/ligo/frame.gwf\n")

        has_osdf, output_file = util.osdf_flatten_frame_cache(
            str(cache_file), new_cache_name="custom.lcf"
        )

        assert output_file == "custom.lcf"

    def test_relative_path_raises(self, tmp_path):
        """Test that relative new_frame_dir raises ValueError."""
        cache_file = tmp_path / "cache.lcf"
        cache_file.write_text("H H1_HOFT 1000000000 1000 osdf:///igwn/ligo/frame.gwf\n")

        with pytest.raises(ValueError):
            util.osdf_flatten_frame_cache(str(cache_file), new_frame_dir="relative")


class TestLookupOsdfFramesForSegment:
    """Tests for lookup_osdf_frames_for_segment function."""

    def test_lookup_osdf_frames(self):
        """Test looking up OSDF frames for a segment."""
        # Create mock cache entries
        entry1 = mock.MagicMock()
        entry1.segment = segment(1000000000, 1000001000)
        entry1.scheme = "osdf"
        entry1.url = "osdf:///igwn/ligo/frame1.gwf"

        entry2 = mock.MagicMock()
        entry2.segment = segment(1000002000, 1000003000)
        entry2.scheme = "osdf"
        entry2.url = "osdf:///igwn/ligo/frame2.gwf"

        uris, size = util.lookup_osdf_frames_for_segment(
            [entry1, entry2], 1000000000, 1000001500
        )

        assert len(uris) == 1
        assert size > 0

    def test_no_matching_frames(self):
        """Test when no frames match the segment."""
        entry = mock.MagicMock()
        entry.segment = segment(2000000000, 2000001000)
        entry.scheme = "osdf"

        uris, size = util.lookup_osdf_frames_for_segment(
            [entry], 1000000000, 1000001000
        )

        assert uris == []
        assert size == 0.0

    def test_non_osdf_frames_excluded(self):
        """Test that non-OSDF frames are excluded."""
        entry = mock.MagicMock()
        entry.segment = segment(1000000000, 1000001000)
        entry.scheme = "file"

        uris, size = util.lookup_osdf_frames_for_segment(
            [entry], 1000000000, 1000001500
        )

        assert uris == []
        assert size == 0.0


class TestAddOsdfFramesToNode:
    """Tests for add_osdf_frames_to_node function."""

    def test_add_osdf_frames(self):
        """Test adding OSDF frames to a node."""
        mock_node = mock.MagicMock()
        mock_option = mock.MagicMock()
        mock_option.name = "frame-cache"
        # Use a real list so we can verify mutations
        mock_node.inputs = [mock_option]

        util.add_osdf_frames_to_node(
            mock_node,
            ["osdf:///igwn/ligo/frame.gwf"],
            "flat_cache.lcf",
            "frame-cache",
            "osdf-frame-files",
        )

        # After deleting the original and inserting 2 new options, should have 2 items
        assert len(mock_node.inputs) == 2

    def test_no_matching_option(self):
        """Test when cache option is not found."""
        mock_node = mock.MagicMock()
        mock_option = mock.MagicMock()
        mock_option.name = "other-option"
        mock_node.inputs = [mock_option]

        # Should not raise
        util.add_osdf_frames_to_node(
            mock_node,
            ["osdf:///igwn/ligo/frame.gwf"],
            "flat_cache.lcf",
            "frame-cache",
            "osdf-frame-files",
        )


class TestAdjustLayerDiskForOsdf:
    """Tests for adjust_layer_disk_for_osdf function."""

    def test_adjust_disk(self):
        """Test adjusting disk requirements."""
        mock_layer = mock.MagicMock()
        mock_layer.submit_description = {"request_disk": "10GB"}

        util.adjust_layer_disk_for_osdf(mock_layer, 5.0, 3.0)

        assert mock_layer.submit_description["request_disk"] == "18.0GB"

    def test_zero_osdf_size(self):
        """Test with zero OSDF size."""
        mock_layer = mock.MagicMock()
        mock_layer.submit_description = {"request_disk": "10GB"}

        util.adjust_layer_disk_for_osdf(mock_layer, 0, 0)

        # Should not modify
        assert mock_layer.submit_description["request_disk"] == "10GB"

    def test_invalid_disk_format_raises(self):
        """Test that invalid disk format raises ValueError."""
        mock_layer = mock.MagicMock()
        mock_layer.submit_description = {"request_disk": 10}

        with pytest.raises(ValueError):
            util.adjust_layer_disk_for_osdf(mock_layer, 5.0)


class TestAddOsdfSupportToLayer:
    """Tests for add_osdf_support_to_layer function."""

    def test_no_source_config(self):
        """Test with no source config."""
        mock_layer = mock.MagicMock()
        util.add_osdf_support_to_layer(mock_layer, None)
        # Should return early without error

    def test_no_frame_cache(self):
        """Test with no frame cache."""
        mock_layer = mock.MagicMock()
        mock_config = mock.MagicMock()
        mock_config.frame_cache = None

        util.add_osdf_support_to_layer(mock_layer, mock_config)
        # Should return early without error

    def test_with_frames_in_osdf(self, tmp_path):
        """Test with frames in OSDF."""
        # Create cache file
        cache_file = tmp_path / "cache.lcf"
        cache_file.write_text("H H1_HOFT 1000000000 1000 osdf:///igwn/ligo/frame.gwf\n")

        mock_layer = mock.MagicMock()
        mock_layer.submit_description = {"request_disk": "10GB"}

        # Create mock node with arguments
        mock_node = mock.MagicMock()
        mock_arg1 = mock.MagicMock()
        mock_arg1.name = "gps-start-time"
        mock_arg1.argument = [1000000000]
        mock_arg2 = mock.MagicMock()
        mock_arg2.name = "gps-end-time"
        mock_arg2.argument = [1000001000]
        mock_node.arguments = [mock_arg1, mock_arg2]

        mock_option = mock.MagicMock()
        mock_option.name = "frame-cache"
        mock_node.inputs = [mock_option]

        mock_layer.nodes = [mock_node]

        mock_config = mock.MagicMock()
        mock_config.frame_cache = str(cache_file)
        mock_config.frames_in_osdf = True
        mock_config.transfer_frame_cache = "flat_cache.lcf"
        mock_config.inj_frames_in_osdf = False

        util.add_osdf_support_to_layer(mock_layer, mock_config)

    def test_missing_gps_times_raises(self, tmp_path):
        """Test that missing GPS times raises ValueError."""
        cache_file = tmp_path / "cache.lcf"
        cache_file.write_text("H H1_HOFT 1000000000 1000 osdf:///igwn/ligo/frame.gwf\n")

        mock_layer = mock.MagicMock()
        mock_node = mock.MagicMock()
        mock_node.arguments = []
        mock_layer.nodes = [mock_node]

        mock_config = mock.MagicMock()
        mock_config.frame_cache = str(cache_file)
        mock_config.frames_in_osdf = True

        with pytest.raises(ValueError) as exc_info:
            util.add_osdf_support_to_layer(mock_layer, mock_config)

        assert "gps-start-time and gps-end-time" in str(exc_info.value)

    def test_with_injection_frames(self, tmp_path):
        """Test with injection frames in OSDF."""
        cache_file = tmp_path / "cache.lcf"
        cache_file.write_text(
            "H H1_HOFT 1000000000 1000 file://localhost/data/frame.gwf\n"
        )
        inj_cache_file = tmp_path / "inj_cache.lcf"
        inj_cache_file.write_text(
            "H H1_INJ 1000000000 1000 osdf:///igwn/shared/inj.gwf\n"
        )

        mock_layer = mock.MagicMock()
        mock_layer.submit_description = {"request_disk": "10GB"}

        mock_node = mock.MagicMock()
        mock_arg1 = mock.MagicMock()
        mock_arg1.name = "gps-start-time"
        mock_arg1.argument = [1000000000]
        mock_arg2 = mock.MagicMock()
        mock_arg2.name = "gps-end-time"
        mock_arg2.argument = [1000001000]
        mock_node.arguments = [mock_arg1, mock_arg2]

        mock_inj_option = mock.MagicMock()
        mock_inj_option.name = "noiseless-inj-frame-cache"
        mock_node.inputs = [mock_inj_option]

        mock_layer.nodes = [mock_node]

        mock_config = mock.MagicMock()
        mock_config.frame_cache = str(cache_file)
        mock_config.frames_in_osdf = False
        mock_config.inj_frame_cache = str(inj_cache_file)
        mock_config.inj_frames_in_osdf = True
        mock_config.transfer_inj_frame_cache = "flat_inj_cache.lcf"

        util.add_osdf_support_to_layer(
            mock_layer, mock_config, is_injection_workflow=True
        )


class TestGroups:
    """Tests for groups function."""

    def test_even_split(self):
        """Test splitting evenly."""
        result = list(util.groups([1, 2, 3, 4], 2))
        assert result == [[1, 2], [3, 4]]

    def test_uneven_split(self):
        """Test splitting unevenly."""
        result = list(util.groups([1, 2, 3, 4, 5], 2))
        assert result == [[1, 2], [3, 4], [5]]

    def test_larger_chunk(self):
        """Test with chunk larger than list."""
        result = list(util.groups([1, 2], 5))
        assert result == [[1, 2]]

    def test_empty_list(self):
        """Test with empty list."""
        result = list(util.groups([], 2))
        assert result == []

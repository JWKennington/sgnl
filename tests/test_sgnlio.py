"""Tests for sgnl.sgnlio module."""

from unittest import mock

import numpy
from igwn_segments import segment, segmentlist, segmentlistdict

from sgnl import sgnlio


class TestTypeFromSqlite:
    """Tests for type_from_sqlite function."""

    def test_integer_type(self):
        """Test INTEGER type conversion."""
        col = {"type": "INTEGER"}
        result = sgnlio.type_from_sqlite(col)
        assert result == "i8"

    def test_real_type(self):
        """Test REAL type conversion."""
        col = {"type": "REAL"}
        result = sgnlio.type_from_sqlite(col)
        assert result == "f8"

    def test_text_type(self):
        """Test TEXT type conversion."""
        col = {"type": "TEXT"}
        result = sgnlio.type_from_sqlite(col)
        assert result == "U64"


class TestContainer:
    """Tests for _Container class."""

    def test_container_creation(self):
        """Test that _Container can be instantiated."""
        container = sgnlio._Container()
        assert container is not None

    def test_container_setattr(self):
        """Test setting attributes on _Container."""
        container = sgnlio._Container()
        container.test_attr = "value"
        assert container.test_attr == "value"


class TestInjection:
    """Tests for _Injection class."""

    def test_injection_init(self):
        """Test _Injection initialization."""
        inj = sgnlio._Injection([1, 2, 3])
        assert len(inj) == 3
        assert list(inj) == [1, 2, 3]

    def test_injection_setup_table(self):
        """Test _Injection.setup_table method."""
        # Create mock data
        data = [
            {"simulation": {"mass1": 1.4, "mass2": 1.4, "name": "test1"}},
            {"simulation": {"mass1": 1.5, "mass2": 1.3, "name": "test2"}},
        ]
        inj = sgnlio._Injection(data)

        schema = {
            "simulation": {
                "columns": [
                    {"name": "mass1", "type": "REAL"},
                    {"name": "mass2", "type": "REAL"},
                    {"name": "name", "type": "TEXT"},
                ]
            }
        }

        inj.setup_table(schema, "simulation")

        assert hasattr(inj, "simulation")
        assert hasattr(inj.simulation, "mass1")
        assert hasattr(inj.simulation, "mass2")
        numpy.testing.assert_array_equal(inj.simulation.mass1, [1.4, 1.5])
        numpy.testing.assert_array_equal(inj.simulation.mass2, [1.4, 1.3])

    def test_injection_setup_single_ifo(self):
        """Test _Injection.setup with single IFO."""
        data = [
            {
                "simulation": {
                    "mass1": 1.4,
                    "mass2": 1.4,
                    "snr_H1": 10.0,
                    "geocent_end_time": 1000000000000000000,
                }
            },
            {
                "simulation": {
                    "mass1": 1.5,
                    "mass2": 1.3,
                    "snr_H1": 12.0,
                    "geocent_end_time": 1000000001000000000,
                }
            },
        ]
        inj = sgnlio._Injection(data)

        schema = {
            "simulation": {
                "columns": [
                    {"name": "mass1", "type": "REAL"},
                    {"name": "mass2", "type": "REAL"},
                    {"name": "snr_H1", "type": "REAL"},
                    {"name": "geocent_end_time", "type": "INTEGER"},
                ]
            }
        }

        combo = frozenset(["H1"])
        inj.setup(schema, combo)

        assert hasattr(inj, "color")
        assert hasattr(inj, "marker")
        assert inj.marker == "o"
        assert hasattr(inj.simulation, "decisive_snr")
        assert hasattr(inj.simulation, "network_snr")
        assert hasattr(inj.simulation, "time")

    def test_injection_setup_multiple_ifos(self):
        """Test _Injection.setup with multiple IFOs."""
        data = [
            {
                "simulation": {
                    "mass1": 1.4,
                    "mass2": 1.4,
                    "snr_H1": 10.0,
                    "snr_L1": 8.0,
                    "geocent_end_time": 1000000000000000000,
                }
            },
            {
                "simulation": {
                    "mass1": 1.5,
                    "mass2": 1.3,
                    "snr_H1": 12.0,
                    "snr_L1": 9.0,
                    "geocent_end_time": 1000000001000000000,
                }
            },
        ]
        inj = sgnlio._Injection(data)

        schema = {
            "simulation": {
                "columns": [
                    {"name": "mass1", "type": "REAL"},
                    {"name": "mass2", "type": "REAL"},
                    {"name": "snr_H1", "type": "REAL"},
                    {"name": "snr_L1", "type": "REAL"},
                    {"name": "geocent_end_time", "type": "INTEGER"},
                ]
            }
        }

        combo = frozenset(["H1", "L1"])
        inj.setup(schema, combo)

        # decisive_snr should be second largest for multiple IFOs
        assert hasattr(inj.simulation, "decisive_snr")
        # For H1=10, L1=8, decisive should be 8 (second largest)
        assert inj.simulation.decisive_snr[0] == 8.0


class TestMissed:
    """Tests for _Missed class."""

    def test_missed_setup(self):
        """Test _Missed.setup method."""
        data = [
            {
                "simulation": {
                    "mass1": 1.4,
                    "mass2": 1.4,
                    "snr_H1": 5.0,
                    "geocent_end_time": 1000000000000000000,
                }
            },
        ]
        missed = sgnlio._Missed(data)

        schema = {
            "simulation": {
                "columns": [
                    {"name": "mass1", "type": "REAL"},
                    {"name": "mass2", "type": "REAL"},
                    {"name": "snr_H1", "type": "REAL"},
                    {"name": "geocent_end_time", "type": "INTEGER"},
                ]
            }
        }

        combo = frozenset(["H1"])
        segments = segmentlist([segment(0, 100)])

        missed.setup(schema, combo, segments)

        assert missed.color == "#000000"
        assert missed.marker == "x"
        assert missed.segments == segments


class TestFound:
    """Tests for _Found class."""

    def test_found_setup(self):
        """Test _Found.setup method."""
        data = [
            {
                "simulation": {
                    "mass1": 1.4,
                    "mass2": 1.4,
                    "snr_H1": 15.0,
                    "geocent_end_time": 1000000000000000000,
                },
                "event": {
                    "combined_far": 1e-10,
                    "time": 1000000000000000000,
                },
            },
        ]
        found = sgnlio._Found(data)

        schema = {
            "simulation": {
                "columns": [
                    {"name": "mass1", "type": "REAL"},
                    {"name": "mass2", "type": "REAL"},
                    {"name": "snr_H1", "type": "REAL"},
                    {"name": "geocent_end_time", "type": "INTEGER"},
                ]
            },
            "event": {
                "columns": [
                    {"name": "combined_far", "type": "REAL"},
                    {"name": "time", "type": "INTEGER"},
                ]
            },
        }

        combo = frozenset(["H1"])
        segments = segmentlist([segment(0, 100)])

        found.setup(schema, combo, segments)

        assert found.marker == "o"
        assert found.segments == segments
        assert hasattr(found, "event")
        assert hasattr(found.event, "combined_far")


class TestInjByOnIFOs:
    """Tests for _InjByOnIFOs class."""

    def test_inj_by_on_ifos(self):
        """Test _InjByOnIFOs initialization."""
        # Create segments
        _segments = segmentlistdict(
            {
                frozenset(["H1"]): segmentlist([segment(0, 2)]),
            }
        )

        # Create injections - time in nanoseconds, segment check uses seconds
        _inj = [
            {
                "simulation": {
                    "mass1": 1.4,
                    "mass2": 1.4,
                    "snr_H1": 10.0,
                    "geocent_end_time": 1,  # 1 nanosecond = within segment [0,2]
                }
            },
        ]

        schema = {
            "simulation": {
                "columns": [
                    {"name": "mass1", "type": "REAL"},
                    {"name": "mass2", "type": "REAL"},
                    {"name": "snr_H1", "type": "REAL"},
                    {"name": "geocent_end_time", "type": "INTEGER"},
                ]
            }
        }

        # Use _Missed which has a simpler setup
        result = sgnlio._InjByOnIFOs(_segments, _inj, schema, sgnlio._Missed)

        assert frozenset(["H1"]) in result


class TestFoundByOnIFOs:
    """Tests for _FoundByOnIFOs class."""

    def test_found_by_on_ifos(self):
        """Test _FoundByOnIFOs initialization."""
        _segments = segmentlistdict(
            {
                frozenset(["H1"]): segmentlist([segment(0, 2)]),
            }
        )

        _found = [
            {
                "simulation": {
                    "mass1": 1.4,
                    "mass2": 1.4,
                    "snr_H1": 15.0,
                    "geocent_end_time": 1,
                },
                "event": {
                    "combined_far": 1e-10,
                    "time": 1,
                },
            },
        ]

        schema = {
            "simulation": {
                "columns": [
                    {"name": "mass1", "type": "REAL"},
                    {"name": "mass2", "type": "REAL"},
                    {"name": "snr_H1", "type": "REAL"},
                    {"name": "geocent_end_time", "type": "INTEGER"},
                ]
            },
            "event": {
                "columns": [
                    {"name": "combined_far", "type": "REAL"},
                    {"name": "time", "type": "INTEGER"},
                ]
            },
        }

        result = sgnlio._FoundByOnIFOs(_segments, _found, schema)

        assert frozenset(["H1"]) in result
        assert isinstance(result[frozenset(["H1"])], sgnlio._Found)


class TestMissedByOnIFOs:
    """Tests for _MissedByOnIFOs class."""

    def test_missed_by_on_ifos(self):
        """Test _MissedByOnIFOs initialization."""
        _segments = segmentlistdict(
            {
                frozenset(["H1"]): segmentlist([segment(0, 2)]),
            }
        )

        _missed = [
            {
                "simulation": {
                    "mass1": 1.4,
                    "mass2": 1.4,
                    "snr_H1": 5.0,
                    "geocent_end_time": 1,
                },
            },
        ]

        schema = {
            "simulation": {
                "columns": [
                    {"name": "mass1", "type": "REAL"},
                    {"name": "mass2", "type": "REAL"},
                    {"name": "snr_H1", "type": "REAL"},
                    {"name": "geocent_end_time", "type": "INTEGER"},
                ]
            },
        }

        result = sgnlio._MissedByOnIFOs(_segments, _missed, schema)

        assert frozenset(["H1"]) in result
        assert isinstance(result[frozenset(["H1"])], sgnlio._Missed)


class TestSgnlDB:
    """Tests for SgnlDB class."""

    def test_sgnldb_post_init(self):
        """Test SgnlDB.__post_init__ calls parent."""
        with mock.patch.object(
            sgnlio.stillsuit.StillSuit, "__post_init__", return_value=None
        ) as mock_parent_init:
            # Create instance - this should call __post_init__
            sgnl_db = object.__new__(sgnlio.SgnlDB)
            sgnl_db.__post_init__()

            mock_parent_init.assert_called_once()

    def test_sgnldb_segments(self):
        """Test SgnlDB.segments method."""
        # Create a mock database
        mock_db = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_db.cursor.return_value = mock_cursor

        # Mock segment rows as dict-like objects
        class MockRow(dict):
            pass

        mock_rows = [
            MockRow({"ifo": "H1", "start_time": 0, "end_time": 100}),
            MockRow({"ifo": "H1", "start_time": 200, "end_time": 300}),
            MockRow({"ifo": "L1", "start_time": 50, "end_time": 150}),
        ]

        mock_cursor.execute.return_value = mock_rows

        # Create SgnlDB with mocked initialization
        with mock.patch.object(sgnlio.SgnlDB, "__post_init__"):
            sgnl_db = object.__new__(sgnlio.SgnlDB)
            sgnl_db.db = mock_db

            result = sgnl_db.segments(name="afterhtgate")

            assert isinstance(result, segmentlistdict)
            # Should have combinations of H1 and L1
            assert len(result) > 0

    def test_sgnldb_missed_found_by_on_ifos(self):
        """Test SgnlDB.missed_found_by_on_ifos method."""
        with mock.patch.object(sgnlio.SgnlDB, "__post_init__"):
            sgnl_db = object.__new__(sgnlio.SgnlDB)

            # Mock get_missed_found
            mock_missed = []
            mock_found = []
            sgnl_db.get_missed_found = mock.MagicMock(
                return_value=(mock_missed, mock_found)
            )

            # Mock segments
            sgnl_db.segments = mock.MagicMock(
                return_value=segmentlistdict(
                    {frozenset(["H1"]): segmentlist([segment(0, 100)])}
                )
            )

            # Mock schema
            sgnl_db.schema = {
                "simulation": {
                    "columns": [
                        {"name": "mass1", "type": "REAL"},
                        {"name": "geocent_end_time", "type": "INTEGER"},
                        {"name": "snr_H1", "type": "REAL"},
                    ]
                },
                "event": {
                    "columns": [
                        {"name": "combined_far", "type": "REAL"},
                    ]
                },
            }

            missed, found = sgnl_db.missed_found_by_on_ifos()

            assert isinstance(missed, sgnlio._MissedByOnIFOs)
            assert isinstance(found, sgnlio._FoundByOnIFOs)

    def test_sgnldb_get_events_basic(self):
        """Test SgnlDB.get_events basic functionality."""
        with mock.patch.object(sgnlio.SgnlDB, "__post_init__"):
            sgnl_db = object.__new__(sgnlio.SgnlDB)

            # Create mock events
            mock_events = [
                {
                    "trigger": [{"time": 1000, "epoch_start": 900, "epoch_end": 1100}],
                    "event": {"time": 1000},
                },
            ]

            # Mock parent get_events using super()
            with mock.patch.object(
                sgnlio.stillsuit.StillSuit,
                "get_events",
                return_value=iter(mock_events),
            ):
                events = list(sgnl_db.get_events())

                assert len(events) == 1

    def test_sgnldb_get_events_nanosec_to_sec(self):
        """Test SgnlDB.get_events with nanosec_to_sec conversion."""
        with mock.patch.object(sgnlio.SgnlDB, "__post_init__"):
            sgnl_db = object.__new__(sgnlio.SgnlDB)

            # Create fresh mock events (not shared with other tests)
            def make_mock_events():
                return iter(
                    [
                        {
                            "trigger": [
                                {
                                    "time": 1000000000000000000,
                                    "epoch_start": 900000000000000000,
                                    "epoch_end": 1100000000000000000,
                                }
                            ],
                            "event": {"time": 1000000000000000000},
                        },
                    ]
                )

            with mock.patch.object(
                sgnlio.stillsuit.StillSuit,
                "get_events",
                side_effect=lambda **kwargs: make_mock_events(),
            ):
                events = list(sgnl_db.get_events(nanosec_to_sec=True))

                # Use approx due to floating point precision
                import pytest

                assert events[0]["event"]["time"] == pytest.approx(1e9)
                assert events[0]["trigger"][0]["time"] == pytest.approx(1e9)

    def test_sgnldb_get_events_template_duration(self):
        """Test SgnlDB.get_events with template_duration."""
        with mock.patch.object(sgnlio.SgnlDB, "__post_init__"):
            sgnl_db = object.__new__(sgnlio.SgnlDB)

            # Mock database
            mock_db = mock.MagicMock()
            mock_cursor = mock.MagicMock()
            mock_db.cursor.return_value = mock_cursor
            mock_cursor.execute.return_value.fetchone.return_value = {
                "template_duration": 64.0
            }
            sgnl_db.db = mock_db

            mock_events = [
                {
                    "trigger": [
                        {
                            "_filter_id": 1,
                            "time": 1000,
                            "epoch_start": 900,
                            "epoch_end": 1100,
                        }
                    ],
                    "event": {"time": 1000},
                },
            ]

            with mock.patch.object(
                sgnlio.stillsuit.StillSuit,
                "get_events",
                return_value=iter(mock_events),
            ):
                events = list(sgnl_db.get_events(template_duration=True))

                assert events[0]["trigger"][0]["template_duration"] == 64.0

    def test_sgnldb_get_events_with_template_duration_and_nanosec(self):
        """Test SgnlDB.get_events with both template_duration and nanosec_to_sec."""
        with mock.patch.object(sgnlio.SgnlDB, "__post_init__"):
            sgnl_db = object.__new__(sgnlio.SgnlDB)

            mock_db = mock.MagicMock()
            mock_cursor = mock.MagicMock()
            mock_db.cursor.return_value = mock_cursor
            mock_cursor.execute.return_value.fetchone.return_value = {
                "template_duration": 64000000000  # 64 seconds in nanoseconds
            }
            sgnl_db.db = mock_db

            mock_events = [
                {
                    "trigger": [
                        {
                            "_filter_id": 1,
                            "time": 1000000000000000000,
                            "epoch_start": 900000000000000000,
                            "epoch_end": 1100000000000000000,
                        }
                    ],
                    "event": {"time": 1000000000000000000},
                },
            ]

            with mock.patch.object(
                sgnlio.stillsuit.StillSuit,
                "get_events",
                return_value=iter(mock_events),
            ):
                events = list(
                    sgnl_db.get_events(template_duration=True, nanosec_to_sec=True)
                )

                # template_duration should also be converted
                assert events[0]["trigger"][0]["template_duration"] == 64.0

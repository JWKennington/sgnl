# Copyright (C) 2024 Chad Hanna
import itertools
from dataclasses import dataclass

import numpy
import stillsuit
from igwn_segments import segment, segmentlist, segmentlistdict

from sgnl.viz import IFO_COMBO_COLOR


@dataclass
class SgnlDB(stillsuit.StillSuit):
    def __post_init__(self):
        super().__post_init__()

    def segments(self, name="afterhtgate"):
        """
        Retrieve segments with the specified name from the database and organize them
        into a dictionary by interferometer (ifo) combination.
        """
        segments = {}
        for row in self.db.cursor().execute(
            "SELECT * FROM segment WHERE name = ?", (name,)
        ):
            row = dict(row)
            segments.setdefault(row["ifo"], []).append(
                (row["start_time"], row["end_time"])
            )

        # Convert rows to segment list dictionary
        segments = segmentlistdict(
            {
                ifo: segmentlist(segment(*interval) for interval in intervals)
                for ifo, intervals in segments.items()
            }
        )

        # Generate all possible ifo combinations
        ifos = frozenset(segments.keys())
        combos = [
            frozenset(combo)
            for level in range(len(ifos), 0, -1)
            for combo in itertools.combinations(ifos, level)
        ]

        # Create the output dictionary
        out = {
            combo: segments.intersection(combo) - segments.union(ifos - combo)
            for combo in combos
        }

        return segmentlistdict(out)

    def missed_found_by_on_ifos(
        self, far_threshold=1 / 86400 / 365.25, segments_name="afterhtgate"
    ):
        """
        get missed and found instruments by on ifos
        FIXME I am sure this is stupidly slow
        """

        _missed, _found = self.get_missed_found(
            selection_func=lambda r: r["event"]["combined_far"] <= far_threshold
        )
        _segments = self.segments(name=segments_name)

        missed = _MissedByOnIFOs(_segments, _missed, self.schema)
        found = _FoundByOnIFOs(_segments, _found, self.schema)

        return missed, found

    def get_events(self, nanosec_to_sec=False, template_duration=False, **kwargs):
        """
        A wrapper function of StillSuit.get_events() with additional
        functionalities
        """

        for event in super(SgnlDB, self).get_events(**kwargs):
            if template_duration:
                # Assume _filter_id is the same for all triggers in an event
                template_duration = dict(
                    self.db.cursor()
                    .execute(
                        "SELECT template_duration FROM filter WHERE _filter_id = ?",
                        (event["trigger"][0]["_filter_id"],),
                    )
                    .fetchone()
                )["template_duration"]
                for trigger in event["trigger"]:
                    trigger["template_duration"] = template_duration
            if nanosec_to_sec:
                for trigger in event["trigger"]:
                    trigger["time"] *= 1e-9
                    trigger["epoch_start"] *= 1e-9
                    trigger["epoch_end"] *= 1e-9
                    if "template_duration" in trigger:
                        trigger["template_duration"] *= 1e-9
                event["event"]["time"] *= 1e-9
            yield event


#
# Private ypes to help with missed / found
#


def type_from_sqlite(col):

    if col["type"] == "INTEGER":
        return "i8"
    if col["type"] == "REAL":
        return "f8"
    # FIXME don't hardcode 64 byte strings
    if col["type"] == "TEXT":
        # return "U%d" % col["size"]
        return "U64"


class _Container:
    pass


class _Injection(list):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setup_table(self, schema, table):
        setattr(self, table, _Container())
        for col in schema[table]["columns"]:
            name = col["name"]
            arr = numpy.zeros(len(self), dtype=type_from_sqlite(col))
            for i, row in enumerate(self):
                arr[i] = row[table][name]
            setattr(getattr(self, table), name, arr)

    def setup(self, schema, combo):
        self.color = IFO_COMBO_COLOR[",".join(sorted(combo))]
        self.marker = "o"
        self.setup_table(schema, "simulation")

        # Add decisive SNR
        if len(combo) > 1:
            self.simulation.decisive_snr = numpy.array(
                [
                    sorted([m["simulation"]["snr_%s" % c] for c in combo])[-2]
                    for m in self
                ]
            )
        else:
            self.simulation.decisive_snr = numpy.array(
                [m["simulation"]["snr_%s" % list(combo)[0]] for m in self]
            )

        # Add network SNR for this ifo combo
        self.simulation.network_snr = numpy.array(
            [
                sum([m["simulation"]["snr_%s" % c] ** 2 for c in combo]) ** 0.5
                for m in self
            ]
        )

        # Add floating point time column
        self.simulation.time = numpy.array(
            [m["simulation"]["geocent_end_time"] * 1e-9 for m in self]
        )


class _Missed(_Injection):
    def setup(self, schema, combo, segments):
        super().setup(schema, combo)
        self.color = "#000000"
        self.marker = "x"
        self.segments = segments


class _Found(_Injection):
    def setup(self, schema, combo, segments):
        super().setup(schema, combo)
        self.setup_table(schema, "event")
        self.segments = segments


class _InjByOnIFOs(dict):
    def __init__(self, _segments, _inj, schema, cls):
        super().__init__(
            {
                c: cls([i for i in _inj if i["simulation"]["geocent_end_time"] in s])
                for c, s in _segments.items()
            }
        )
        for c in self:
            self[c].setup(schema, c, _segments[c])


class _FoundByOnIFOs(_InjByOnIFOs):
    def __init__(self, _segments, _found, schema):
        super().__init__(_segments, _found, schema, _Found)


class _MissedByOnIFOs(_InjByOnIFOs):
    def __init__(self, _segments, _missed, schema):
        super().__init__(_segments, _missed, schema, _Missed)

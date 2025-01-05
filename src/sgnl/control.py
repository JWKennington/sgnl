from dataclasses import dataclass

from gwpy.time import tconvert
from sgn.base import SinkElement
from sgn.control import HTTPControl, HTTPControlSinkElement


class SnapShotControl(HTTPControl):
    """Adds snapshot functionality on top of HTTPControl which is on top of SignalEOS"""

    snapshot_interval = 14400
    last_snapshot = {}
    snapshots_enabled = False

    def __enter__(self):
        super().__enter__()
        SnapShotControl.snapshots_enabled = True

    def __exit__(self, exc_type, exc_value, exc_traceback):
        SnapShotControl.snapshots_enabled = False
        super().__exit__(exc_type, exc_value, exc_traceback)

    @classmethod
    def _register_snapshot(cls, elem, t):
        cls.last_snapshot[elem] = t

    @classmethod
    def _snapshot_ready(cls, elem, t):
        if not cls.snapshots_enabled:
            return False
        if elem not in cls.last_snapshot:
            raise ValueError(
                "{elem} not found in last_snapshot, perhaps you forgot to call register_snapshot() when you initialized your element?"
            )
        return t - cls.last_snapshot[elem] >= cls.snapshot_interval

    @classmethod
    def _update_last_snapshot_time(cls, elem, t):
        if elem not in cls.last_snapshot:
            raise ValueError(
                "{elem} not found in last_snapshot, perhaps you forgot to call register_snapshot() when you initialized your element?"
            )
        old_t = cls.last_snapshot[elem]
        cls.last_snapshot[elem] = t
        return (old_t, t - old_t)


# FIXME needs to be the HTTPControlSinkElement
@dataclass
class SnapShotControlSinkElement(HTTPControlSinkElement, SnapShotControl):
    def __post_init__(self):
        HTTPControlSinkElement.__post_init__(self)
        SnapShotControl._register_snapshot(self.name, int(tconvert("now")))
        self.descriptions = []
        self.extensions = []

    def snapshot_ready(self):
        return SnapShotControl._snapshot_ready(self.name, int(tconvert("now")))

    def add_snapshot_filename(self, description, extension):
        self.descriptions.append(description)
        self.extensions.append(extension)

    def snapshot_filenames(self, ifos="H1L1V1"):
        assert (
            self.descriptions is not None
            and self.extensions is not None
            and len(self.descriptions) == len(self.extensions)
        )
        start, duration = SnapShotControl._update_last_snapshot_time(
            self.name, int(tconvert("now"))
        )
        for desc, ext in zip(self.descriptions, self.extensions):
            yield "%s-%s-%d-%d.%s" % (ifos, desc, start, duration, ext)

import os
from dataclasses import dataclass

from gwpy.time import tconvert
from sgn.control import HTTPControl, HTTPControlSinkElement


class SnapShotControl(HTTPControl):
    """Adds snapshot functionality on top of HTTPControl which is on top of SignalEOS"""

    snapshot_interval = 14400
    last_snapshot: dict[str, float] = {}
    snapshots_enabled = False
    # delay = 0
    startup_delay = 0

    def __enter__(self):
        super().__enter__()
        SnapShotControl.snapshots_enabled = True

    def __exit__(self, exc_type, exc_value, exc_traceback):
        SnapShotControl.snapshots_enabled = False
        super().__exit__(exc_type, exc_value, exc_traceback)

    @classmethod
    def _register_snapshot(cls, elem, t, descs):
        cls.last_snapshot[elem] = {}
        t += cls.startup_delay
        for desc in descs:
            cls.last_snapshot[elem][desc] = t
            # t += cls.delay

    @classmethod
    def _snapshot_ready(cls, elem, t, desc):
        if not cls.snapshots_enabled:
            return False
        if elem not in cls.last_snapshot:
            raise ValueError(
                "{elem} not found in last_snapshot, perhaps you forgot to call "
                "register_snapshot() when you initialized your element?"
            )
        return t - cls.last_snapshot[elem][desc] >= cls.snapshot_interval

    @classmethod
    def _update_last_snapshot_time(cls, elem, t, desc):
        if elem not in cls.last_snapshot:
            raise ValueError(
                "{elem} not found in last_snapshot, perhaps you forgot to call "
                "register_snapshot() when you initialized your element?"
            )
        old_t = cls.last_snapshot[elem][desc]
        cls.last_snapshot[elem][desc] = t
        return (old_t, t - old_t)


# FIXME needs to be the HTTPControlSinkElement
@dataclass
class SnapShotControlSinkElement(HTTPControlSinkElement, SnapShotControl):
    def __post_init__(self):
        HTTPControlSinkElement.__post_init__(self)
        self.descriptions = []
        self.extensions = []
        self.startup_delays = {}

    def snapshot_ready(self, desc):
        return SnapShotControl._snapshot_ready(self.name, int(tconvert("now")), desc)

    def add_snapshot_filename(self, description, extension):
        self.descriptions.append(description)
        self.extensions.append(extension)

    def register_snapshot(self):
        # shoud be called after adding filenames
        SnapShotControl._register_snapshot(
            self.name, int(tconvert("now")), self.descriptions
        )
        for desc in self.descriptions:
            self.startup_delays[desc] = self.startup_delay

    def snapshot_filenames(self, desc, ifos="H1L1V1"):
        assert (
            self.descriptions is not None
            and self.extensions is not None
            and len(self.descriptions) == len(self.extensions)
        )
        start, duration = SnapShotControl._update_last_snapshot_time(
            self.name, int(tconvert("now")), desc
        )
        startup_delay = self.startup_delays[desc]
        if startup_delay:
            start -= startup_delay
            duration += startup_delay
            self.startup_delays[desc] = 0

        gpsdir = str(start)[:5]
        try:
            os.mkdir(gpsdir)
        except OSError:
            pass

        ext = self.extensions[self.descriptions.index(desc)]
        return "%s/%s-%s-%d-%d.%s" % (gpsdir, ifos, desc, start, duration, ext)

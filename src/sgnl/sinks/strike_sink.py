"""A sink element to write out triggers to likelihood ratio class in strike."""

# Copyright (C) 2024-2025 Yun-Jing Huang, Chad Hanna, Prathamesh Joshi, Leo Tsukada
#                         Zach Yarbrough

import io
from dataclasses import dataclass
from time import asctime

from sgnl.control import SnapShotControlSinkElement
from sgnl.strike_object import StrikeObject


@dataclass
class event_dummy(object):
    ifo: list[str]
    end: float
    snr: float
    chisq: float
    combochisq: float
    template_id: int


def xml_string(rstat):
    f = io.BytesIO()
    rstat.save_fileobj(f)
    f.seek(0)
    return f.read().decode("utf-8")


@dataclass
class StrikeSink(SnapShotControlSinkElement):
    ifos: list[str] = None
    is_online: bool = False
    injections: bool = False
    strike_object: StrikeObject = None
    bankids_map: dict[str, list] = None
    background_pad: str = None
    horizon_pads: list[str] = None
    count_removal_times: list[int] = None

    def __post_init__(self):
        assert isinstance(self.ifos, list)
        assert isinstance(self.background_pad, str)
        assert isinstance(self.horizon_pads, list)
        assert self.strike_object is not None
        self.sink_pad_names = (self.background_pad,) + tuple(self.horizon_pads)

        super().__post_init__()

        if self.is_online and not self.injections:
            # setup bottle

            self.state_dict = {
                "xml": {},
                "zerolagxml": {},
                "count_tracker": 0,
                "count_removal_times": [],
            }

            for bankid in self.bankids_map:
                four_digit_id = "%04d" % int(bankid)
                self.add_snapshot_filename(
                    "%s_SGNL_LIKELIHOOD_RATIO" % four_digit_id, "xml.gz"
                )
                self.state_dict["xml"][four_digit_id] = xml_string(
                    self.strike_object.likelihood_ratios[bankid]
                )
                self.state_dict["zerolagxml"][four_digit_id] = xml_string(
                    self.strike_object.zerolag_rank_stat_pdfs[bankid]
                )

                if self.count_removal_times is None:
                    self.count_removal_times = (
                        self.strike_object.likelihood_ratios[bankid]
                        .terms["P_of_SNR_chisq"]
                        .remove_counts_times
                    )
                    self.state_dict["count_removal_times"] = self.count_removal_times

                else:
                    assert (
                        self.count_removal_times
                        == self.strike_object.likelihood_ratios[bankid]
                        .terms["P_of_SNR_chisq"]
                        .remove_counts_times
                    )
            self.register_snapshot()

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)

        if self.rsnks[pad] == self.background_pad:
            background = frame["background"].data
            if background is None:
                return

            #
            # Background triggers
            #
            self.strike_object.train_noise(
                frame["background"].ts / 1e9,
                background["snrs"],
                background["chisqs"],
                background["single_masks"],
            )

            #
            # Trigger rates
            #
            # FIXME : come up with a way to make populating the trigger rate
            # object as part of train_noise
            trigger_rates = frame["trigger_rates"].data
            for ifo, trigger_rate in trigger_rates.items():
                for bankid in self.bankids_map:
                    buf_seg, count = trigger_rate[bankid]
                    self.strike_object.likelihood_ratios[bankid].terms[
                        "P_of_tref_Dh"
                    ].triggerrates[ifo].add_ratebin(list(buf_seg), count)

        elif self.rsnks[pad] in self.horizon_pads:
            data = frame["data"]
            if data.ts - data.te == 0:
                return

            ifo = data.data["ifo"]
            horizon = data.data["horizon"]
            horizon_time = data.data["epoch"] / 1_000_000_000
            # Epoch is the mid point of the most recent FFT
            # interval used to obtain this PSD
            if (
                horizon is not None
                and float(data.data["n_samples"] / data.data["navg"]) > 0.3
            ):
                # n_samples / navg is the "stability", which is a measure of the
                # fraction of the configured averaging timescale used to obtain this
                # measurement.
                for bankid in self.bankids_map:
                    self.strike_object.likelihood_ratios[bankid].terms[
                        "P_of_tref_Dh"
                    ].horizon_history[ifo][horizon_time] = horizon[bankid]
            else:
                for bankid in self.bankids_map:
                    self.strike_object.likelihood_ratios[bankid].terms[
                        "P_of_tref_Dh"
                    ].horizon_history[ifo][horizon_time] = 0

    def internal(self):
        if self.injections:
            return
        if self.is_online:

            SnapShotControlSinkElement.exchange_state(self.name, self.state_dict)

            if self.state_dict["count_tracker"] != 0:
                self.count_removal_callback()

        if self.at_eos:
            if self.is_online:
                for bankid in self.bankids_map:
                    desc = "%s_SGNL_LIKELIHOOD_RATIO" % bankid
                    fn = self.snapshot_filenames(desc)
                    self.on_snapshot(bankid, fn)
            else:
                for bankid in self.bankids_map:
                    self.strike_object.save_snr_chi_lnpdf(bankid)
                    self.strike_object.save_counts_by_template_id(bankid)
                    self.strike_object.likelihood_ratios[bankid].save(
                        self.strike_object.output_likelihood_file[bankid]
                    )
        else:
            if self.is_online:
                for i, bankid in enumerate(self.bankids_map):
                    desc = "%s_SGNL_LIKELIHOOD_RATIO" % bankid
                    if self.snapshot_ready(desc):
                        fn = self.snapshot_filenames(desc)
                        self.on_snapshot(bankid, fn)
                        print(f"{asctime()} SrikeSink: update assign lr {bankid}...")
                        self.strike_object.update_assign_lr(bankid)
                        if i == 0:
                            self.strike_object.load_rank_stat_pdf()

    def on_snapshot(self, bankid, fn):
        self.strike_object.save_snr_chi_lnpdf(bankid)
        self.strike_object.save_counts_by_template_id(bankid)
        four_digit_id = "%04d" % int(bankid)

        # update bottle
        self.state_dict["xml"][four_digit_id] = xml_string(
            self.strike_object.likelihood_ratios[bankid]
        )
        self.state_dict["zerolagxml"][four_digit_id] = xml_string(
            self.strike_object.zerolag_rank_stat_pdfs[bankid]
        )
        print(f"{asctime()} Writing out likelihood ratio and zerolag file {fn}...")
        self.strike_object.save_snapshot(bankid, fn)

    def count_removal_callback(self):
        # FIXME : at the time of calling exchange_state() posted data is already
        # added on top of existing remove counts list and converted into json
        # format. where should I add the functionality?
        # FIXME : how can I make this callback to be called upon posting remove
        # count info?
        # FIXME : how can an external program know the self.name of StrikeSink
        # for each inspiral job, which is part of URL to post information to but
        # not included in registry.txt?

        if self.state_dict["count_tracker"] > 0:
            self.count_removal_times.append(self.state_dict["count_tracker"])

        elif self.state_dict["count_tracker"] < 0:
            gps_time = abs(self.state_dict["count_tracker"])
            if gps_time in self.count_removal_times:
                self.count_removal_times.remove(gps_time)
            else:
                print(f"{gps_time} not in self.count_removal_times, not removing")

        self.state_dict["count_removal_times"] = self.count_removal_times

        for bankid, likelihood_ratio in self.strike_object.likelihood_ratios.items():
            # four_digit_id = "%04d" % int(bankid)

            # update the internal array for the removed times
            print(f"bankid: {bankid} count remove times: {self.count_removal_times}")
            likelihood_ratio.terms["P_of_SNR_chisq"].remove_counts_times = (
                self.count_removal_times
            )

        self.state_dict["count_tracker"] = 0

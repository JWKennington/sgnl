"""A sink element to write out triggers to likelihood ratio class in strike."""

# Copyright (C) 2024-2025 Yun-Jing Huang, Chad Hanna, Prathamesh Joshi, Leo Tsukada
#                         Zach Yarbrough

import time
from dataclasses import dataclass
from queue import Empty

from sgn.subprocess import Parallelize, ParallelizeSinkElement

from sgnl.control import SnapShotControlSinkElement
from sgnl.strike_object import StrikeObject, xml_string


def on_snapshot(data, shutdown=False, reset_dynamic=True):
    lr = data["lr"]
    zero_lr = data["zero_lr"]
    bankid = data["bankid"]
    output_likelihood_file = data["output_likelihood_file"]
    output_zerolag_likelihood_file = data["output_zerolag_likelihood_file"]
    fn = data["fn"]

    # output LR files
    StrikeObject.snapshot_io(
        lr,
        zero_lr,
        fn,
        output_likelihood_file,
        output_zerolag_likelihood_file,
    )

    if shutdown:
        print("shutdown, don't update lr", flush=True)
        return

    # make xmlobj for http/bottle
    sdict = StrikeObject.snapshot_fileobj(
        lr,
        zero_lr,
        bankid,
    )

    # update LR
    frankenstein, likelihood_ratio_upload = StrikeObject._update_assign_lr(lr)

    # set triggerrates and horizon_history to zero since they will be
    # replaced anyways
    # Only reset in multiprocessing mode

    if reset_dynamic:
        StrikeObject.reset_dynamic(frankenstein, likelihood_ratio_upload)

    sdict["frankenstein"] = {bankid: frankenstein}
    sdict["likelihood_ratio_upload"] = {bankid: likelihood_ratio_upload}
    sdict["bankid"] = bankid
    return sdict


@dataclass
class StrikeSink(SnapShotControlSinkElement, ParallelizeSinkElement):
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
        self.temp = 0

        SnapShotControlSinkElement.__post_init__(self)
        if not self.injections:
            ParallelizeSinkElement.__post_init__(self)

        if self.is_online and not self.injections:
            # setup bottle

            self.state_dict = {
                "xml": {},
                "zerolagxml": {},
                "count_tracker": 0,
                "count_removal_times": [],
            }

            for bankid in self.bankids_map:
                self.add_snapshot_filename(
                    "%s_SGNL_LIKELIHOOD_RATIO" % bankid, "xml.gz"
                )
                self.state_dict["xml"][bankid] = xml_string(
                    self.strike_object.likelihood_ratios[bankid]
                )
                self.state_dict["zerolagxml"][bankid] = xml_string(
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

    def process_outqueue(self, sdict):
        # FIXME This reduces the ram, but is a mess
        xml = sdict["xml"]
        zerolagxml = sdict["zerolagxml"]
        count_tracker = self.state_dict["count_tracker"]
        count_removal_times = self.state_dict["count_removal_times"]
        del self.state_dict
        self.state_dict = {
            "xml": xml,
            "zerolagxml": zerolagxml,
            "count_tracker": count_tracker,
            "count_removal_times": count_removal_times,
        }

        frankenstein = sdict["frankenstein"]
        likelihood_ratio_upload = sdict["likelihood_ratio_upload"]
        bankid = sdict["bankid"]
        self.strike_object.update_dynamic(
            bankid, frankenstein[bankid], likelihood_ratio_upload[bankid]
        )
        del sdict

    def get_state_from_queue(self):
        try:
            sdict = self.out_queue.get_nowait()
            self.process_outqueue(sdict)
        except Empty:
            return

    def internal(self):
        if self.injections:
            return
        if self.is_online:
            if Parallelize.enabled:
                # FIXME: is this the correct logic?
                ParallelizeSinkElement.internal(self)
            self.get_state_from_queue()
            SnapShotControlSinkElement.exchange_state(self.name, self.state_dict)

            if self.state_dict["count_tracker"] != 0:
                self.count_removal_callback()

        if self.at_eos:
            if self.is_online:
                if Parallelize.enabled:
                    if self.terminated.is_set():
                        print("At EOS and subprocess is terminated")
                    else:
                        drained_outq = self.sub_process_shutdown(600)
                        print("after shutdown", len(drained_outq))

                # Do this in the main thread
                # no reason to put this in subprocess
                # only write out the LR files, don't update
                for bankid in self.bankids_map:
                    desc = "%s_SGNL_LIKELIHOOD_RATIO" % bankid
                    fn = self.snapshot_filenames(desc)
                    self.strike_object.update_array_data(bankid)
                    data = self.strike_object.prepare_inq_data(fn, bankid)
                    sdict = on_snapshot(data, shutdown=True, reset_dynamic=False)
            else:
                for bankid in self.bankids_map:
                    self.strike_object.update_array_data(bankid)
                    self.strike_object.likelihood_ratios[bankid].save(
                        self.strike_object.output_likelihood_file[bankid]
                    )
        else:
            if self.is_online:
                for i, bankid in enumerate(self.bankids_map):
                    desc = "%s_SGNL_LIKELIHOOD_RATIO" % bankid
                    if self.snapshot_ready(desc):
                        fn = self.snapshot_filenames(desc)
                        self.strike_object.update_array_data(bankid)
                        data = self.strike_object.prepare_inq_data(fn, bankid)
                        if Parallelize.enabled:
                            self.in_queue.put(data)
                        else:
                            sdict = on_snapshot(
                                data, shutdown=False, reset_dynamic=False
                            )
                            self.process_outqueue(sdict)

                        if i == 0:
                            self.strike_object.load_rank_stat_pdf()

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
            # update the internal array for the removed times
            print(f"bankid: {bankid} count remove times: {self.count_removal_times}")
            likelihood_ratio.terms["P_of_SNR_chisq"].remove_counts_times = (
                self.count_removal_times
            )

        self.state_dict["count_tracker"] = 0

    @staticmethod
    def sub_process_internal(**kwargs):
        inq, outq = kwargs["inq"], kwargs["outq"]

        try:
            data = inq.get(timeout=2)
            sdict = on_snapshot(data, kwargs["worker_shutdown"].is_set(), True)
            if sdict is not None:
                outq.put(sdict)
                time.sleep(10)
        except Empty:
            return

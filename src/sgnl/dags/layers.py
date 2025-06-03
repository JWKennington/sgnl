"""A module to contruct layers in a DAG for inspiral workflows."""

# Copyright (C) 2020       Patrick Godwin
# Copyright (C) 2024-2025  Yun-Jing Huang, Cort Posnansky

import itertools
import os
import shutil
from collections.abc import Mapping
from math import ceil
from typing import Iterable

import numpy
from ezdag import Argument, Layer, Node, Option
from lal import rate

from sgnl.dags import util
from sgnl.dags.util import condor_scratch_space, format_ifo_args, groups, to_ifo_list

DEFAULT_MAX_FILES = 500


def create_layer(executable, condor_config, resource_requests, retries=3, name=None):
    # Default submit file options
    submit_description = create_submit_description(condor_config)

    # Add resource requests
    submit_description.update(resource_requests)

    # Allow arbitrary submit file options from the config
    if executable in condor_config:
        submit_description.update(condor_config[executable])

    # Set file transfer
    if condor_config.transfer_files is not None:
        transfer_files = condor_config.transfer_files
    else:
        transfer_files = True

    return Layer(
        executable,
        name=name if name else executable,
        transfer_files=transfer_files,
        submit_description=submit_description,
        retries=retries,
    )


def create_submit_description(condor_config):
    submit_description = {
        "want_graceful_removal": "True",
        "kill_sig": "15",
        "accounting_group": condor_config.accounting_group,
        "accounting_group_user": condor_config.accounting_group_user,
    }

    if condor_config.getenv:
        submit_description["getenv"] = condor_config.getenv

    requirements = []
    environment = {}

    # Container options
    if "container" in condor_config:
        submit_description["MY.SingularityImage"] = f'"{condor_config.container}"'
        submit_description["transfer_executable"] = False

    # Scitoken options
    if condor_config.use_scitokens is True:
        submit_description["use_oauth_services"] = "scitokens"
        environment["BEARER_TOKEN_FILE"] = (
            f"$$(CondorScratchDir)/.condor_creds/"
            f"{submit_description['use_oauth_services']}.use"
        )

    # Config options
    if "directives" in condor_config:
        submit_description.update(condor_config["directives"])
    if "requirements" in condor_config:
        requirements.extend(condor_config["requirements"])
    if "environment" in condor_config:
        environment.update(condor_config["environment"])

    # Condor requirements
    submit_description["requirements"] = " && ".join(requirements)

    # Condor environment
    env_opts = [f"{key}={val}" for (key, val) in environment.items()]
    if "environment" in submit_description:
        env_opts.append(submit_description["environment"].strip('"'))
    submit_description["environment"] = f'"{" ".join(env_opts)}"'

    return submit_description


def test(echo_config, condor_config):
    # FIXME Delete this layer
    executable = "echo"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "10MB",
        "request_disk": "10MB",
    }
    layer = create_layer(executable, condor_config, resource_requests)

    for i in range(echo_config.jobs):
        arguments = [Argument("words", f"This is job {i}")]
        inputs = [Argument("file_in", "inputs/file.txt")]
        outputs = [Argument("file_out", "outputs/output.txt")]
        layer += Node(arguments=arguments, inputs=inputs, outputs=outputs)

    return layer


def reference_psd(psd_config, source_config, condor_config, ref_psd_cache):
    executable = "sgnl-reference-psd"
    resource_requests = {
        "request_cpus": 2,
        "request_memory": "2GB",
        "request_disk": "2GB",
    }
    layer = create_layer(executable, condor_config, resource_requests)

    common_opts = [
        Option("data-source", "frames"),
        Option("psd-fft-length", psd_config.fft_length),
        Option("frame-segments-name", source_config.frame_segments_name),
    ]

    for (ifo_combo, span), psds in ref_psd_cache.groupby("ifo", "time").items():
        ifos = to_ifo_list(ifo_combo)
        start, end = span

        arguments = [
            Option("gps-start-time", int(start)),
            Option("gps-end-time", int(end)),
            Option("channel-name", format_ifo_args(ifos, source_config.channel_name)),
            *common_opts,
        ]
        inputs = [
            Option(
                "frame-segments-file", source_config.frame_segments_file, track=False
            )
        ]

        if source_config.frame_cache:
            inputs.append(Option("frame-cache", source_config.frame_cache, track=False))
        else:
            arguments.extend(
                [
                    Option(
                        "frame-type", format_ifo_args(ifos, source_config.frame_type)
                    ),
                    Option("data-find-server", source_config.data_find_server),
                ]
            )

        layer += Node(
            arguments=arguments,
            inputs=inputs,
            outputs=Option("output-name", psds.files),
        )

    return layer


def median_psd(psd_config, condor_config, ref_psd_cache, median_psd_cache):
    executable = "sgnl-median-of-psds"
    resource_requests = {
        "request_cpus": 2,
        "request_memory": "2GB",
        "request_disk": "2GB",
    }
    layer = create_layer(executable, condor_config, resource_requests)

    layer += Node(
        inputs=Option("input-files", ref_psd_cache.files),
        outputs=Option("output-name", median_psd_cache.files),
    )

    return layer


def svd_bank(
    svd_config,
    condor_config,
    all_ifos,
    split_bank_cache,
    median_psd_cache,
    svd_cache,
    svd_bins,
    svd_stats,
):
    executable = "sgnl-inspiral-svd-bank"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "7GB",
        "request_disk": "2GB",
    }
    layer = create_layer(executable, condor_config, resource_requests)

    arguments = [
        Option("f-low", svd_config.f_low),
        Option("f-final", svd_config.max_f_final),
        Option("approximant", svd_config.approximant),
        Option("overlap", svd_config.overlap),
        Option("instrument", to_ifo_list(all_ifos)),
        Option("n", svd_config.num_split_templates),
        Option("num-banks", svd_config.num_banks),
        Option("sort-by", svd_config.sort_by),
    ]

    if svd_config.autocorrelation_length:
        mchirp_to_ac_length = autocorrelation_length_map(
            svd_config.autocorrelation_length
        )

    split_banks = split_bank_cache.groupby("bin")
    for (ifo, svd_bin), svd_banks in svd_cache.groupby("ifo", "bin").items():

        # grab sub-bank specific configuration if available
        if "bank_name" in svd_stats["bins"][svd_bin]:
            bank_name = svd_stats["bins"][svd_bin]["bank_name"]
            this_svd_config = svd_config.sub_banks[bank_name]
        else:
            this_svd_config = svd_config

        arguments = [
            Option("instrument-override", ifo),
            Option("flow", this_svd_config.f_low),
            Option("samples-min", this_svd_config.samples_min),
            Option("samples-max-64", this_svd_config.samples_max_64),
            Option("samples-max-256", this_svd_config.samples_max_256),
            Option("samples-max", this_svd_config.samples_max),
            Option("svd-tolerance", this_svd_config.tolerance),
        ]

        # FIXME: in gstlal offline, autocrrelation_length is in the option file,
        # but in online, it's in the config. Which one should we use?
        if this_svd_config.autocorrelation_length:
            bin_mchirp = svd_stats["bins"][svd_bin]["mean_mchirp"]
            arguments.append(
                Option("autocorrelation-length", mchirp_to_ac_length(bin_mchirp))
            )
            if "ac_length" in svd_stats["bins"][svd_bin]:
                # FIXME: sanity check autocorrelation length is the same, remove
                # this once we decide on a single location to get the
                # autocorrelation length
                assert svd_stats["bins"][svd_bin] == mchirp_to_ac_length(bin_mchirp)
        elif "ac_length" in svd_stats["bins"][svd_bin]:
            arguments.append(
                Option(
                    "autocorrelation-length", svd_stats["bins"][svd_bin]["ac_length"]
                )
            )
        else:
            raise ValueError("Unknown autocorrelation length option")
        if "max_duration" in this_svd_config:
            arguments.append(Option("max-duration", this_svd_config.max_duration))
        if "sample_rate" in this_svd_config:
            arguments.append(Option("sample-rate", this_svd_config.sample_rate))
        # FIXME figure out where this option should live
        # if 'use_bankchisq' in config.rank and config.rank.use_bankchisq:
        #    arguments.append(Option("use-bankchisq"))

        layer += Node(
            arguments=arguments,
            inputs=[
                Option("reference-psd", median_psd_cache.files),
                Option("template-banks", sorted(split_banks[svd_bin].files)),
            ],
            outputs=Option("write-svd", svd_banks.files),
        )

    return layer


def filter(
    psd_config,
    svd_config,
    filter_config,
    source_config,
    condor_config,
    ref_psd_cache,
    svd_bank_cache,
    lr_cache,
    trigger_cache,
    svd_stats,
    min_instruments,
):
    executable = "sgnl-inspiral"
    resource_requests = {
        "request_cpus": 2,
        "request_memory": "4GB",
        "request_disk": "5GB",
    }

    # Set torch-device
    if filter_config.torch_device:
        torch_device = filter_config.torch_device
    else:
        torch_device = "cpu"

    if "cuda" in torch_device:
        resource_requests["request_gpus"] = 1
        resource_requests["request_cpus"] = 1

    layer = create_layer(executable, condor_config, resource_requests)

    common_opts = [
        Option("track-psd"),
        Option("data-source", "frames"),
        Option("psd-fft-length", psd_config.fft_length),
        Option("frame-segments-name", source_config.frame_segments_name),
        Option("coincidence-threshold", filter_config.coincidence_threshold),
        Option("snr-min", filter_config.snr_min),
    ]

    common_opts.append(Option("min-instruments-candidates", min_instruments))

    if filter_config.all_triggers_to_background:
        common_opts.append(Option("all-triggers-to-background"))

    common_inputs = [
        Option("event-config", filter_config.event_config_file),
    ]

    # Set torch-dtype
    if filter_config.torch_dtype:
        torch_dtype = filter_config.torch_dtype
    else:
        torch_dtype = "float32"
    common_opts.append(Option("torch-dtype", torch_dtype))

    common_opts.append(Option("torch-device", torch_device))

    # Set trigger-finding-duration
    if filter_config.trigger_finding_duration:
        trigger_finding_duration = filter_config.trigger_finding_duration
    else:
        trigger_finding_duration = 1
    common_opts.append(Option("trigger-finding-duration", trigger_finding_duration))

    # Checkpoint by grouping SVD bins together
    if filter_config.group_svd_num:
        group_svd_num = filter_config.group_svd_num
    else:
        group_svd_num = 1

    ref_psds = ref_psd_cache.groupby("ifo", "time")
    svd_banks = svd_bank_cache.groupby("ifo", "bin")
    lrs = lr_cache.groupby("ifo", "time", "bin")
    for (ifo_combo, span), triggers in trigger_cache.groupby("ifo", "time").items():
        ifos = to_ifo_list(ifo_combo)
        start, end = span

        filter_opts = [
            Option("gps-start-time", int(start)),
            Option("gps-end-time", int(end)),
            Option("channel-name", format_ifo_args(ifos, source_config.channel_name)),
        ]
        inputs = [
            Option("frame-segments-file", source_config.frame_segments_file),
            # Option("veto-segments-file", filter_config.veto_segments_file),
            Option("reference-psd", ref_psds[(ifo_combo, span)].files),
            # Option("time-slide-file", filter_config.time_slide_file),
        ]

        if source_config.frame_cache:
            inputs.append(Option("frame-cache", source_config.frame_cache, track=False))
        else:
            filter_opts.extend(
                [
                    Option(
                        "frame-type", format_ifo_args(ifos, source_config.frame_type)
                    ),
                    Option("data-find-server", source_config.data_find_server),
                ]
            )

        if source_config.idq_channel_name:
            filter_opts.append(
                Option(
                    "idq-channel-name",
                    format_ifo_args(ifos, source_config.idq_channel_name),
                )
            )

        filter_opts.extend(common_opts)

        if source_config.idq_channel_name and filter_config.idq_gate_threshold:
            filter_opts.append(
                Option("idq-gate-threshold", filter_config.idq_gate_threshold)
            )

        if source_config.idq_channel_name and source_config.idq_state_channel_name:
            filter_opts.append(
                Option(
                    "idq-state-channel-name",
                    format_ifo_args(ifos, source_config.idq_state_channel_name),
                )
            )

        for trigger_group in triggers.chunked(group_svd_num):
            svd_bins = trigger_group.groupby("bin").keys()

            thresholds = [
                svd_stats.bins[svd_bin]["ht_gate_threshold"] for svd_bin in svd_bins
            ]
            these_opts = [Option("ht-gate-threshold", thresholds), *filter_opts]

            svd_bank_files = util.flatten(
                [
                    svd_banks[(ifo, svd_bin)].files
                    for ifo in ifos
                    for svd_bin in svd_bins
                ]
            )
            lr_files = util.flatten(
                [lrs[(ifo_combo, span, svd_bin)].files for svd_bin in svd_bins]
            )

            layer += Node(
                arguments=these_opts,
                inputs=[
                    Option("svd-bank", svd_bank_files),
                    *inputs,
                    *common_inputs,
                ],
                outputs=[
                    Option("trigger-output", trigger_group.files),
                    Option("output-likelihood-file", lr_files),
                ],
            )

    return layer


def injection_filter(
    psd_config,
    svd_config,
    filter_config,
    injection_config,
    source_config,
    condor_config,
    ref_psd_cache,
    svd_bank_cache,
    trigger_cache,
    svd_stats,
    min_instruments,
):
    executable = "sgnl-inspiral"
    resource_requests = {
        "request_cpus": 2,
        "request_memory": "5GB",
        "request_disk": "5GB",
    }
    # Set torch-device
    if filter_config.torch_device:
        torch_device = filter_config.torch_device
    else:
        torch_device = "cpu"

    if "cuda" in torch_device:
        resource_requests["request_gpus"] = 1
        resource_requests["request_cpus"] = 1

    layer = create_layer(
        executable, condor_config, resource_requests, name="sgnl-inspiral-inj"
    )

    common_opts = [
        Option("track-psd"),
        Option("data-source", "frames"),
        Option("psd-fft-length", psd_config.fft_length),
        Option("frame-segments-name", source_config.frame_segments_name),
        Option("coincidence-threshold", filter_config.coincidence_threshold),
        Option("snr-min", filter_config.snr_min),
        Option("injections"),
    ]

    common_opts.append(Option("min-instruments-candidates", min_instruments))

    common_inputs = [
        Option("event-config", filter_config.event_config_file),
    ]

    # Set torch-dtype
    if filter_config.torch_dtype:
        torch_dtype = filter_config.torch_dtype
    else:
        torch_dtype = "float32"
    common_opts.append(Option("torch-dtype", torch_dtype))

    common_opts.append(Option("torch-device", torch_device))

    # Set trigger-finding-duration
    if filter_config.trigger_finding_duration:
        trigger_finding_duration = filter_config.trigger_finding_duration
    else:
        trigger_finding_duration = 1
    common_opts.append(Option("trigger-finding-duration", trigger_finding_duration))

    # Checkpoint by grouping SVD bins together
    if filter_config.group_svd_num:
        group_svd_num = filter_config.group_svd_num
    else:
        group_svd_num = 1

    ref_psds = ref_psd_cache.groupby("ifo", "time")
    svd_banks = svd_bank_cache.groupby("ifo", "bin")
    for (ifo_combo, span, inj_type), triggers in trigger_cache.groupby(
        "ifo", "time", "subtype"
    ).items():
        ifos = to_ifo_list(ifo_combo)
        start, end = span
        injection_file = injection_config.filter[inj_type]["file"]

        filter_opts = [
            Option("gps-start-time", int(start)),
            Option("gps-end-time", int(end)),
        ]
        inputs = [
            Option("frame-segments-file", source_config.frame_segments_file),
            Option("reference-psd", ref_psds[(ifo_combo, span)].files),
            Option("injection-file", injection_file),
            # Option("time-slide-file", filter_config.time_slide_file),
        ]

        if (
            source_config.inj_frame_cache
            and injection_config.filter[inj_type]["noiseless_inj_frames"]
        ):
            inputs.append(Option("frame-cache", source_config.frame_cache, track=False))
            inputs.append(
                Option(
                    "noiseless-inj-frame-cache",
                    source_config.inj_frame_cache,
                    track=False,
                )
            )
            filter_opts.append(
                Option(
                    "noiseless-inj-channel-name",
                    format_ifo_args(ifos, source_config.inj_channel_name),
                )
            )
            filter_opts.append(
                Option(
                    "channel-name", format_ifo_args(ifos, source_config.channel_name)
                )
            )
        elif (
            source_config.inj_frame_cache
            and not injection_config.filter[inj_type]["noiseless_inj_frames"]
        ):
            inputs.append(
                Option("frame-cache", source_config.inj_frame_cache, track=False)
            )
            filter_opts.append(
                Option(
                    "channel-name",
                    format_ifo_args(ifos, source_config.inj_channel_name),
                )
            )
        elif source_config.frame_cache and not source_config.inj_frame_cache:
            inputs.append(Option("frame-cache", source_config.frame_cache, track=False))
            filter_opts.append(
                Option(
                    "channel-name", format_ifo_args(ifos, source_config.channel_name)
                )
            )
        else:
            filter_opts.extend(
                [
                    Option(
                        "frame-type", format_ifo_args(ifos, source_config.frame_type)
                    ),
                    Option("data-find-server", source_config.data_find_server),
                    Option(
                        "channel-name",
                        format_ifo_args(ifos, source_config.channel_name),
                    ),
                ]
            )

        if source_config.idq_channel_name:
            filter_opts.append(
                Option(
                    "idq-channel-name",
                    format_ifo_args(ifos, source_config.idq_channel_name),
                )
            )

        filter_opts.extend(common_opts)

        if source_config.idq_channel_name and filter_config.idq_gate_threshold:
            filter_opts.append(
                Option("idq-gate-threshold", filter_config.idq_gate_threshold)
            )

        if source_config.idq_channel_name and source_config.idq_state_channel_name:
            filter_opts.append(
                Option(
                    "idq-state-channel-name",
                    format_ifo_args(ifos, source_config.idq_state_channel_name),
                )
            )

        for trigger_group in triggers.chunked(group_svd_num):
            svd_bins = trigger_group.groupby("bin").keys()

            thresholds = [
                svd_stats.bins[svd_bin]["ht_gate_threshold"] for svd_bin in svd_bins
            ]
            these_opts = [Option("ht-gate-threshold", thresholds), *filter_opts]

            svd_bank_files = util.flatten(
                [
                    svd_banks[(ifo, svd_bin)].files
                    for ifo in ifos
                    for svd_bin in svd_bins
                ]
            )

            layer += Node(
                arguments=these_opts,
                inputs=[
                    Option("svd-bank", svd_bank_files),
                    *inputs,
                    *common_inputs,
                ],
                outputs=[
                    Option("trigger-output", trigger_group.files),
                ],
            )

    return layer


def aggregate(filter_config, condor_config, trigger_cache, clustered_triggers_cache):
    executable = "stillsuit-merge-reduce"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
        "request_disk": "1GB",
    }

    layer = create_layer(executable, condor_config, resource_requests)

    clustered_triggers = clustered_triggers_cache.groupby("ifo", "time", "bin")

    # cluster triggers by SNR
    for (ifo_combo, span, svd_bin), triggers in trigger_cache.groupby(
        "ifo", "time", "bin"
    ).items():
        layer += Node(
            arguments=[
                Option("clustering-column", "network_chisq_weighted_snr"),
                Option("clustering-window", 0.1),
            ],
            inputs=[
                Option("config-schema", filter_config.event_config_file),
                Option("dbs", triggers.files),
            ],
            outputs=Option(
                "db-to-insert-into",
                clustered_triggers[(ifo_combo, span, svd_bin)].files,
            ),
        )
    return layer


def marginalize_likelihood_ratio(
    condor_config,
    prior_config,
    lr_cache,
    marg_lr_cache,
    prior_cache=None,
):

    executable = "strike-marginalize-likelihood"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
        "request_disk": "3GB",
    }
    if prior_cache is not None:
        name = executable + "-prior"
        prior = prior_cache.groupby("bin")
    else:
        name = executable + "-likelihood-ratio-across-time"
    layer = create_layer(
        executable,
        condor_config,
        resource_requests,
        name=name,
    )

    lrs = lr_cache.groupby("bin")

    for svd_bin, marg_lrs in marg_lr_cache.groupby("bin").items():
        layer += Node(
            arguments=Option("marginalize", "likelihood-ratio"),
            inputs=[
                Option(
                    "input",
                    (
                        lrs[svd_bin].files + prior[svd_bin].files
                        if prior_cache is not None
                        else lrs[svd_bin].files
                    ),
                )
            ],
            outputs=Option("output", marg_lrs.files),
        )

    return layer


def create_prior(
    condor_config,
    prior_config,
    coincidence_threshold,
    svd_bank_cache,
    prior_cache,
    ifos,
    min_instruments,
    svd_stats,
    write_empty_zerolag=None,
    write_empty_marg_zerolag=None,
):
    executable = "sgnl-create-prior-diststats"
    resource_requests = {
        "request_cpus": 2,
        "request_memory": "4GB",
        "request_disk": "3GB",
    }
    layer = create_layer(executable, condor_config, resource_requests)

    svd_banks = svd_bank_cache.groupby("bin")
    arguments = [
        Option("instrument", ifos),
        Option("min-instruments", min_instruments),
        Option("coincidence-threshold", coincidence_threshold),
    ]
    if write_empty_zerolag:
        zerolag_pdf = write_empty_zerolag.groupby("bin")
    if write_empty_marg_zerolag:
        marg_zerolag_pdf = write_empty_marg_zerolag.files[0]

    for i, (svd_bin, prior) in enumerate(prior_cache.groupby("bin").items()):
        inputs = [
            Option("svd-file", svd_banks[svd_bin].files),
            *add_likelihood_ratio_file_options(svd_bin, svd_stats, prior_config),
        ]
        outputs = [Option("output-likelihood-file", prior.files)]
        if write_empty_zerolag and write_empty_marg_zerolag:
            # create an empty rankingstatpdf using the first svd bin's prior file
            # as a bootstrap. This is meant to be used during the setup stage of
            # an online analysis to create the zerolag pdf file
            if i == 0:
                outputs += [
                    Option(
                        "write-empty-rankingstatpdf",
                        zerolag_pdf[(svd_bin)].files + [marg_zerolag_pdf],
                    )
                ]
            else:
                outputs += [
                    Option(
                        "write-empty-rankingstatpdf",
                        zerolag_pdf[(svd_bin)].files + [" /dev/null"],
                    )
                ]

        layer += Node(
            arguments=arguments,
            inputs=inputs,
            outputs=outputs,
        )

    return layer


def add_trigger_dbs(
    condor_config, filter_config, trigger_cache, clustered_trigger_cache, column, window
):
    executable = "stillsuit-merge-reduce"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "6GB",
        "request_disk": "2GB",
    }
    layer = create_layer(
        executable,
        condor_config,
        resource_requests,
        name=executable + "-" + column.replace("_", "-"),
    )

    triggers = trigger_cache.groupby("bin", "subtype")

    # cluster triggers by SNR across time
    for (svd_bin, subtype), clustered_triggers in clustered_trigger_cache.groupby(
        "bin", "subtype"
    ).items():
        layer += Node(
            arguments=[
                Option("clustering-column", column),
                Option("clustering-window", window),
            ],
            inputs=[
                Option("config-schema", filter_config.event_config_file),
                Option("dbs", triggers[(svd_bin, subtype)].files),
            ],
            outputs=Option(
                "db-to-insert-into",
                clustered_triggers.files,
            ),
        )
    return layer


def merge_and_reduce(
    condor_config, filter_config, trigger_cache, clustered_trigger_cache, column, window
):
    executable = "stillsuit-merge-reduce"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "6GB",
        "request_disk": "2GB",
    }
    layer = create_layer(
        executable,
        condor_config,
        resource_requests,
        name=executable + "-" + column.replace("_", "-"),
    )

    triggers = trigger_cache.groupby("subtype")

    # cluster triggers by SNR across time
    for (subtype), clustered_triggers in clustered_trigger_cache.groupby(
        "subtype"
    ).items():
        layer += Node(
            arguments=[
                Option("clustering-column", column),
                Option("clustering-window", window),
            ],
            inputs=[
                Option("config-schema", filter_config.event_config_file),
                Option("dbs", triggers[(subtype)].files),
            ],
            outputs=Option(
                "db-to-insert-into",
                clustered_triggers.files,
            ),
        )
    return layer


def assign_likelihood(
    condor_config,
    filter_config,
    prior_config,
    trigger_cache,
    lr_cache,
    lr_trigger_cache,
    svd_stats,
):
    executable = "sgnl-assign-likelihood"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "6GB",
        "request_disk": "3GB",
    }

    layer = create_layer(executable, condor_config, resource_requests)

    # assign likelihood to triggers
    lr_triggers = lr_trigger_cache.groupby("bin", "subtype")
    lrs = lr_cache.groupby("bin")
    for (svd_bin, subtype), triggers in trigger_cache.groupby("bin", "subtype").items():

        # find path relative to current directory
        # where assigned triggers will reside
        # split_dirname = dirname.split(os.sep)
        # dir_idx = split_dirname.index("triggers")
        # calc_dirname = os.path.join(config.data.rank_dir, *split_dirname[dir_idx:])

        arguments = [
            Option("force"),
            Option("tmp-space", condor_scratch_space()),
        ]

        # allow impossible candidates for inj jobs
        # if config.rank.allow_impossible_inj_candidates:
        #    if inj_type:
        #        arguments.append(Option("allow-impossible-candidates", "True"))
        #    else:
        #        arguments.append(Option("allow-impossible-candidates", "False"))

        layer += Node(
            arguments=arguments,
            inputs=[
                Option("config-schema", filter_config.event_config_file),
                Option("input-database-file", triggers.files),
                Option("input-likelihood-file", lrs[svd_bin].files),
                *add_likelihood_ratio_file_options(
                    svd_bin, svd_stats, prior_config, transfer_only=True
                ),
            ],
            outputs=Option(
                "output-database-file",
                lr_triggers[(svd_bin, subtype)].files,
            ),
        )

    return layer


def calc_pdf(
    condor_config, rank_config, config_svd_bins, lr_cache, pdf_cache, mass_model_file
):
    # FIXME: expose this in configuration
    num_cores = rank_config.calc_pdf_cores if rank_config.calc_pdf_cores else 1

    executable = "strike-calc-rank-pdfs"
    resource_requests = {
        "request_cpus": num_cores,
        "request_memory": "3GB",
        "request_disk": "3GB",
    }

    layer = create_layer(executable, condor_config, resource_requests)

    lrs = lr_cache.groupby("bin")
    arguments = [
        Option("num-samples", rank_config.ranking_stat_samples),
        Option("num-cores", num_cores),
    ]
    for svd_bin, pdfs in pdf_cache.groupby("bin").items():
        for pdf in pdfs.files:
            layer += Node(
                arguments=arguments,
                inputs=[
                    Option("input-likelihood-file", lrs[svd_bin].files),
                ],
                outputs=Option("output-rankingstatpdf-file", pdf),
            )

    return layer


def extinct_bin(
    condor_config, event_config_file, pdf_cache, trigger_cache, extinct_cache
):
    executable = "sgnl-extinct-bin"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "4GB",
        "request_disk": "1GB",
    }

    layer = create_layer(executable, condor_config, resource_requests)

    trigger_cache = trigger_cache.groupby("subtype")[""]  # noninj triggers
    trigger_cache = trigger_cache.groupby("bin")
    extinct_pdfs = extinct_cache.groupby("bin")
    for svd_bin, pdfs in pdf_cache.groupby("bin").items():
        assert (
            len(trigger_cache[svd_bin].files) == 1
        ), "Must provide exactly one trigger file per bin to the extinct_bin layer"
        trigger_file = trigger_cache[svd_bin].files[0]

        layer += Node(
            arguments=[Option("reset-zerolag")],
            inputs=[
                Option("config-schema", event_config_file),
                Option("input-database-file", trigger_file),
                Option("input-rankingstatpdf-file", pdfs.files),
            ],
            outputs=Option("output-rankingstatpdf-file", extinct_pdfs[svd_bin].files),
        )

    return layer


def marginalize_pdf(
    condor_config, rank_config, rank_dir, all_ifos, span, pdf_cache, marg_pdf_cache
):
    executable = "strike-marginalize-likelihood"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
        "request_disk": "1GB",
    }
    round1_layer = create_layer(
        executable, condor_config, resource_requests, name=executable + "-pdf-round-one"
    )

    num_files = (
        rank_config.marg_pdf_files if rank_config.marg_pdf_files else DEFAULT_MAX_FILES
    )

    # if number of bins is large, combine in two stages instead
    if len(pdf_cache.files) > num_files:
        partial_pdf_files = []
        for pdf_subset in pdf_cache.chunked(num_files):

            # determine bin range and determine partial file name
            svd_bins = list(pdf_subset.groupby("bin").keys())
            min_bin, max_bin = min(svd_bins), max(svd_bins)
            partial_pdf_filename = pdf_subset.name.filename(
                all_ifos,
                span,
                svd_bin=f"{min_bin}_{max_bin}",
            )
            partial_pdf_path = os.path.join(
                pdf_subset.name.directory(root=rank_dir, start=span[0]),
                partial_pdf_filename,
            )

            partial_pdf_files.append(partial_pdf_path)

            # combine across subset of bins (stage 1)
            round1_layer += Node(
                arguments=Option("marginalize", "ranking-stat-pdf"),
                inputs=Option("input", pdf_subset.files),
                outputs=Option("output", partial_pdf_path),
            )

        executable = "strike-marginalize-likelihood"
        resource_requests = {
            "request_cpus": 1,
            "request_memory": "2GB",
            "request_disk": "1GB",
        }
        round2_layer = create_layer(
            executable,
            condor_config,
            resource_requests,
            name=executable + "-pdf-round-two",
        )

        # combine across all bins (stage 2)
        round2_layer += Node(
            arguments=Option("marginalize", "ranking-stat-pdf"),
            inputs=Option("input", partial_pdf_files),
            outputs=Option("output", marg_pdf_cache.files),
        )
        layers = [round1_layer, round2_layer]
    else:
        round1_layer += Node(
            arguments=Option("marginalize", "ranking-stat-pdf"),
            inputs=Option("input", pdf_cache.files),
            outputs=Option("output", marg_pdf_cache.files),
        )
        layers = [round1_layer]

    return layers


def assign_far(
    condor_config,
    event_config_file,
    trigger_cache,
    marg_pdf_cache,
    post_pdf_cache,
    far_trigger_cache,
):
    executable = "sgnl-extinct-bin"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "6GB",
        "request_disk": "4GB",
    }

    extinct_layer = create_layer(
        executable, condor_config, resource_requests, name=executable + "-round-two"
    )

    executable = "sgnl-assign-far"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
        "request_disk": "5GB",
    }
    assign_far_layer = create_layer(executable, condor_config, resource_requests)

    far_triggers = far_trigger_cache.groupby("subtype")

    for subtype, triggers in trigger_cache.groupby("subtype").items():
        if subtype == "":
            extinct_layer += Node(
                inputs=[
                    Option("config-schema", event_config_file),
                    Option("input-database-file", triggers.files),
                    Option("input-rankingstatpdf-file", marg_pdf_cache.files),
                ],
                outputs=[Option("output-rankingstatpdf-file", post_pdf_cache.files)],
            )
        inputs = [
            Option("config-schema", event_config_file),
            Option("input-database-file", triggers.files),
            Option("input-rankingstatpdf-file", post_pdf_cache.files),
        ]
        assign_far_layer += Node(
            arguments=[
                Option("force"),
                Option("tmp-space", condor_scratch_space()),
            ],
            inputs=inputs,
            outputs=[
                Option("output-database-file", far_triggers[subtype].files),
            ],
        )

    return [extinct_layer, assign_far_layer]


def summary_page(
    condor_config,
    event_config_file,
    segments_file,
    segments_name,
    webdir,
    far_trigger_cache,
    seg_far_trigger_cache,
    post_pdf_cache,
):
    executable = "sgnl-add-segments"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "6GB",
        "request_disk": "4GB",
    }
    seg_layer = create_layer(executable, condor_config, resource_requests)

    executable = "sgnl-sim-page"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "6GB",
        "request_disk": "10GB",
    }

    sim_layer = create_layer(executable, condor_config, resource_requests)

    executable = "sgnl-results-page"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "6GB",
        "request_disk": "10GB",
    }

    results_layer = create_layer(executable, condor_config, resource_requests)

    far_triggers = far_trigger_cache.groupby("subtype")

    for subtype, seg_triggers in seg_far_trigger_cache.groupby("subtype").items():
        seg_layer += Node(
            arguments=Option("segments-name", segments_name),
            inputs=[
                Option("config-schema", event_config_file),
                Option("input-database-file", far_triggers[subtype].files),
                Option("segments-file", segments_file),
            ],
            outputs=Option("output-database-file", seg_triggers.files),
        )
        if subtype == "":
            results_layer += Node(
                inputs=[
                    Option("config-schema", event_config_file),
                    Option("input-db", seg_triggers.files),
                    Option("input-rank-stat-pdf", post_pdf_cache.files),
                ],
                outputs=Option("output-html", webdir + "/sgnl-results-page.html"),
            )
        else:
            sim_layer += Node(
                inputs=[
                    Option("config-schema", event_config_file),
                    Option("input-db", seg_triggers.files),
                ],
                outputs=Option(
                    "output-html", webdir + "/sgnl-sim-page-" + subtype + ".html"
                ),
            )

    return [seg_layer, sim_layer, results_layer]


def filter_online(
    psd_config,
    filter_config,
    upload_config,
    services_config,
    source_config,
    condor_config,
    ref_psd_cache,
    svd_bank_cache,
    lr_cache,
    svd_stats,
    zerolag_pdf_cache,
    marg_pdf_cache,
    ifos,
    tag,
    min_instruments,
):
    executable = "sgnl-inspiral"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "5GB",
        "request_disk": "2GB",
    }
    # Set torch-device
    if filter_config.torch_device:
        torch_device = filter_config.torch_device
    else:
        torch_device = "cpu"

    if "cuda" in torch_device:
        resource_requests["request_gpus"] = 1

    layer = create_layer(executable, condor_config, resource_requests, retries=1000)

    assert source_config.data_source == "devshm"

    # set up common options
    common_opts = [
        Option("data-source", "devshm"),
        Option("source-queue-timeout", source_config.source_queue_timeout),
        Option(
            "shared-memory-dir", format_ifo_args(ifos, source_config.shared_memory_dir)
        ),
        Option("track-psd"),
        Option("psd-fft-length", psd_config.fft_length),
        Option("channel-name", format_ifo_args(ifos, source_config.channel_name)),
        Option(
            "state-channel-name",
            format_ifo_args(ifos, source_config.state_channel_name),
        ),
        Option(
            "state-vector-on-bits",
            format_ifo_args(ifos, source_config.state_vector_on_bits),
        ),
        # Option("tmp-space", dagutil.condor_scratch_space()),
        Option("coincidence-threshold", filter_config.coincidence_threshold),
        Option("analysis-tag", tag),
        Option("far-trials-factor", upload_config.far_trials_factor),
        Option("gracedb-far-threshold", upload_config.gracedb_far_threshold),
        Option(
            "gracedb-aggregator-far-threshold", upload_config.aggregator_far_threshold
        ),
        Option(
            "gracedb-aggregator-far-trials-factor",
            upload_config.aggregator_far_trials_factor,
        ),
        Option("gracedb-group", upload_config.gracedb_group),
        Option("gracedb-search", upload_config.gracedb_search),
        # Option("snr-min", filter_config.snr_min),
    ]

    if filter_config.snapshot_multiprocess:
        common_opts.append(Option("snapshot-multiprocess"))
    if filter_config.snapshot_interval:
        common_opts.append(Option("snapshot-interval", filter_config.snapshot_interval))
    # if filter_config.snapshot_delay:
    #     common_opts.append(Option("snapshot-delay", filter_config.snapshot_delay))

    common_opts.append(Option("min-instruments-candidates", min_instruments))

    if filter_config.all_triggers_to_background:
        common_opts.append(Option("all-triggers-to-background"))

    # Set torch-dtype
    if filter_config.torch_dtype:
        torch_dtype = filter_config.torch_dtype
    else:
        torch_dtype = "float32"
    common_opts.append(Option("torch-dtype", torch_dtype))

    common_opts.append(Option("torch-device", torch_device))

    # Set trigger-finding-duration
    if filter_config.trigger_finding_duration:
        trigger_finding_duration = filter_config.trigger_finding_duration
    else:
        trigger_finding_duration = 1
    common_opts.append(Option("trigger-finding-duration", trigger_finding_duration))

    if services_config.kafka_server:
        common_opts.append(Option("output-kafka-server", services_config.kafka_server))

    if filter_config.cap_singles:
        common_opts.append(Option("cap-singles"))

    if filter_config.verbose:
        common_opts.extend([Option("verbose")])

    # compress ranking stat if requested
    if filter_config.compress_likelihood_ratio:
        common_opts.extend(
            [
                Option("compress-likelihood-ratio"),
                Option(
                    "compress-likelihood-ratio-threshold",
                    filter_config.compress_likelihood_ratio_threshold,
                ),
            ]
        )

    zerolag_pdfs = zerolag_pdf_cache.groupby("bin")

    group_svd_num = None
    if filter_config.group_svd_num:
        group_svd_num = filter_config.group_svd_num
        lr_groups = [lr_group for lr_group in lr_cache.chunked(group_svd_num)]
    elif filter_config.dynamic_group:
        lr_groups = []
        i = 0
        for num in filter_config.dynamic_group.split(","):
            num = int(num)
            lr_groups.append(util.DataCache(lr_cache.name, lr_cache.cache[i : i + num]))
            i = i + num
    else:
        group_svd_num = 1
        lr_groups = [lr_group for lr_group in lr_cache.chunked(group_svd_num)]

    svd_banks = svd_bank_cache.groupby("ifo", "bin")
    for lr_group in lr_groups:
        svd_bins = list(lr_group.groupby("bin").keys())
        if group_svd_num == 1:
            job_tag = f"{int(svd_bins[0]):04d}_noninj"
        else:
            job_tag = f"{int(svd_bins[0]):04d}_{int(svd_bins[-1]):04d}_noninj"

        thresholds = [filter_config.ht_gate_threshold for _ in svd_bins]
        filter_opts = [
            Option("job-tag", job_tag),
            # FIXME: fix gate threshold
            # Option("ht-gate-threshold", calc_gate_threshold(config, svd_bin)),
            # Option("ht-gate-threshold", svd_stats.bins[svd_bin]["ht_gate_threshold"]),
            Option("ht-gate-threshold", thresholds),
        ]
        filter_opts.extend(common_opts)

        svd_bank_files = util.flatten(
            [svd_banks[(ifo, svd_bin)].files for ifo in ifos for svd_bin in svd_bins]
        )

        zerolag_pdf_files = [
            f for svd_bin in svd_bins for f in zerolag_pdfs[(svd_bin)].files
        ]

        inputs = [
            Option("svd-bank", svd_bank_files),
            Option("reference-psd", ref_psd_cache),
            Option("event-config", filter_config.event_config_file),
            Option("input-likelihood-file", lr_group.files),
            Option("rank-stat-pdf", marg_pdf_cache.files),
        ]
        outputs = [
            Option("output-likelihood-file", lr_group.files),
            Option("zerolag-rank-stat-pdf", zerolag_pdf_files),
        ]

        layer += Node(
            arguments=filter_opts,
            inputs=inputs,
            outputs=outputs,
        )

    return layer


def injection_filter_online(
    psd_config,
    filter_config,
    upload_config,
    services_config,
    source_config,
    condor_config,
    ref_psd_cache,
    svd_bank_cache,
    lr_cache,
    svd_stats,
    marg_pdf_cache,
    ifos,
    tag,
    min_instruments,
):
    executable = "sgnl-inspiral"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "5GB",
        "request_disk": "2GB",
    }
    # Set torch-device
    if filter_config.torch_device:
        torch_device = filter_config.torch_device
    else:
        torch_device = "cpu"

    if "cuda" in torch_device:
        resource_requests["request_gpus"] = 1

    layer = create_layer(
        executable,
        condor_config,
        resource_requests,
        retries=1000,
        name=executable + "-inj",
    )

    assert source_config.data_source == "devshm"

    # set up common options
    common_opts = [
        Option("data-source", "devshm"),
        Option("source-queue-timeout", source_config.source_queue_timeout),
        Option(
            "shared-memory-dir",
            format_ifo_args(ifos, source_config.inj_shared_memory_dir),
        ),
        Option("track-psd"),
        Option("psd-fft-length", psd_config.fft_length),
        Option("channel-name", format_ifo_args(ifos, source_config.inj_channel_name)),
        Option(
            "state-channel-name",
            format_ifo_args(ifos, source_config.state_channel_name),
        ),
        Option(
            "state-vector-on-bits",
            format_ifo_args(ifos, source_config.state_vector_on_bits),
        ),
        # Option("tmp-space", dagutil.condor_scratch_space()),
        Option("coincidence-threshold", filter_config.coincidence_threshold),
        Option("analysis-tag", tag),
        Option("far-trials-factor", upload_config.far_trials_factor),
        Option("gracedb-far-threshold", upload_config.gracedb_far_threshold),
        Option(
            "gracedb-aggregator-far-threshold", upload_config.aggregator_far_threshold
        ),
        Option(
            "gracedb-aggregator-far-trials-factor",
            upload_config.aggregator_far_trials_factor,
        ),
        Option("gracedb-group", upload_config.gracedb_group),
        Option("gracedb-search", upload_config.inj_gracedb_search),
        Option("injections"),
        # Option("snr-min", filter_config.snr_min),
    ]

    if filter_config.snapshot_multiprocess:
        common_opts.append(Option("snapshot-multiprocess"))
    if filter_config.snapshot_interval:
        common_opts.append(Option("snapshot-interval", filter_config.snapshot_interval))
    # if filter_config.snapshot_delay:
    #     common_opts.append(Option("snapshot-delay", filter_config.snapshot_delay))

    common_opts.append(Option("min-instruments-candidates", min_instruments))

    # Set torch-dtype
    if filter_config.torch_dtype:
        torch_dtype = filter_config.torch_dtype
    else:
        torch_dtype = "float32"
    common_opts.append(Option("torch-dtype", torch_dtype))

    common_opts.append(Option("torch-device", torch_device))

    # Set trigger-finding-duration
    if filter_config.trigger_finding_duration:
        trigger_finding_duration = filter_config.trigger_finding_duration
    else:
        trigger_finding_duration = 1
    common_opts.append(Option("trigger-finding-duration", trigger_finding_duration))

    if services_config.kafka_server:
        common_opts.append(Option("output-kafka-server", services_config.kafka_server))

    if filter_config.cap_singles:
        common_opts.append(Option("cap-singles"))

    if filter_config.verbose:
        common_opts.extend([Option("verbose")])

    # compress ranking stat if requested
    if filter_config.compress_likelihood_ratio:
        common_opts.extend(
            [
                Option("compress-likelihood-ratio"),
                Option(
                    "compress-likelihood-ratio-threshold",
                    filter_config.compress_likelihood_ratio_threshold,
                ),
            ]
        )

    group_svd_num = None
    if filter_config.group_svd_num:
        group_svd_num = filter_config.group_svd_num
        lr_groups = [lr_group for lr_group in lr_cache.chunked(group_svd_num)]
    elif filter_config.dynamic_group:
        lr_groups = []
        i = 0
        for num in filter_config.dynamic_group.split(","):
            num = int(num)
            lr_groups.append(util.DataCache(lr_cache.name, lr_cache.cache[i : i + num]))
            i = i + num
    else:
        group_svd_num = 1
        lr_groups = [lr_group for lr_group in lr_cache.chunked(group_svd_num)]

    svd_banks = svd_bank_cache.groupby("ifo", "bin")
    for lr_group in lr_groups:
        svd_bins = list(lr_group.groupby("bin").keys())
        if group_svd_num == 1:
            job_tag = f"{int(svd_bins[0]):04d}_inj"
        else:
            job_tag = f"{int(svd_bins[0]):04d}_{int(svd_bins[-1]):04d}_inj"

        thresholds = [filter_config.ht_gate_threshold for _ in svd_bins]
        filter_opts = [
            Option("job-tag", job_tag),
            # FIXME: fix gate threshold
            # Option("ht-gate-threshold", calc_gate_threshold(config, svd_bin)),
            # Option("ht-gate-threshold", svd_stats.bins[svd_bin]["ht_gate_threshold"]),
            Option("ht-gate-threshold", thresholds),
        ]
        filter_opts.extend(common_opts)

        svd_bank_files = util.flatten(
            [svd_banks[(ifo, svd_bin)].files for ifo in ifos for svd_bin in svd_bins]
        )

        inputs = [
            Option("svd-bank", svd_bank_files),
            Option("reference-psd", ref_psd_cache),
            Option("event-config", filter_config.event_config_file),
            Option("input-likelihood-file", lr_group.files),
            Option("rank-stat-pdf", marg_pdf_cache.files),
        ]
        layer += Node(
            arguments=filter_opts,
            inputs=inputs,
        )

    return layer


def marginalize_online(
    condor_config,
    filter_config,
    services_config,
    lr_cache,
    tag,
    marg_pdf_cache,
    extinct_percent=None,
    fast_burnin=False,
    calc_pdf_cores=1,
):
    num_cores = calc_pdf_cores if calc_pdf_cores else 1

    executable = "sgnl-ll-marginalize-likelihoods-online"
    resource_requests = {
        "request_cpus": num_cores,
        "request_memory": "4GB",
        "request_disk": "5GB",
    }
    layer = create_layer(executable, condor_config, resource_requests, retries=1000)

    group_svd_num = None
    if filter_config.group_svd_num:
        group_svd_num = filter_config.group_svd_num
        lr_groups = [lr_group for lr_group in lr_cache.chunked(group_svd_num)]
    elif filter_config.dynamic_group:
        lr_groups = []
        i = 0
        for num in filter_config.dynamic_group.split(","):
            num = int(num)
            lr_groups.append(util.DataCache(lr_cache.name, lr_cache.cache[i : i + num]))
            i = i + num
    else:
        group_svd_num = 1
        lr_groups = [lr_group for lr_group in lr_cache.chunked(group_svd_num)]

    svd_bin_groups = [list(lr_group.groupby("bin").keys()) for lr_group in lr_groups]

    if group_svd_num == 1:
        registries = list(
            f"{int(svd_bins[0]):04d}_noninj_registry.txt" for svd_bins in svd_bin_groups
        )
    else:
        registries = list(
            f"{int(svd_bins[0]):04d}_{int(svd_bins[-1]):04d}_noninj_registry.txt"
            for svd_bins in svd_bin_groups
        )
    arguments = [
        Option("registry", registries),
        Option("output", list(marg_pdf_cache.files)),
        Option("output-kafka-server", services_config.kafka_server),
        Option("tag", tag),
        Option("verbose"),
        Option("num-cores", num_cores),
    ]

    if fast_burnin:
        arguments.append(Option("fast-burnin"))

    if extinct_percent:
        arguments.append(Option("extinct-percent", extinct_percent))

    layer += Node(arguments=arguments)

    return layer


def track_noise(
    condor_config,
    source_config,
    filter_config,
    psd_config,
    metrics_config,
    services_config,
    ifos,
    tag,
    ref_psd,
    injection=False,
):
    executable = "sgnl-ll-dq"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
        "request_disk": "1GB",
    }
    if injection is True:
        name = executable + "-inj"
    else:
        name = executable

    layer = create_layer(
        executable, condor_config, resource_requests, retries=1000, name=name
    )

    assert source_config.data_source == "devshm"
    for ifo in ifos:
        if injection is True:
            channel_names = [source_config.inj_channel_name[ifo]]
        else:
            channel_names = [source_config.channel_name[ifo]]
        for channel in channel_names:
            # set up datasource options
            if injection is True:
                memory_location = source_config.inj_shared_memory_dir[ifo]
            else:
                memory_location = source_config.shared_memory_dir[ifo]
            arguments = [
                Option("data-source", "devshm"),
                Option("shared-memory-dir", f"{ifo}={memory_location}"),
            ]
            if injection is True:
                arguments.append(Option("injections"))

            arguments.extend(
                [
                    Option("analysis-tag", tag),
                    Option("psd-fft-length", psd_config.fft_length),
                    Option("channel-name", f"{ifo}={channel}"),
                    Option(
                        "state-channel-name",
                        f"{ifo}={source_config.state_channel_name[ifo]}",
                    ),
                    Option(
                        "state-vector-on-bits",
                        f"{ifo}={source_config.state_vector_on_bits[ifo]}",
                    ),
                    Option("reference-psd", ref_psd),
                ]
            )

            if services_config.kafka_server:
                arguments.extend(
                    [Option("output-kafka-server", services_config.kafka_server)]
                )

            layer += Node(arguments=arguments)

    return layer


def count_events(condor_config, services_config, upload_config, tag, zerolag_pdf):
    executable = "sgnl-ll-trigger-counter"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
        "request_disk": "1GB",
    }
    layer = create_layer(executable, condor_config, resource_requests, retries=1000)

    layer += Node(
        arguments=[
            Option("kafka-server", services_config.kafka_server),
            Option("output-period", 300),
            Option("topic", "coinc"),
            Option("tag", tag),
        ],
        outputs=Option("output", zerolag_pdf.files),
    )

    return layer


def upload_events(
    condor_config, upload_config, services_config, metrics_config, svd_bins, tag
):
    executable = "sgnl-ll-inspiral-event-uploader"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
        "request_disk": "1GB",
    }
    layer = create_layer(executable, condor_config, resource_requests, retries=1000)

    input_topics = (
        ["events", "inj_events"]
        if upload_config.enable_injection_uploads
        else ["events"]
    )

    for input_topic in input_topics:
        arguments = [
            Option("kafka-server", services_config.kafka_server),
            Option("gracedb-group", upload_config.gracedb_group),
            Option("gracedb-pipeline", upload_config.gracedb_pipeline),
            Option("far-threshold", upload_config.aggregator_far_threshold),
            Option("far-trials-factor", upload_config.aggregator_far_trials_factor),
            Option("upload-cadence-type", upload_config.aggregator_cadence_type),
            Option("upload-cadence-factor", upload_config.aggregator_cadence_factor),
            Option("num-jobs", len(svd_bins)),
            Option("tag", tag),
            Option("input-topic", input_topic),
            Option("verbose"),
            Option("scald-config", metrics_config.scald_config),
            Option("max-partitions", 10),
        ]

        # add gracedb service url
        if "inj_" in input_topic:
            arguments.append(
                Option("gracedb-service-url", upload_config.inj_gracedb_service_url)
            )
            arguments.append(Option("gracedb-search", upload_config.inj_gracedb_search))
        else:
            arguments.append(
                Option("gracedb-service-url", upload_config.gracedb_service_url)
            )
            arguments.append(Option("gracedb-search", upload_config.gracedb_search))

        layer += Node(arguments=arguments)

    return layer


def plot_events(condor_config, upload_config, services_config, tag):

    executable = "sgnl-ll-inspiral-event-plotter"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "3GB",
        "request_disk": "1GB",
    }
    layer = create_layer(executable, condor_config, resource_requests, retries=1000)

    upload_topics = (
        ["uploads", "inj_uploads"]
        if upload_config.enable_injection_uploads
        else ["uploads"]
    )
    ranking_stat_topics = (
        ["ranking_stat", "inj_ranking_stat"]
        if upload_config.enable_injection_uploads
        else ["ranking_stat"]
    )

    to_upload = (
        "RANKING_DATA",
        "RANKING_PLOTS",
        "SNR_PLOTS",
        "PSD_PLOTS",
        "DTDPHI_PLOTS",
    )

    for upload_topic, ranking_stat_topic in zip(upload_topics, ranking_stat_topics):
        for upload in to_upload:
            arguments = [
                Option("kafka-server", services_config.kafka_server),
                Option("upload-topic", upload_topic),
                Option("ranking-stat-topic", ranking_stat_topic),
                Option("tag", tag),
                Option("plot", upload),
                Option("verbose"),
            ]

            # add gracedb service url
            if "inj_" in upload_topic:
                arguments.append(
                    Option("gracedb-service-url", upload_config.inj_gracedb_service_url)
                )
            else:
                arguments.append(
                    Option("gracedb-service-url", upload_config.gracedb_service_url)
                )

            layer += Node(arguments=arguments)

    return layer


def collect_metrics(
    dag,
    condor_config,
    metrics_config,
    services_config,
    filter_config,
    tag,
    ifos,
    svd_bins,
):
    executable = "scald"

    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
        "request_disk": "1GB",
    }
    metric_leader_layer = create_layer(
        executable,
        condor_config,
        resource_requests,
        retries=1000,
        name="scald_event_collector",
    )

    # FIXME when is this layer used?
    # metric_layer = create_layer(executable, condor_config, resource_requests,
    # retries=1000, name="scald_event_collector")

    # set up common options
    common_opts = [
        Argument("command", "aggregate"),
        Option("config", metrics_config.scald_config),
        Option("uri", f"kafka://{tag}-collect@{services_config.kafka_server}"),
    ]

    # set topic_prefix to distinguish inj and noninj topics
    topic_prefix = ["", "inj_"] if filter_config.injections else [""]

    # define metrics used for aggregation jobs
    snr_metrics = [
        f"{prefix}{ifo}_snr_history" for ifo in ifos for prefix in topic_prefix
    ]
    range_metrics = [f"{prefix}range_history" for prefix in topic_prefix]
    network_metrics = []
    for prefix, topic in list(
        itertools.product(
            topic_prefix,
            ("likelihood_history", "snr_history", "latency_history", "far_history"),
        )
    ):
        network_metrics.append(f"{prefix}{topic}")

    heartbeat_metrics = []
    for prefix, topic in list(
        itertools.product(
            topic_prefix,
            (
                "uptime",
                "event_uploader_heartbeat",
                "event_plotter_heartbeat",
                "pastro_uploader_heartbeat",
            ),
        )
    ):
        heartbeat_metrics.append(f"{prefix}{topic}")
    heartbeat_metrics.append("marginalize_likelihoods_online_heartbeat")
    heartbeat_metrics.append("trigger_counter_heartbeat")

    # state_metrics = [
    #     f"{prefix}{ifo}_strain_dropped" for ifo in ifos for prefix in topic_prefix
    # ]  # FIXME do we need this?
    usage_metrics = [f"{prefix}ram_history" for prefix in topic_prefix]

    latency_metrics = [
        f"{prefix}{ifo}_{stage}_latency"
        for ifo in ifos
        for stage in ("datasource", "whitening", "snrSlice")
        for prefix in topic_prefix
    ]
    latency_metrics.extend([f"{prefix}all_itacacac_latency" for prefix in topic_prefix])

    agg_metrics = list(
        itertools.chain(
            snr_metrics,
            range_metrics,
            network_metrics,
            usage_metrics,
            # state_metrics,
            latency_metrics,
            heartbeat_metrics,
        )
    )

    gates = [
        # f"{gate}segments" for gate in ("statevector", "dqvector", "whiteht", "idq")
        f"{gate}segments"
        for gate in ("statevector", "afterhtgate")
    ]
    seg_metrics = [
        f"{prefix}{ifo}_{gate}"
        for ifo in ifos
        for gate in gates
        for prefix in topic_prefix
    ]

    # set up partitioning
    # FIXME don't hard code the 1000
    max_agg_jobs = 1000

    if filter_config.group_svd_num:
        num_jobs = ceil(len(svd_bins) / filter_config.group_svd_num)
    elif filter_config.dynamic_group:
        num_jobs = len(filter_config.dynamic_group.split(","))
    else:
        num_jobs = len(svd_bins)

    agg_job_bounds = list(range(0, num_jobs, max_agg_jobs))
    min_topics_per_job = 1

    # timeseries metrics
    agg_metrics = groups(
        agg_metrics, max(max_agg_jobs // (4 * num_jobs), min_topics_per_job)
    )
    seg_metrics = groups(
        seg_metrics, max(max_agg_jobs // (4 * num_jobs), min_topics_per_job)
    )

    for metrics in itertools.chain(agg_metrics, seg_metrics):
        for i, _ in enumerate(agg_job_bounds):
            # add jobs to consume each metric
            arguments = list(common_opts)
            topics = []
            schemas = []
            for metric in metrics:
                if "latency_history" in metric:
                    # for latency history we want to
                    # aggregate by max and median so
                    # we need two schemas
                    for aggfunc in ("max", "median"):
                        topics.append(f"sgnl.{tag}.{metric}")
                        schemas.append(f"{metric}_{aggfunc}")
                else:
                    topics.append(f"sgnl.{tag}.{metric}")
                    schemas.append(metric)

            arguments.extend(
                [
                    Option("data-type", "timeseries"),
                    Option("topic", topics),
                    Option("schema", schemas),
                ]
            )

            # elect first metric collector as leader
            if i == 0:
                arguments.append(Option("across-jobs"))
                metric_leader_layer += Node(arguments=arguments)
            else:
                raise ValueError("not implemented")
                # metric_layer += Node(arguments=arguments) # FIXME

    # if metric_layer.nodes:
    #    dag.attach(metric_leader_layer)
    #    dag.attach(metric_layer)
    # else:
    #    dag.attach(metric_leader_layer)
    return metric_leader_layer


def collect_metrics_event(
    condor_config,
    metrics_config,
    services_config,
    filter_config,
    tag,
):
    executable = "scald"

    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
        "request_disk": "1GB",
    }
    event_layer = create_layer(
        executable,
        condor_config,
        resource_requests,
        retries=1000,
        name="scald_event_collector_event",
    )
    # event metrics
    schemas = ["coinc", "inj_coinc"] if filter_config.injections else ["coinc"]

    common_opts = [
        Argument("command", "aggregate"),
        Option("config", metrics_config.scald_config),
        Option("uri", f"kafka://{tag}-collect@{services_config.kafka_server}"),
    ]
    for schema in schemas:
        event_arguments = list(common_opts)
        event_arguments.extend(
            [
                Option("data-type", "triggers"),
                Option("topic", f"sgnl.{tag}.{schema}"),
                Option("schema", schema),
                Option("uri", f"kafka://{tag}-collect@{services_config.kafka_server}"),
            ]
        )
        event_layer += Node(arguments=event_arguments)

    return event_layer


def upload_pastro(
    condor_config, services_config, upload_config, pastro_config, tag, marg_pdf_cache
):

    executable = "sgnl-ll-pastro-uploader"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "3GB",
        "request_disk": "2GB",
    }

    layer = create_layer(
        executable,
        condor_config,
        resource_requests,
        retries=1000,
    )

    input_topics = (
        ["uploads", "inj_uploads"]
        if upload_config.enable_injection_uploads
        else ["uploads"]
    )

    for model, options in pastro_config.items():
        # make a copy of the model file for injection
        #  jobs to avoid issues with IO
        # FIXME this is so hacky
        if upload_config.enable_injection_uploads:
            path, filename = options.mass_model.split("/")
            inj_mass_model = os.path.join(path, "inj_" + filename)
            shutil.copy(options.mass_model, inj_mass_model)
        for input_topic in input_topics:
            arguments = [
                Option("kafka-server", services_config.kafka_server),
                Option("tag", tag),
                Option("input-topic", input_topic),
                Option("model-name", model),
                Option("pastro-filename", options.upload_file),
                Option(
                    "pastro-model-file",
                    options.mass_model if input_topic == "uploads" else inj_mass_model,
                ),
                Option("rank-stat", marg_pdf_cache.files),
                Option("verbose"),
            ]

            # add gracedb service url
            if "inj_" in input_topic:
                arguments.append(
                    Option("gracedb-service-url", upload_config.inj_gracedb_service_url)
                )
            else:
                arguments.append(
                    Option("gracedb-service-url", upload_config.gracedb_service_url)
                )

            layer += Node(arguments=arguments)

    return layer


def add_likelihood_ratio_file_options(
    svd_bin, svd_stats, prior_config, transfer_only=False
):
    """
    Return a list of options relating to files used for
    terms in the ranking statistic,
    including:
        * dtdphi
        * iDQ timeseries

    if transfer_only is True, do not add options to programs,
    instead, only use these files as inputs for Condor file
    transfer. This is required for jobs that require these files,
    as their paths are tracked through the ranking stat, so
    Condor needs to be aware of these files when not relying on
    a shared file system.
    """
    if transfer_only:
        kwargs = {"track": False, "suppress": True}
    else:
        kwargs = {}

    inputs = [Option("mass-model-file", prior_config.mass_model, **kwargs)]

    if prior_config.idq_timeseries:
        inputs.append(Option("idq-file", prior_config.idq_timeseries, **kwargs))

    if prior_config.dtdphi:
        if isinstance(prior_config.dtdphi, Mapping):
            sub_bank = svd_stats.bins[svd_bin]["bank_name"]
            inputs.append(
                Option("dtdphi-file", prior_config.dtdphi[sub_bank], **kwargs)
            )
        else:
            inputs.append(Option("dtdphi-file", prior_config.dtdphi, **kwargs))

    return inputs


def autocorrelation_length_map(ac_length_range):
    """
    Given autocorrelation length ranges (e.g. 0:15:701)
    or a single autocorrelation value, returns a function that
    maps a given chirp mass to an autocorrelation length.
    """
    if isinstance(ac_length_range, str):
        ac_length_range = [ac_length_range]

    # handle case with AC length ranges
    if isinstance(ac_length_range, Iterable):
        ac_lengths = []
        min_mchirps = []
        max_mchirps = []
        for this_range in ac_length_range:
            min_mchirp, max_mchirp, ac_length = this_range.split(":")
            min_mchirps.append(float(min_mchirp))
            max_mchirps.append(float(max_mchirp))
            ac_lengths.append(int(ac_length))

        # sanity check inputs
        for bound1, bound2 in zip(min_mchirps[1:], max_mchirps[:-1]):
            assert bound1 == bound2, "gaps not allowed in autocorrelation length ranges"

        # convert to binning
        bins = rate.IrregularBins([min_mchirps[0]] + max_mchirps)

    # handle single value case
    else:
        ac_lengths = [ac_length_range]
        bins = rate.IrregularBins([0.0, numpy.inf])

    # create mapping
    def mchirp_to_ac_length(mchirp):
        idx = bins[mchirp]
        return ac_lengths[idx]

    return mchirp_to_ac_length

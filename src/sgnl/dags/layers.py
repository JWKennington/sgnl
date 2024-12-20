# Copyright (C) 2020  Patrick Godwin (patrick.godwin@ligo.org)
# Copyright (C) 2024  Cort Posnansky (cort.posnansky@ligo.org)
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	See the GNU General
# Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.


from ezdag import Argument, Layer, Node, Option

from sgnl.dags import util
from sgnl.dags.util import format_ifo_args, to_ifo_list

# from gstlal import plugins
# from gstlal.datafind import DataType, DataCache
# from gstlal.dags import Argument, Option
# from gstlal.dags import util as dagutil
# from gstlal.dags.layers import Layer, Node


def create_layer(executable, condor_config, resource_requests, name=None):
    # Default submit file options
    submit_description = create_submit_description(condor_config)

    # Add resource requests
    submit_description.update(resource_requests)

    # Allow arbitrary submit file options from the config
    if executable in condor_config:
        submit_description.update(condor_config[executable])

    # Set file transfer
    if condor_config.transfer_files:
        transfer_files = condor_config.transfer_files
    else:
        transfer_files = True

    return Layer(
        executable,
        name=name if name else executable,
        transfer_files=transfer_files,
        submit_description=submit_description,
    )


def create_submit_description(condor_config):
    submit_description = {
        "want_graceful_removal": "True",
        "kill_sig": "15",
        "accounting_group": condor_config.accounting_group,
        "accounting_group_user": condor_config.accounting_group_user,
    }
    requirements = []
    environment = {}

    # Container options
    if "container" in condor_config:
        # singularity_image = condor_config["container"]
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


def reference_psd(psd_config, condor_config, psd_cache):
    pass


def median_psd(psd_config, condor_config, psd_cache):
    pass


def filter(
    psd_config,
    svd_config,
    filter_config,
    source_config,
    condor_config,
    ref_psd_cache,
    svd_bank_cache,
    dist_stat_cache,
    trigger_cache,
    svd_stats,
):
    executable = "sgnl-inspiral"
    resource_requests = {
        "request_cpus": 2,
        "request_memory": "4GB",
        "request_disk": "5GB",
    }
    layer = create_layer(executable, condor_config, resource_requests)

    common_opts = [
        Option("track-psd"),
        Option("data-source", "frames"),
        Option("psd-fft-length", psd_config.fft_length),
        Option("frame-segments-name", source_config.frame_segments_name),
        Option("coincidence-threshold", filter_config.coincidence_threshold),
    ]

    common_inputs = [
        Option("event-config", filter_config.event_config_file),
    ]

    # Set torch-dtype
    if filter_config.torch_dtype:
        torch_dtype = filter_config.torch_dtype
    else:
        torch_dtype = "float32"
    common_opts.append(Option("torch-dtype", torch_dtype))

    # Set torch-device
    if filter_config.torch_device:
        torch_device = filter_config.torch_device
    else:
        torch_device = "cpu"
    common_opts.append(Option("torch-device", torch_device))

    # Set trigger-finding-duration
    if filter_config.trigger_finding_duration:
        trigger_finding_duration = filter_config.trigger_finding_duration
    else:
        trigger_finding_duration = 1
    common_opts.append(Option("trigger-finding-duration", trigger_finding_duration))

    # Checkpoint by grouping SVD bins together
    if filter_config.group_svd:
        max_concurrency = 10
        num_per_group = min(1 + len(svd_config.bins) // 20, max_concurrency)
        if num_per_group > 1:
            common_opts.append(Option("local-frame-caching"))
    else:
        num_per_group = 1

    ref_psds = ref_psd_cache.groupby("ifo", "time")
    svd_banks = svd_bank_cache.groupby("ifo", "bin")
    dist_stats = dist_stat_cache.groupby("ifo", "time", "bin")
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

        for trigger_group in triggers.chunked(num_per_group):
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
            dist_stat_files = util.flatten(
                [dist_stats[(ifo_combo, span, svd_bin)].files for svd_bin in svd_bins]
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
                    Option("output-likelihood-file", dist_stat_files),
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
):
    executable = "sgnl-inspiral"
    resource_requests = {
        "request_cpus": 2,
        "request_memory": "4GB",
        "request_disk": "5GB",
    }
    layer = create_layer(
        executable, condor_config, resource_requests, name="sgnl-inspiral-inj"
    )

    common_opts = [
        Option("track-psd"),
        Option("data-source", "frames"),
        Option("psd-fft-length", psd_config.fft_length),
        Option("frame-segments-name", source_config.frame_segments_name),
        Option("coincidence-threshold", filter_config.coincidence_threshold),
        Option("injections"),
    ]

    common_inputs = [
        Option("event-config", filter_config.event_config_file),
    ]

    # Set torch-dtype
    if filter_config.torch_dtype:
        torch_dtype = filter_config.torch_dtype
    else:
        torch_dtype = "float32"
    common_opts.append(Option("torch-dtype", torch_dtype))

    # Set torch-device
    if filter_config.torch_device:
        torch_device = filter_config.torch_device
    else:
        torch_device = "cpu"
    common_opts.append(Option("torch-device", torch_device))

    # Set trigger-finding-duration
    if filter_config.trigger_finding_duration:
        trigger_finding_duration = filter_config.trigger_finding_duration
    else:
        trigger_finding_duration = 1
    common_opts.append(Option("trigger-finding-duration", trigger_finding_duration))

    # Checkpoint by grouping SVD bins together
    if filter_config.group_svd:
        max_concurrency = 10
        num_per_group = min(1 + len(svd_config.bins) // 20, max_concurrency)
        if num_per_group > 1:
            common_opts.append(Option("local-frame-caching"))
    else:
        num_per_group = 1

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

        for trigger_group in triggers.chunked(num_per_group):
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
        "request_memory": 2000,
        "request_disk": "1GB",
    }

    layer = create_layer(executable, condor_config, resource_requests)

    # FIXME: find better way of discovering SQL file
    # share_path = os.path.split(dagutil.which("gstlal_inspiral"))[0
    #               ].replace("bin", "share/gstlal")
    # snr_cluster_sql_file = os.path.join(share_path, "snr_simplify_and_cluster.sql")
    # inj_snr_cluster_sql_file = os.path.join(share_path,
    #                               "inj_snr_simplify_and_cluster.sql")

    clustered_triggers = clustered_triggers_cache.groupby("ifo", "time", "bin")

    # cluster triggers by SNR
    for (ifo_combo, span, svd_bin), triggers in trigger_cache.groupby(
        "ifo", "time", "bin"
    ).items():
        layer += Node(
            arguments=[
                Option("clustering-column", "network_chisq_weighted_snr"),
                Option("clustering-window", filter_config.clustering_window),
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


def marginalize_dist_stats(
    filter_config, condor_config, dist_stat_cache, marg_dist_stat_cache
):

    executable = "strike-marginalize-likelihood"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": 2000,
        "request_disk": "1GB",
    }
    layer = create_layer(executable, condor_config, resource_requests)

    dist_stats = dist_stat_cache.groupby("bin")

    for svd_bin, marg_dist_stats in marg_dist_stat_cache.groupby("bin").items():
        layer += Node(
            arguments=Option("marginalize", "likelihood-ratio"),
            inputs=Option("input", dist_stats[svd_bin].files),
            outputs=Option("output", marg_dist_stats.files),
        )

    return layer

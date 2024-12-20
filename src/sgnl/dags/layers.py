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


import os
from ezdag import Argument, Layer, Node, Option

from sgnl.dags import util
from sgnl.dags.util import condor_scratch_space, format_ifo_args, to_ifo_list

# from gstlal import plugins
# from gstlal.datafind import DataType, DataCache
# from gstlal.dags import Argument, Option
# from gstlal.dags import util as dagutil
# from gstlal.dags.layers import Layer, Node

DEFAULT_MAX_FILES = 500


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
        "getenv": condor_config.getenv,
    }
    requirements = []
    environment = {}

    # Container options
    if "container" in condor_config:
        # singularity_image = condor_config["container"]
        # FIXME add singularity once we have a container
        # submit_description["MY.SingularityImage"] = f'"{condor_config.container}"'
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
    lr_cache,
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
        Option("snr-min", filter_config.snr_min),
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
):
    executable = "sgnl-inspiral"
    resource_requests = {
        "request_cpus": 2,
        "request_memory": "5GB",
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
        Option("snr-min", filter_config.snr_min),
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
    lr_cache,
    marg_lr_cache,
    prior_cache=None,
    mass_model_file=None,
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

    inputs = []
    if mass_model_file is not None:
        inputs.append(Option("mass-model-file", mass_model_file, suppress=True))

    for svd_bin, marg_lrs in marg_lr_cache.groupby("bin").items():
        layer += Node(
            arguments=Option("marginalize", "likelihood-ratio"),
            inputs=inputs
            + [
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
    coincidence_threshold,
    mass_model,
    svd_bank_cache,
    prior_cache,
    ifos,
    min_ifos,
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
        Option("min-instruments", min_ifos),
        Option("coincidence-threshold", coincidence_threshold),
    ]
    for svd_bin, prior in prior_cache.groupby("bin").items():
        inputs = [
            Option("svd-file", svd_banks[svd_bin].files),
            Option("mass-model-file", mass_model),
        ]
        layer += Node(
            arguments=arguments,
            inputs=inputs,
            outputs=Option("output-likelihood-file", prior.files),
        )

    return layer


def cluster_snr(
    condor_config, filter_config, trigger_cache, clustered_trigger_cache, column, window
):
    executable = "stillsuit-merge-reduce"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
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
        "request_memory": "2GB",
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
    trigger_cache,
    lr_cache,
    lr_trigger_cache,
    mass_model_file,
):
    executable = "sgnl-assign-likelihood"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
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
                Option("mass-model-file", mass_model_file, suppress=True),
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
    num_cores = rank_config.calc_pdf_cores if rank_config.calc_pdf_cores else 4

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
                    Option("mass-model-file", mass_model_file, suppress=True),
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
        "request_memory": "2GB",
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
        "request_memory": "2GB",
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
):
    executable = "sgnl-add-segments"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
        "request_disk": "4GB",
    }
    seg_layer = create_layer(executable, condor_config, resource_requests)

    executable = "sgnl-sim-page"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
        "request_disk": "10GB",
    }

    sim_layer = create_layer(executable, condor_config, resource_requests)

    executable = "sgnl-results-page"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "2GB",
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

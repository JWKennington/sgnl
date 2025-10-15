"""A module for building SGN graphs of the LLOID algorithm
(https://arxiv.org/abs/1107.2665)
"""

# Copyright (C) 2009-2013 Kipp Cannon, Chad Hanna, Drew Keppel
# Copyright (C) 2021      Patrick Godwin
# Copyright (C) 2024-2025 Yun-Jing Huang

from math import ceil

from sgnts.base import AdapterConfig, Offset
from sgnts.base.array_ops import TorchBackend
from sgnts.transforms import Adder, Amplify, Converter, Matmul, Resampler, SumIndex

from sgnl.transforms.lloid_correlate import LLOIDCorrelate

# See mkmultiband in gstlal/python/pipeparts/condition.py
# and audioresample_variance_gain in gstlal/python/pipeparts/transform.py
RESAMPLE_VARIANCE_GAIN = 0.9684700588501590213


def lloid(
    pipeline,
    sorted_bank,
    input_source_links: dict[str, str],
    nslice: int,
    device,
    dtype,
    reconstruction_segment_list=None,
):
    TorchBackend.set_device(device)
    TorchBackend.set_dtype(dtype)

    output_source_links = {}

    bank_metadata = sorted_bank.bank_metadata
    ifos = bank_metadata["ifos"]
    unique_rates = list(bank_metadata["unique_rates"].keys())
    maxrate = bank_metadata["maxrate"]
    bases = sorted_bank.bases_cat
    coeff = sorted_bank.coeff_sv_cat

    pipeline.insert(
        Converter(
            name="converter1",
            sink_pad_names=tuple(ifos),
            source_pad_names=tuple(ifos),
            adapter_config=AdapterConfig(stride=Offset.SAMPLE_STRIDE_AT_MAX_RATE),
            backend="torch",
            dtype=dtype,
            device=device,
        ),
    )

    # Multi-band
    sorted_rates = bank_metadata["sorted_rates"]
    for ifo in ifos:
        pipeline.insert(
            link_map={
                "converter1:snk:" + ifo: input_source_links[ifo],
            }
        )
        prev_source_pad = "converter1:src:" + ifo

        for i, rate in enumerate(unique_rates[:-1]):
            rate_down = unique_rates[i + 1]
            name = f"{ifo}_down_{rate_down}"
            amp_name = f"{ifo}_amplify_{rate_down}"
            sink_pad_full = amp_name + ":snk:" + ifo

            source_pad_full = name + ":src:" + ifo

            pipeline.insert(
                Amplify(
                    name=amp_name,
                    sink_pad_names=(ifo,),
                    source_pad_names=(ifo,),
                    factor=1 / (RESAMPLE_VARIANCE_GAIN * rate_down / rate) ** 0.5,
                ),
                Resampler(
                    name=name,
                    sink_pad_names=(ifo,),
                    source_pad_names=(ifo,),
                    backend=TorchBackend,
                    inrate=rate,
                    outrate=rate_down,
                ),
                link_map={
                    name + ":snk:" + ifo: amp_name + ":src:" + ifo,
                    sink_pad_full: prev_source_pad,
                },
            )
            prev_source_pad = source_pad_full

    # time segment shift
    for ifo in ifos:
        snr_slices = {r1: {} for r1 in reversed(unique_rates)}
        final_adder_addslices_map = {}

        for from_rate in reversed(unique_rates):
            for to_rate, rate_group in sorted_rates[from_rate].items():
                segments = rate_group["segments"]
                uppad = rate_group["uppad"]
                uppad = Offset.fromsamples(
                    ceil(uppad / Offset.MAX_RATE * from_rate), from_rate
                )
                downpad = rate_group["downpad"]
                delays = []
                for segment in segments:
                    delays.append(Offset.fromsec(segment[0]))

                # Correlate
                corrname = f"{ifo}_corr_{from_rate}_{to_rate}"
                pipeline.insert(
                    LLOIDCorrelate(
                        name=corrname,
                        sink_pad_names=(ifo,),
                        source_pad_names=(ifo,),
                        filters=bases[from_rate][to_rate][ifo],
                        backend=TorchBackend,
                        uppad=uppad,
                        downpad=downpad,
                        delays=delays,
                        reconstruction_segment_list=reconstruction_segment_list,
                    ),
                )
                if from_rate != maxrate:
                    pipeline.insert(
                        link_map={
                            corrname
                            + ":snk:"
                            + ifo: f"{ifo}_down_{from_rate}:src:"
                            + ifo
                        },
                    )
                else:
                    pipeline.insert(
                        link_map={corrname + ":snk:" + ifo: "converter1:src:" + ifo},
                    )

                # matmul
                mmname = f"{ifo}_mm_{from_rate}_{to_rate}"
                pipeline.insert(
                    Matmul(
                        name=mmname,
                        sink_pad_names=(ifo,),
                        source_pad_names=(ifo,),
                        backend=TorchBackend,
                        matrix=coeff[from_rate][to_rate][ifo],
                    ),
                    link_map={mmname + ":snk:" + ifo: corrname + ":src:" + ifo},
                )

                # sum same rate
                sumname = None
                if rate_group["sum_same_rate_slices"] is not None:
                    sl = rate_group["sum_same_rate_slices"]
                    sumname = f"{ifo}_sumindex_{from_rate}_{to_rate}"
                    pipeline.insert(
                        SumIndex(
                            name=sumname,
                            sink_pad_names=(ifo,),
                            source_pad_names=(ifo,),
                            sl=sl,
                            backend=TorchBackend,
                        ),
                        link_map={sumname + ":snk:" + ifo: mmname + ":src:" + ifo},
                    )
                    snr_slices[from_rate][to_rate] = sumname + ":src:" + ifo
                else:
                    snr_slices[from_rate][to_rate] = mmname + ":src:" + ifo

                if from_rate != maxrate:
                    upname = f"{ifo}_up_{from_rate}_{to_rate}"

                    # upsample
                    pipeline.insert(
                        Resampler(
                            name=upname,
                            sink_pad_names=(ifo,),
                            source_pad_names=(ifo,),
                            backend=TorchBackend,
                            inrate=from_rate,
                            outrate=to_rate[-1],
                        ),
                    )

                    # add
                    addname = f"{ifo}_add_{from_rate}_{to_rate}"
                    sink_name = f"{ifo}_up_{from_rate}_{to_rate}"

                    if to_rate[-1] != maxrate:
                        pipeline.insert(
                            Adder(
                                name=addname,
                                sink_pad_names=(ifo, sink_name),
                                source_pad_names=(ifo,),
                                backend=TorchBackend,
                                addslices_map={
                                    sink_name: (
                                        rate_group["addslice"],
                                        slice(rate_group["ntempmax"]),
                                    )
                                },
                            ),
                        )
                    else:
                        final_adder_addslices_map[sink_name] = (
                            rate_group["addslice"],
                            slice(rate_group["ntempmax"]),
                        )

        if nslice == 1 or len(unique_rates) == 1:
            output_source_links[ifo] = mmname + ":src:" + ifo
        else:
            # final adder
            pipeline.insert(
                Adder(
                    name=f"{ifo}_add_{maxrate}",
                    sink_pad_names=(ifo,)
                    + tuple(k for k in final_adder_addslices_map.keys()),
                    source_pad_names=(ifo,),
                    backend=TorchBackend,
                    addslices_map=final_adder_addslices_map,
                ),
            )
            output_source_links[ifo] = f"{ifo}_add_{maxrate}:src:" + ifo

        connected = []
        # links for upsampler and adder
        for from_rate, v in snr_slices.items():
            for to_rate in v.keys():
                if from_rate != maxrate:
                    if to_rate[-1] != maxrate:
                        upname = f"{ifo}_up_{to_rate[-1]}_{to_rate[:-1]}:snk:" + ifo
                        pipeline.insert(
                            link_map={
                                upname: f"{ifo}_add_{from_rate}_{to_rate}:src:" + ifo,
                            }
                        )
                        pipeline.insert(
                            link_map={
                                f"{ifo}_add_{from_rate}_{to_rate}:snk:"
                                + ifo
                                + f"_up_{from_rate}_{to_rate}": f"{ifo}_up_{from_rate}"
                                f"_{to_rate}:src:" + ifo,
                            }
                        )
                        pipeline.insert(
                            link_map={
                                f"{ifo}_add_{from_rate}_{to_rate}:snk:"
                                + ifo: snr_slices[to_rate[-1]][to_rate[:-1]]
                            }
                        )
                    else:
                        pipeline.insert(
                            link_map={
                                f"{ifo}_add_{maxrate}:snk:"
                                + ifo
                                + f"_up_{from_rate}_{to_rate}": f"{ifo}_up_{from_rate}"
                                f"_{to_rate}:src:" + ifo,
                            }
                        )
                        pipeline.insert(
                            link_map={
                                f"{ifo}_add_{maxrate}:snk:"
                                + ifo: snr_slices[to_rate[-1]][to_rate[:-1]]
                            }
                        )
                    connected.append(snr_slices[to_rate[-1]][to_rate[:-1]])

        # link the rest
        # FIXME: find a better way
        for from_rate, v in snr_slices.items():
            for to_rate, snr_link in v.items():
                if from_rate != maxrate:
                    if snr_link not in connected:
                        upname = f"{ifo}_up_{from_rate}_{to_rate}"
                        pipeline.insert(
                            link_map={
                                f"{ifo}_up_{from_rate}_{to_rate}:snk:" + ifo: snr_link
                            }
                        )
    return output_source_links

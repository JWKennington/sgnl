import cmath
import math
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import lal
import lalsimulation as lalsim
import numpy
from lal import LIGOTimeGPS
from scipy.linalg import svd as scipy_svd

from sgnl import chirptime, kernels, spawaveform, templates
from sgnl.psd import condition_psd, taperzero_fseries


def tukeywindow(data, samps=200.0):
    assert (
        len(data) >= 2 * samps
    )  # make sure that the user is requesting something sane
    tp = float(samps) / len(data)
    return lal.CreateTukeyREAL8Window(len(data), tp).data.data


def generate_template(
    template_bank_row,
    approximant,
    sample_rate,
    duration,
    f_low,
    f_high,
    amporder=0,
    order=7,
    fwdplan=None,
    fworkspace=None,
):
    """
    Generate a single frequency-domain template, which
    1. is band-limited between f_low and f_high,
    2. has an IFFT which is duration seconds long and
    3. has an IFFT which is sampled at sample_rate Hz
    """
    if approximant not in templates.sgnl_approximants:
        raise ValueError("Unsupported approximant given %s" % approximant)
    assert f_high <= sample_rate // 2

    # FIXME use hcross somday?
    # We don't here because it is not guaranteed to be orthogonal
    # and we add orthogonal phase later

    parameters = {}
    parameters["m1"] = lal.MSUN_SI * template_bank_row.mass1
    parameters["m2"] = lal.MSUN_SI * template_bank_row.mass2
    parameters["S1x"] = template_bank_row.spin1x
    parameters["S1y"] = template_bank_row.spin1y
    parameters["S1z"] = template_bank_row.spin1z
    parameters["S2x"] = template_bank_row.spin2x
    parameters["S2y"] = template_bank_row.spin2y
    parameters["S2z"] = template_bank_row.spin2z
    parameters["distance"] = 1.0e6 * lal.PC_SI
    parameters["inclination"] = 0.0
    parameters["phiRef"] = 0.0
    parameters["longAscNodes"] = 0.0
    parameters["eccentricity"] = 0.0
    parameters["meanPerAno"] = 0.0
    parameters["deltaF"] = 1.0 / duration
    parameters["f_min"] = f_low
    parameters["f_max"] = f_high
    parameters["f_ref"] = 0.0
    parameters["LALparams"] = None
    parameters["approximant"] = lalsim.GetApproximantFromString(str(approximant))

    hplus, hcross = lalsim.SimInspiralFD(**parameters)
    assert len(hplus.data.data) == int(round(f_high * duration)) + 1
    # pad the output vector if the sample rate was higher than the
    # requested final frequency
    if f_high < sample_rate / 2:
        fseries = lal.CreateCOMPLEX16FrequencySeries(
            name=hplus.name,
            epoch=hplus.epoch,
            f0=hplus.f0,
            deltaF=hplus.deltaF,
            length=int(round(sample_rate * duration)) // 2 + 1,
            sampleUnits=hplus.sampleUnits,
        )
        fseries.data.data = numpy.zeros(fseries.data.length)
        fseries.data.data[: hplus.data.length] = hplus.data.data[:]
        hplus = fseries
    return hplus


def condition_imr_template(
    approximant, data, epoch_time, sample_rate_max, max_ringtime
):
    assert (
        -len(data) / sample_rate_max <= epoch_time < 0.0
    ), "Epoch returned follows a different convention"
    # find the index for the peak sample using the epoch returned by
    # the waveform generator
    epoch_index = -int(epoch_time * sample_rate_max) - 1
    # align the peaks according to an overestimate of max rinddown
    # time for a given split bank
    target_index = len(data) - 1 - int(sample_rate_max * max_ringtime)
    # rotate phase so that sample with peak amplitude is real
    phase = numpy.arctan2(data[epoch_index].imag, data[epoch_index].real)
    data *= numpy.exp(-1.0j * phase)
    data = numpy.roll(data, target_index - epoch_index)
    # re-taper the ends of the waveform that got cyclically permuted
    # around the ring
    tukey_beta = 2.0 * abs(target_index - epoch_index) / float(len(data))
    assert 0.0 <= tukey_beta <= 1.0, "waveform got rolled WAY too much"
    data *= lal.CreateTukeyREAL8Window(len(data), tukey_beta).data.data
    # done
    return data, target_index


def condition_ear_warn_template(
    approximant, data, epoch_time, sample_rate_max, max_shift_time
):
    assert (
        -len(data) / sample_rate_max <= epoch_time < 0.0
    ), "Epoch returned follows a different convention"
    # find the index for the peak sample using the epoch returned by
    # the waveform generator
    epoch_index = -int(epoch_time * sample_rate_max) - 1
    # move the early warning waveforms forward according to the waveform
    # that spends the longest in going from fhigh to ISCO in a given
    # split bank. This effectively ends some waveforms at f < fhigh
    target_index = int(sample_rate_max * max_shift_time)
    data = numpy.roll(data, target_index - epoch_index)
    return data, target_index


def compute_autocorrelation_mask(autocorrelation):
    """
    Given an autocorrelation time series, estimate the optimal
    autocorrelation length to use and return a matrix which masks
    out the unwanted elements. FIXME TODO for now just returns
    ones
    """
    return numpy.ones(autocorrelation.shape, dtype="int")


# FIXME FIR_WHITENER variable
@dataclass
class templates_workspace:
    template_table: Sequence[Any]
    approximant: int
    psd: Sequence[Any]
    f_low: float
    time_slices: Sequence[Any]
    autocorrelation_length: int
    fhigh: float
    sample_rate_max: float = None
    duration: float = None
    length_max: int = None
    FIR_WHITENER: bool = 0

    def __post_init__(self):
        if self.sample_rate_max is None:
            self.sample_rate_max = max(self.time_slices["rate"])
        if self.fhigh is None:
            self.fhigh = self.sample_rate_max / 2.0
        if self.duration is None:
            self.duration = max(self.time_slices["end"])
        if self.length_max is None:
            self.length_max = int(round(self.duration * self.sample_rate_max))

        # Some input checking to avoid incomprehensible error messages
        if not self.template_table:
            raise ValueError("template list is empty")
        if self.f_low < 0.0:
            raise ValueError("f_low must be >= 0. %s" % repr(self.f_low))

        # working f_low to actually use for generating the waveform.  pick
        # template with lowest chirp mass, compute its duration starting
        # from f_low;  the extra time is 10% of this plus 3 cycles (3 /
        # f_low);  invert to obtain f_low corresponding to desired padding.
        # NOTE:  because SimInspiralChirpStartFrequencyBound() does not
        # account for spin, we set the spins to 0 in the call to
        # SimInspiralChirpTimeBound() regardless of the component's spins.
        template = min(self.template_table, key=lambda row: row.mchirp)
        tchirp = lalsim.SimInspiralChirpTimeBound(
            self.f_low,
            template.mass1 * lal.MSUN_SI,
            template.mass2 * lal.MSUN_SI,
            0.0,
            0.0,
        )
        self.working_f_low = lalsim.SimInspiralChirpStartFrequencyBound(
            1.1 * tchirp + 3.0 / self.f_low,
            template.mass1 * lal.MSUN_SI,
            template.mass2 * lal.MSUN_SI,
        )

        # Add duration of PSD to template length for PSD ringing, round up to power of
        # 2 count of samples
        self.working_length = templates.ceil_pow_2(
            self.length_max + round(1.0 / self.psd.deltaF * self.sample_rate_max)
        )
        self.working_duration = float(self.working_length) / self.sample_rate_max

        if self.psd is not None:
            # Smooth the PSD and interpolate to required resolution
            self.psd = condition_psd(
                self.psd,
                1.0 / self.working_duration,
                minfs=(self.working_f_low, self.f_low),
                maxfs=(self.sample_rate_max / 2.0 * 0.90, self.sample_rate_max / 2.0),
                fir_whiten=self.FIR_WHITENER,
            )

        if self.FIR_WHITENER:
            # Compute a frequency response of the time-domain whitening kernel and
            # effectively taper the psd by zero-ing some elements for a FIR kernel
            self.kernel_fseries = taperzero_fseries(
                kernels.fir_whitener_kernel(
                    self.working_length,
                    self.working_duration,
                    self.sample_rate_max,
                    self.psd,
                ),
                minfs=(self.working_f_low, self.f_low),
                maxfs=(self.sample_rate_max / 2.0 * 0.90, self.sample_rate_max / 2.0),
            )

        self.revplan = lal.CreateReverseCOMPLEX16FFTPlan(self.working_length, 1)
        self.fwdplan = lal.CreateForwardREAL8FFTPlan(self.working_length, 1)
        self.tseries = lal.CreateCOMPLEX16TimeSeries(
            name="timeseries",
            epoch=LIGOTimeGPS(0.0),
            f0=0.0,
            deltaT=1.0 / self.sample_rate_max,
            length=self.working_length,
            sampleUnits=lal.Unit("strain"),
        )
        self.fworkspace = lal.CreateCOMPLEX16FrequencySeries(
            name="template",
            epoch=LIGOTimeGPS(0),
            f0=0.0,
            deltaF=1.0 / self.working_duration,
            length=self.working_length // 2 + 1,
            sampleUnits=lal.Unit("strain s"),
        )

        # Calculate the maximum ring down time or maximum shift time
        if self.approximant in templates.sgnl_IMR_approximants:
            self.max_ringtime = max(
                [
                    chirptime.ringtime(
                        row.mass1 * lal.MSUN_SI + row.mass2 * lal.MSUN_SI,
                        chirptime.overestimate_j_from_chi(max(row.spin1z, row.spin2z)),
                    )
                    for row in self.template_table
                ]
            )
        else:
            if self.sample_rate_max > 2.0 * self.fhigh:
                # Calculate the maximum time we need to shift the early warning
                # waveforms forward by, calculated by the 3.5 approximation from
                # fhigh to ISCO.
                self.max_shift_time = max(
                    [
                        spawaveform.chirptime(
                            row.mass1,
                            row.mass2,
                            7,
                            self.fhigh,
                            0.0,
                            spawaveform.computechi(
                                row.mass1, row.mass2, row.spin1z, row.spin2z
                            ),
                        )
                        for row in self.template_table
                    ]
                )

            #
            # Generate each template, downsampling as we go to save memory
            # generate "cosine" component of frequency-domain template.
            # waveform is generated for a canonical distance of 1 Mpc.
            #

    def make_whitened_template(self, template_table_row):
        # FIXME: This is won't work
        # assert template_table_row in self.template_table, "The input
        # Sngl_Inspiral:Table is not found in the workspace."

        # Create template
        fseries = generate_template(
            template_table_row,
            self.approximant,
            self.sample_rate_max,
            self.working_duration,
            self.f_low,
            self.fhigh,
            fwdplan=self.fwdplan,
            fworkspace=self.fworkspace,
        )

        if self.FIR_WHITENER:
            #
            # Compute a product of freq series of the whitening kernel and the template
            # (convolution in time domain) then add quadrature phase
            #
            assert (len(self.kernel_fseries.data.data) // 2 + 1) == len(
                fseries.data.data
            ), "the size of whitening kernel freq series does not match with a given "
            "format of COMPLEX16FrequencySeries."
            fseries.data.data *= self.kernel_fseries.data.data[
                len(self.kernel_fseries.data.data) // 2 - 1 :
            ]
            fseries = templates.QuadraturePhase.add_quadrature_phase(
                fseries, self.working_length
            )
        else:
            #
            # whiten and add quadrature phase ("sine" component)
            #

            if self.psd is not None:
                lal.WhitenCOMPLEX16FrequencySeries(fseries, self.psd)
                fseries = templates.QuadraturePhase.add_quadrature_phase(
                    fseries, self.working_length
                )

        #
        # compute time-domain autocorrelation function
        #

        if self.autocorrelation_length is not None:
            autocorrelation = templates.normalized_autocorrelation(
                fseries, self.revplan
            ).data.data
        else:
            autocorrelation = None

        #
        # transform template to time domain
        #

        lal.COMPLEX16FreqTimeFFT(self.tseries, fseries, self.revplan)

        data = self.tseries.data.data
        epoch_time = fseries.epoch.gpsSeconds + fseries.epoch.gpsNanoSeconds * 1.0e-9

        #
        # extract the portion to be used for filtering
        #

        #
        # condition the template if necessary (e.g. line up IMR
        # waveforms by peak amplitude)
        #

        if self.approximant in templates.sgnl_IMR_approximants:
            data, target_index = condition_imr_template(
                self.approximant,
                data,
                epoch_time,
                self.sample_rate_max,
                self.max_ringtime,
            )
            # record the new end times for the waveforms (since we performed the shifts)
            template_table_row.end = LIGOTimeGPS(
                float(target_index - (len(data) - 1.0)) / self.sample_rate_max
            )
        else:
            if self.sample_rate_max > self.fhigh * 2.0:
                data, target_index = condition_ear_warn_template(
                    self.approximant,
                    data,
                    epoch_time,
                    self.sample_rate_max,
                    self.max_shift_time,
                )
                data *= tukeywindow(data, samps=32)
                # record the new end times for the waveforms (since we performed the
                # shifts)
                template_table_row.end = LIGOTimeGPS(
                    float(target_index) / self.sample_rate_max
                )
            else:
                data *= tukeywindow(data, samps=32)

        data = data[-self.length_max :]

        #
        # normalize so that inner product of template with itself
        # is 2
        #

        norm = abs(numpy.dot(data, numpy.conj(data)))
        data *= cmath.sqrt(2 / norm)

        #
        # sigmasq = 2 \sum_{k=0}^{N-1} |\tilde{h}_{k}|^2 / S_{k} \Delta f
        #
        # XLALWhitenCOMPLEX16FrequencySeries() computed
        #
        # \tilde{h}'_{k} = \sqrt{2 \Delta f} \tilde{h}_{k} / \sqrt{S_{k}}
        #
        # and XLALCOMPLEX16FreqTimeFFT() computed
        #
        # h'_{j} = \Delta f \sum_{k=0}^{N-1} exp(2\pi i j k / N) \tilde{h}'_{k}
        #
        # therefore, "norm" is
        #
        # \sum_{j} |h'_{j}|^{2} = (\Delta f)^{2} \sum_{j} \sum_{k=0}^{N-1}
        # \sum_{k'=0}^{N-1} exp(2\pi i j (k-k') / N) \tilde{h}'_{k} \tilde{h}'^{*}_{k'}
        #                       = (\Delta f)^{2} \sum_{k=0}^{N-1} \sum_{k'=0}^{N-1}
        # \tilde{h}'_{k} \tilde{h}'^{*}_{k'} \sum_{j} exp(2\pi i j (k-k') / N)
        #                       = (\Delta f)^{2} N \sum_{k=0}^{N-1} |\tilde{h}'_{k}|^{2}
        #                       = (\Delta f)^{2} N 2 \Delta f \sum_{k=0}^{N-1}
        # |\tilde{h}_{k}|^{2} / S_{k}
        #                       = (\Delta f)^{2} N sigmasq
        #
        # and \Delta f = 1 / (N \Delta t), so "norm" is
        #
        # \sum_{j} |h'_{j}|^{2} = 1 / (N \Delta t^2) sigmasq
        #
        # therefore
        #
        # sigmasq = norm * N * (\Delta t)^2
        #

        sigmasq = norm * len(data) / self.sample_rate_max**2.0

        return data, autocorrelation, sigmasq


def generate_templates(
    template_table,
    approximant,
    psd,
    f_low,
    time_slices,
    autocorrelation_length=None,
    fhigh=None,
    verbose=False,
):
    # Create workspace for making template bank
    workspace = templates_workspace(
        template_table,
        approximant,
        psd,
        f_low,
        time_slices,
        autocorrelation_length=autocorrelation_length,
        fhigh=fhigh,
    )

    # Check parity of autocorrelation length
    if autocorrelation_length is not None:
        if not (autocorrelation_length % 2):
            raise ValueError(
                f"autocorrelation_length must be odd (got {autocorrelation_length})"
            )
        autocorrelation_bank = numpy.zeros(
            (len(template_table), autocorrelation_length), dtype="cdouble"
        )
        autocorrelation_mask = compute_autocorrelation_mask(autocorrelation_bank)
    else:
        autocorrelation_bank = None
        autocorrelation_mask = None

    # Multiply by 2 * length of the number of sngl_inspiral rows to get the sine/cosine
    # phases.
    template_bank = [
        numpy.zeros(
            (2 * len(template_table), int(round(rate * (end - begin)))), dtype="double"
        )
        for rate, begin, end in time_slices
    ]

    # Store the original normalization of the waveform.  After
    # whitening, the waveforms are normalized.  Use the sigmasq factors
    # to get back the original waveform.
    sigmasq = []

    for i, row in enumerate(template_table):
        if verbose:
            print(
                f"generating template {i + 1}/{len(template_table)}:  "
                f"m1 = {row.mass1:g}, m2 = {row.mass2:g}, "
                f"s1x = {row.spin1x:g}, s1y = {row.spin1y:g}, s1z = {row.spin1z:g}, "
                f"s2x = {row.spin2x:g}, s2y = {row.spin2y:g}, s2z = {row.spin2z:g}",
                file=sys.stderr,
            )
        # FIXME: ensure the row is in template_table
        template, autocorrelation, this_sigmasq = workspace.make_whitened_template(row)

        sigmasq.append(this_sigmasq)

        if autocorrelation is not None:
            autocorrelation_bank[i, ::-1] = numpy.concatenate(
                (
                    autocorrelation[-(autocorrelation_length // 2) :],
                    autocorrelation[: (autocorrelation_length // 2 + 1)],
                )
            )

        #
        # copy real and imaginary parts into adjacent (real-valued)
        # rows of template bank
        #

        for j, time_slice in enumerate(time_slices):
            # start and end times are measured *backwards* from
            # template end;  subtract from n to convert to
            # start and end index;  end:start is the slice to
            # extract, but there's also an amount added equal
            # to 1 less than the stride that I can't explain
            # but probaby has something to do with the reversal
            # of the open/closed boundary conditions through
            # all of this (argh!  Chad!)

            stride = int(round(workspace.sample_rate_max / time_slice["rate"]))
            begin_index = (
                workspace.length_max
                - int(round(time_slice["begin"] * workspace.sample_rate_max))
                + stride
                - 1
            )
            end_index = (
                workspace.length_max
                - int(round(time_slice["end"] * workspace.sample_rate_max))
                + stride
                - 1
            )
            # make sure the rates are commensurate
            assert stride * time_slice["rate"] == workspace.sample_rate_max

            # extract every stride-th sample.  we multiply by
            # \sqrt{stride} to maintain inner product
            # normalization so that the templates still appear
            # to be unit vectors at the reduced sample rate.
            # note that the svd returns unit basis vectors
            # regardless so this factor has no effect on the
            # normalization of the basis vectors used for
            # filtering but it ensures that the chifacs values
            # have the correct relative normalization.
            template_bank[j][(2 * i + 0), :] = template.real[
                end_index:begin_index:stride
            ] * math.sqrt(stride)
            template_bank[j][(2 * i + 1), :] = template.imag[
                end_index:begin_index:stride
            ] * math.sqrt(stride)

    return template_bank, autocorrelation_bank, autocorrelation_mask, sigmasq, workspace


def decompose_templates(template_bank, tolerance, identity=False):
    #
    # sum-of-squares for each template (row).
    #

    chifacs = (template_bank * template_bank).sum(1)

    #
    # this turns this function into a no-op:  the output "basis
    # vectors" are exactly the input templates and the reconstruction
    # matrix is absent (triggers pipeline construction code to omit
    # matrixmixer element)
    #

    if identity:
        return template_bank, None, None, chifacs

    #
    # adjust tolerance according to local norm
    #

    tolerance = 1 - (1 - tolerance) / chifacs.max()

    #
    # S.V.D.
    #

    try:
        U, s, Vh = scipy_svd(template_bank.T)
    except numpy.linalg.LinAlgError as e:
        U, s, Vh = spawaveform.svd(template_bank.T, mod=True, inplace=True)
        print("Falling back on spawaveform", e, file=sys.stderr)

    #
    # determine component count
    #

    residual = numpy.sqrt((s * s).cumsum() / numpy.dot(s, s))
    # FIXME in an ad hoc way force at least 6 principle components
    n = max(min(residual.searchsorted(tolerance) + 1, len(s)), 6)

    #
    # clip decomposition, pre-multiply Vh by s
    #

    U = U[:, :n]
    Vh = numpy.dot(numpy.diag(s), Vh)[:n, :]
    s = s[:n]

    #
    # renormalize the truncated SVD approximation of these template
    # waveform slices making sure their squares still add up to chifacs.
    # This is done by renormalizing the sum of the square of the
    # singular value weighted reconstruction coefficients associated with
    # each template.
    #

    V2 = (Vh * Vh).sum(0)
    for idx, v2 in enumerate(V2):
        Vh[:, idx] *= numpy.sqrt(chifacs[idx] / v2)

    #
    # done.
    #

    return U.T, s, Vh, chifacs

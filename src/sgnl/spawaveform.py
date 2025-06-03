import sys
from math import log

import lal


def schwarz_isco(m1, m2):

    m = lal.MTSUN_SI * (m1 + m2)
    return 1.0 / (6.0**1.5) / m / lal.PI


def bkl_isco(m1, m2):
    q = (m1 / m2) if m1 < m2 else (m2 / m1)
    return (0.8 * q**3 - 2.6 * q**2 + 2.8 * q + 1.0) * schwarz_isco(m1, m2)


def light_ring(m1, m2, chi):
    m = lal.MTSUN_SI * (m1 + m2)
    return 1.0 / (3.0**1.5) / m / lal.PI


def ffinal(mass1, mass2, s=None):
    if s == "schwarz_isco" or s is None:
        return schwarz_isco(mass1, mass2)
    elif s == "bkl_isco":
        return bkl_isco(mass1, mass2)
    elif s == "light_ring":
        return light_ring(mass1, mass2)
    else:
        raise ValueError(
            "Unrecognized ending frequency, must be 'schwarz_isco', 'bkl_isco', or "
            "'light_ring'"
        )


def chirp_time(m1, m2, fLower, order, chi):
    m = m1 + m2
    eta = m1 * m2 / m / m
    c0T = c2T = c3T = c4T = c5T = c6T = c6LogT = c7T = 0.0
    # Switch on PN order, set the chirp time coeffs for that order
    if order in [7, 8]:
        c7T = lal.PI * (
            14809.0 * eta * eta - 75703.0 * eta / 756.0 - 15419335.0 / 127008.0
        )
    if order in [6, 7, 8]:
        c6T = (
            lal.GAMMA * 6848.0 / 105.0
            - 10052469856691.0 / 23471078400.0
            + lal.PI * lal.PI * 128.0 / 3.0
            + eta * (3147553127.0 / 3048192.0 - lal.PI * lal.PI * 451.0 / 12.0)
            - eta * eta * 15211.0 / 1728.0
            + eta * eta * eta * 25565.0 / 1296.0
            + log(4.0) * 6848.0 / 105.0
        )
        c6LogT = 6848.0 / 105.0
    if order in [5, 6, 7, 8]:
        c5T = (
            13.0 * lal.PI * eta / 3.0
            - 7729.0 / 252.0
            - (
                0.4
                * 565.0
                * (-146597.0 + 135856.0 * eta + 17136.0 * eta * eta)
                * chi
                / (2268.0 * (-113.0 + 76.0 * eta))
            )
        )
        # last term is 0 if chi is 0
    if order in [4, 5, 6, 7, 8]:
        c4T = (
            3058673.0 / 508032.0
            + eta * (5429.0 / 504.0 + eta * 617.0 / 72.0)
            + (
                0.4
                * 63845.0
                * (-81.0 + 4.0 * eta)
                * chi
                * chi
                / (8.0 * (-113.0 + 76.0 * eta) ** 2)
            )
        )
        # last term is 0 if chi is 0
        c3T = -32.0 * lal.PI / 5.0 + (
            0.4 * 113.0 * chi / 3.0
        )  # last term is 0 if chi is 0
        c2T = 743.0 / 252.0 + eta * 11.0 / 3.0
        c0T = 5.0 * m * lal.MTSUN_SI / (256.0 * eta)
    else:
        print("ERROR!!!", file=sys.stderr)

    # This is the PN parameter v evaluated at the lower freq. cutoff
    xT = (lal.PI * m * lal.MTSUN_SI * fLower) ** (1 / 3)
    x2T = xT * xT
    x3T = xT * x2T
    x4T = x2T * x2T
    x5T = x2T * x3T
    x6T = x3T * x3T
    x7T = x3T * x4T
    x8T = x4T * x4T

    # Computes the chirp time as tC = t(v_low)
    # tC = t(v_low) - t(v_upper) would be more
    # correct, but the difference is negligble.

    # This formula works for any PN order, because
    # higher order coeffs will be set to zero.

    return (
        c0T
        * (
            1
            + c2T * x2T
            + c3T * x3T
            + c4T * x4T
            + c5T * x5T
            + (c6T + c6LogT * log(xT)) * x6T
            + c7T * x7T
        )
        / x8T
    )


def chirp_time_between_f1_and_f2(m1, m2, fLower, fUpper, order, chi):
    return chirp_time(m1, m2, fLower, order, chi) - chirp_time(
        m1, m2, fUpper, order, chi
    )


def chirptime(mass1, mass2, order, fLower, fFinal=0, chi=0):
    if fFinal:
        return chirp_time_between_f1_and_f2(mass1, mass2, fLower, fFinal, order, chi)
    else:
        return chirp_time(mass1, mass2, fLower, order, chi)


def compute_chi(m1, m2, spin1, spin2):
    totalMass = m1 + m2
    eta = m1 * m2 / totalMass**2
    delta = (1.0 - 4.0 * eta) ** 0.5
    return 0.5 * (spin1 * (1.0 + delta) + spin2 * (1.0 - delta))

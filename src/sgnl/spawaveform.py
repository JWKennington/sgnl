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

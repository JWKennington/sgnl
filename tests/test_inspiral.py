"""Unit test for inspiral pipeline
"""

import pathlib

from sgnl.bin import inspiral

PATH_DATA = pathlib.Path(__file__).parent / "data"

PATHS_SVD_BANK = [
    PATH_DATA / "H1-0750_GSTLAL_SVD_BANK-0-0.xml.gz",
    PATH_DATA / "L1-0750_GSTLAL_SVD_BANK-0-0.xml.gz",
    PATH_DATA / "V1-0750_GSTLAL_SVD_BANK-0-0.xml.gz",
]


class TestInspiral:
    """Unit test for inspiral pipeline"""

    def test_inspiral_whitenoise(self):
        """Test inspiral pipeline

        Based on input from: /home/yun-jing.huang/phd/greg/sgn-runs/runmewhite.sh
        """
        inspiral.inspiral(
            data_source="white",
            channel_name=["H1=FAKE", "L1=FAKE", "V1=FAKE"],
            sample_rate=4096,
            source_buffer_duration=1,
            num_buffers=10,
            psd_fft_length=4,
            svd_bank=[p.as_posix() for p in PATHS_SVD_BANK],
            torch_device="cpu",
            torch_dtype="float32",
            trigger_finding_length=2048,
            trigger_output="H1L1V1-0750_TRIGGERS-0-0.xml.gz",
            fake_sink=True,
        )

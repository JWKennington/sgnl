"""Unit test for inspiral pipeline"""

import pathlib

from sgnligo.sources import DataSourceInfo
from sgnligo.transforms import ConditionInfo

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

        data_source_info = DataSourceInfo(
            data_source="white",
            channel_name=["H1=FAKE", "L1=FAKE", "V1=FAKE"],
            input_sample_rate=4096,
            gps_start_time=0,
            gps_end_time=100,
        )

        condition_info = ConditionInfo(
            psd_fft_length=4,
        )

        inspiral.inspiral(
            data_source_info=data_source_info,
            condition_info=condition_info,
            svd_bank=[p.as_posix() for p in PATHS_SVD_BANK],
            torch_device="cpu",
            torch_dtype="float32",
            trigger_finding_duration=1,
            fake_sink=True,
        )

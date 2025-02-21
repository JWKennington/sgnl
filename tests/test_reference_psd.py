"""Unit test for inspiral pipeline"""

from sgnligo.sources import DataSourceInfo
from sgnligo.transforms import ConditionInfo

from sgnl.bin import reference_psd


class TestReferencePsd:
    """Unit test for inspiral pipeline"""

    def test_reference_psd(self):
        """Test reference_psd pipeline"""
        data_source_info = DataSourceInfo(
            data_source="white",
            channel_name=["H1=FAKE", "L1=FAKE", "V1=FAKE"],
            input_sample_rate=1024,
            gps_start_time=0,
            gps_end_time=10,
        )

        condition_info = ConditionInfo(
            psd_fft_length=4,
            whiten_sample_rate=512,
        )

        reference_psd.reference_psd(
            data_source_info=data_source_info,
            condition_info=condition_info,
        )

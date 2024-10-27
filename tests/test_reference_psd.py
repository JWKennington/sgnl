"""Unit test for inspiral pipeline
"""

import pathlib

from sgnl.bin import reference_psd

class TestReferencePsd:
    """Unit test for inspiral pipeline"""

    def test_reference_psd(self):
        """Test reference_psd pipeline
        """
        reference_psd.reference_psd(
            data_source="white",
            channel_name=["H1=FAKE", "L1=FAKE", "V1=FAKE"],
            input_sample_rate=1024,
            sample_rate=512,
            num_buffers=2048,
            psd_fft_length=4,
        )

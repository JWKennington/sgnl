import copy
import pathlib

import pytest
import torch

from sgnl.sort_bank import SortedBank, group_and_read_banks
from sgnl.svd_bank import Bank

PATH_DATA = pathlib.Path(__file__).parent / "data"

PATHS_SVD_BANK = [
    PATH_DATA / "H1-0008_SGNL_SVD_BANK-0-0.xml.gz",
    PATH_DATA / "L1-0008_SGNL_SVD_BANK-0-0.xml.gz",
    PATH_DATA / "V1-0008_SGNL_SVD_BANK-0-0.xml.gz",
    PATH_DATA / "H1-0009_SGNL_SVD_BANK-0-0.xml.gz",
    PATH_DATA / "L1-0009_SGNL_SVD_BANK-0-0.xml.gz",
    PATH_DATA / "V1-0009_SGNL_SVD_BANK-0-0.xml.gz",
]


class TestGroupBanks:
    def test1(self):
        banks = group_and_read_banks(
            [p.as_posix() for p in PATHS_SVD_BANK[:3]],
            source_ifos=["H1", "V1", "L1"],
        )
        assert len(banks) == 3
        assert [k for k in banks.keys()] == ["H1", "L1", "V1"]
        for bs in banks.values():
            assert len(bs) == 2  # 2 subbanks
            for b in bs:
                assert isinstance(b, Bank)

    def test2(self):
        banks = group_and_read_banks(
            [p.as_posix() for p in PATHS_SVD_BANK],
            source_ifos=["H1", "L1", "V1"],
        )
        assert len(banks) == 3
        assert [k for k in banks.keys()] == ["H1", "L1", "V1"]
        for bs in banks.values():
            assert len(bs) == 3  # 3 subbanks
            for b in bs:
                assert isinstance(b, Bank)

    def test3(self):
        banks = group_and_read_banks(
            [PATHS_SVD_BANK[0].as_posix()],
            source_ifos=["H1"],
        )
        assert len(banks) == 1
        assert [k for k in banks.keys()] == ["H1"]
        for bs in banks.values():
            assert len(bs) == 2
            for b in bs:
                assert isinstance(b, Bank)

    def test4(self):
        with pytest.raises(ValueError):
            group_and_read_banks(
                [
                    PATHS_SVD_BANK[0].as_posix(),
                    PATHS_SVD_BANK[1].as_posix(),
                    PATHS_SVD_BANK[3].as_posix(),
                ],
            )

    def test5(self):
        with pytest.raises(ValueError):
            group_and_read_banks(
                [
                    PATHS_SVD_BANK[0].as_posix(),
                    PATHS_SVD_BANK[1].as_posix(),
                ],
                source_ifos=["H1"],
            )

    def test6(self, capsys):
        banks = group_and_read_banks(
            [
                PATHS_SVD_BANK[0].as_posix(),
                PATHS_SVD_BANK[1].as_posix(),
            ],
            source_ifos=["H1", "L1"],
            nsubbank_pretend=8,
            verbose=True,
        )
        assert len(banks) == 2
        assert [k for k in banks.keys()] == ["H1", "L1"]
        for bs in banks.values():
            assert len(bs) == 8
            for b in bs:
                assert isinstance(b, Bank)

    def test7(self):
        banks = group_and_read_banks(
            [
                PATHS_SVD_BANK[0].as_posix(),
                PATHS_SVD_BANK[1].as_posix(),
            ],
            source_ifos=["H1", "L1"],
            nslice=1,
        )
        assert len(banks) == 2
        assert [k for k in banks.keys()] == ["H1", "L1"]
        for bs in banks.values():
            assert len(bs) == 2
            for b in bs:
                assert isinstance(b, Bank)
                assert len(b.bank_fragments) == 1

    def test8(self):
        with pytest.raises(ValueError):
            group_and_read_banks(
                [
                    PATHS_SVD_BANK[0].as_posix(),
                    PATHS_SVD_BANK[1].as_posix(),
                ],
                source_ifos=["H1", "L1"],
                nslice=8,
            )


class TestSortedBank:
    """Tests for SortedBank class."""

    def test_sorted_bank_basic(self):
        """Test basic SortedBank initialization."""
        banks = group_and_read_banks(
            [PATHS_SVD_BANK[0].as_posix(), PATHS_SVD_BANK[1].as_posix()],
            source_ifos=["H1", "L1"],
        )
        sorted_bank = SortedBank(banks, device="cpu", dtype=torch.float32)

        assert sorted_bank.device == "cpu"
        assert sorted_bank.dtype == torch.float32
        assert sorted_bank.bank_metadata is not None
        assert "ifos" in sorted_bank.bank_metadata
        assert sorted_bank.bank_metadata["ifos"] == ["H1", "L1"]

    def test_sorted_bank_with_nsubbank_pretend(self):
        """Test SortedBank with nsubbank_pretend (lines 300, 365-368, 663)."""
        banks = group_and_read_banks(
            [PATHS_SVD_BANK[0].as_posix(), PATHS_SVD_BANK[1].as_posix()],
            source_ifos=["H1", "L1"],
            nsubbank_pretend=4,
        )
        # Create SortedBank with nsubbank_pretend to hit lines 300, 365-368, 663
        sorted_bank = SortedBank(
            banks,
            device="cpu",
            dtype=torch.float32,
            nsubbank_pretend=4,
            verbose=True,
        )

        assert sorted_bank.nsubbank_pretend == 4
        assert sorted_bank.bank_metadata["nsubbank"] == 4

    def test_sorted_bank_with_different_autocorrelation_lengths(self):
        """Test SortedBank with different autocorrelation lengths.

        This covers lines 708-709, 721, 735-740.
        """
        banks = group_and_read_banks(
            [PATHS_SVD_BANK[0].as_posix(), PATHS_SVD_BANK[1].as_posix()],
            source_ifos=["H1", "L1"],
        )

        # Modify one bank to have a different autocorrelation length
        # Get the current autocorrelation bank shape
        original_acorr = banks["H1"][0].autocorrelation_bank
        orig_len = original_acorr.shape[1]

        # Create a shorter autocorrelation bank for one subbank
        # The padding code uses: pad = (max_acl - acorr_len) // 2
        # So (max_acl - new_len) must be even for proper slicing
        # If original length is odd (e.g., 353), we need new_len with same parity
        # to make the difference even
        new_len = orig_len - 20  # Subtract an even number to maintain parity
        shorter_acorr = original_acorr[:, :new_len]
        banks["H1"][0].autocorrelation_bank = shorter_acorr

        # Now create SortedBank - this should hit lines 708-709, 721, 735-740
        sorted_bank = SortedBank(banks, device="cpu", dtype=torch.float32)

        # Verify autocorrelation_length_mask was created (not None)
        assert sorted_bank.autocorrelation_length_mask is not None
        assert "H1" in sorted_bank.autocorrelation_length_mask
        assert "L1" in sorted_bank.autocorrelation_length_mask

    def test_sorted_bank_verbose(self):
        """Test SortedBank with verbose output."""
        banks = group_and_read_banks(
            [PATHS_SVD_BANK[0].as_posix(), PATHS_SVD_BANK[1].as_posix()],
            source_ifos=["H1", "L1"],
        )
        sorted_bank = SortedBank(banks, device="cpu", dtype=torch.float32, verbose=True)

        assert sorted_bank.verbose is True

    def test_sorted_bank_with_consecutive_same_rates(self):
        """Test SortedBank with consecutive bank fragments at same rate.

        This tests lines 466-469 and 497-499 for sum_same_rate_slices logic.
        """
        banks = group_and_read_banks(
            [PATHS_SVD_BANK[0].as_posix(), PATHS_SVD_BANK[1].as_posix()],
            source_ifos=["H1", "L1"],
        )

        # Modify banks to have consecutive fragments with same non-max rate
        # We duplicate a non-max-rate fragment to create consecutive same rates
        for ifo in ["H1", "L1"]:
            for subbank in banks[ifo]:
                fragments = subbank.bank_fragments
                if len(fragments) >= 2:
                    # Find a fragment that's not at max rate
                    max_rate = max(f.rate for f in fragments)
                    for i, frag in enumerate(fragments):
                        if frag.rate != max_rate and i > 0:
                            # Insert a copy of this fragment before it
                            # to create consecutive same rates
                            dup_frag = copy.copy(frag)
                            fragments.insert(i, dup_frag)
                            break

        sorted_bank = SortedBank(banks, device="cpu", dtype=torch.float32)

        # The test passes if SortedBank can be created with the modified banks
        assert sorted_bank.bank_metadata is not None

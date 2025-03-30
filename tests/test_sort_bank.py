import pathlib

import pytest

from sgnl.sort_bank import group_and_read_banks
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

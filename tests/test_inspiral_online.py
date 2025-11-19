"""Test online inspiral pipeline similar to examples/online Makefile."""

import os
from pathlib import Path

import pytest

from sgnl.bin import inspiral
from sgnl.bin.inspiral import ConditionInfo, DataSourceInfo

# Skip this entire module if sgnl_test_data is not installed
pytest.importorskip("sgnl_test_data")

from sgnl_test_data import get_data_path  # noqa: E402


class TestInspiralOnline:
    """Test online inspiral pipeline with realistic data."""

    def test_inspiral_online_with_snapshots(self, tmp_path):
        """Test online inspiral command with gwdata-noise-realtime source.

        This test runs the online inspiral pipeline with gwdata-noise-realtime
        and verifies that snapshots are created at the specified interval.
        """
        # Get test data paths
        svd_bank_dir = get_data_path("online/svd_bank")
        h1_svd = svd_bank_dir / "H1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz"
        l1_svd = svd_bank_dir / "L1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz"
        v1_svd = svd_bank_dir / "V1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz"

        lr_file = get_data_path(
            "online/likelihood_ratio/H1L1V1-0000_SGNL_LIKELIHOOD_RATIO-0-0.xml.gz"
        )
        rank_stat_file = get_data_path(
            "online/rank_stat_pdfs/H1L1V1-SGNL_RANK_STAT_PDFS-0-0.xml.gz"
        )
        zlg_file = get_data_path(
            "online/zerolag_rank_stat_pdfs/"
            "H1L1V1-0000_SGNL_ZEROLAG_RANK_STAT_PDFS-0-0.xml.gz"
        )

        # Verify all files exist
        assert h1_svd.exists(), f"H1 SVD bank not found: {h1_svd}"
        assert l1_svd.exists(), f"L1 SVD bank not found: {l1_svd}"
        assert v1_svd.exists(), f"V1 SVD bank not found: {v1_svd}"
        assert lr_file.exists(), f"Likelihood ratio file not found: {lr_file}"
        assert rank_stat_file.exists(), f"Rank stat file not found: {rank_stat_file}"
        assert zlg_file.exists(), f"Zerolag file not found: {zlg_file}"

        # Get event config from sgnl repo
        config_file = Path(__file__).parent.parent / "config" / "cbc_db.yaml"
        assert config_file.exists(), f"Event config not found: {config_file}"

        # Set up data source (gwdata-noise-realtime for online mode)
        # Start in the middle of the SVD bank time range for consistency
        gps_start = 1242105073  # Middle of the SVD bank time range
        data_source_info = DataSourceInfo(
            data_source="gwdata-noise-realtime",
            channel_name=["H1=FAKE", "L1=FAKE", "V1=FAKE"],
            input_sample_rate=16384,
            gps_start_time=gps_start,
            gps_end_time=gps_start + 15,  # Run for 15 seconds
        )

        condition_info = ConditionInfo(
            psd_fft_length=4,
        )

        # Create temporary output files
        output_lr = tmp_path / "output_likelihood_ratio.xml.gz"
        trigger_db = tmp_path / "triggers.sqlite"

        # Copy the input likelihood file to the output location for online mode
        import shutil

        from igwn_ligolw import ligolw, lsctables
        from igwn_ligolw import utils as ligolw_utils
        from igwn_ligolw.array import use_in as array_use_in
        from igwn_ligolw.param import use_in as param_use_in

        # Create mass_model directory in tmp_path and copy the mass model file
        mass_model_dir = tmp_path / "mass_model"
        mass_model_dir.mkdir(exist_ok=True)
        mass_model_src = get_data_path(
            "online/mass_model/H1L1V1-GSTLAL_BBH_SALPETER_MASS_MODEL-0-0.h5"
        )
        mass_model_dst = mass_model_dir / "H1L1V1-GSTLAL_BBH_SALPETER_MASS_MODEL-0-0.h5"
        shutil.copy(mass_model_src, mass_model_dst)

        # Load the likelihood ratio file and update the population_model_file path
        # Set up content handler to read old-style LIGO_LW files
        @lsctables.use_in
        @array_use_in
        @param_use_in
        class LIGOLWContentHandler(ligolw.LIGOLWContentHandler):
            pass

        xmldoc = ligolw_utils.load_filename(
            str(lr_file), contenthandler=LIGOLWContentHandler
        )
        # Find and update the population_model_file parameter
        for param in xmldoc.getElementsByTagName("Param"):
            if param.getAttribute("Name") == "population_model_file:param":
                # Update to use the copied mass model file
                param.pcdata = str(mass_model_dst)
                break
        # Write the modified likelihood ratio file
        ligolw_utils.write_filename(xmldoc, str(output_lr))

        # Change to tmp_path so snapshots are created there
        original_dir = os.getcwd()
        os.chdir(tmp_path)

        try:
            # Enable debug logging to track the division by zero issue
            import logging

            logging.basicConfig(level=logging.DEBUG)

            print(
                "Starting inspiral pipeline with 15 second runtime, "
                "snapshot at 14 seconds"
            )
            print(f"GPS start time: {gps_start} (after SVD bank horizon history)")
            print(f"GPS end time: {gps_start + 15}")
            print(f"Working directory: {tmp_path}")

            # Run inspiral pipeline (without Kafka/GraceDB for simplicity)
            # This tests that the pipeline can load the data files and run
            inspiral.inspiral(
                data_source_info=data_source_info,
                condition_info=condition_info,
                svd_bank=[
                    str(h1_svd),
                    str(l1_svd),
                    str(v1_svd),
                ],
                event_config=str(config_file),
                input_likelihood_file=[
                    str(output_lr)
                ],  # In online mode, input and output must be the same
                output_likelihood_file=[str(output_lr)],
                rank_stat_pdf_file=str(rank_stat_file),
                zerolag_rank_stat_pdf_file=[str(zlg_file)],
                trigger_output=[str(trigger_db)],
                torch_device="cpu",
                torch_dtype="float32",
                trigger_finding_duration=1,
                snapshot_interval=14,  # Take snapshot after 14 seconds
                analysis_tag="test",
                job_tag="0000_test",
                gracedb_far_threshold=1e-6,
                # No Kafka or GraceDB for testing
                output_kafka_server=None,
                gracedb_service_url=None,
            )
        finally:
            # Always change back to original directory
            os.chdir(original_dir)

        # Verify output file was created
        assert output_lr.exists(), f"Output likelihood ratio not created: {output_lr}"

        # Basic sanity check: file should have some content
        assert output_lr.stat().st_size > 0, "Output file is empty"

        # Verify snapshot files were created
        # Snapshots are created in GPS-based directories like 00000/
        # With 15 second run and 14 second interval, we should get exactly 1 snapshot
        snapshot_files = list(tmp_path.glob("*/*.xml.gz"))
        assert len(snapshot_files) > 0, f"No snapshot files created in {tmp_path}"

        # Check for expected snapshot file types based on GPS directory structure
        likelihood_snapshots = [
            f for f in snapshot_files if "LIKELIHOOD_RATIO" in f.name
        ]
        zerolag_snapshots = [
            f for f in snapshot_files if "ZEROLAG_RANK_STAT_PDFS" in f.name
        ]

        # We should have at least one snapshot of each type
        assert (
            len(likelihood_snapshots) >= 1
        ), f"No likelihood ratio snapshots found in {tmp_path}"
        assert (
            len(zerolag_snapshots) >= 1
        ), f"No zerolag rank stat PDF snapshots found in {tmp_path}"

        # Verify snapshot files have content
        for snapshot_file in snapshot_files:
            assert (
                snapshot_file.stat().st_size > 0
            ), f"Snapshot file {snapshot_file} is empty"

        print(
            f"Successfully created {len(snapshot_files)} snapshot files "
            f"with snapshot_interval=14"
        )

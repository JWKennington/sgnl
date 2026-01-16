"""Tests for sgnl.simulation module."""

import igwn_segments as segments
from igwn_ligolw import ligolw, lsctables
from igwn_ligolw import utils as ligolw_utils

from sgnl import simulation


def create_sim_inspiral_row(sim_table, geocent_end_time, geocent_end_time_ns=0):
    """Helper to create a properly initialized SimInspiral row."""
    row = sim_table.RowType()

    # Set all columns to default values
    row.process_id = 0
    row.simulation_id = 0
    row.waveform = "test"
    row.geocent_end_time = geocent_end_time
    row.geocent_end_time_ns = geocent_end_time_ns
    row.h_end_time = geocent_end_time
    row.h_end_time_ns = geocent_end_time_ns
    row.l_end_time = geocent_end_time
    row.l_end_time_ns = geocent_end_time_ns
    row.g_end_time = geocent_end_time
    row.g_end_time_ns = geocent_end_time_ns
    row.t_end_time = geocent_end_time
    row.t_end_time_ns = geocent_end_time_ns
    row.v_end_time = geocent_end_time
    row.v_end_time_ns = geocent_end_time_ns
    row.end_time_gmst = 0.0
    row.source = "test"
    row.mass1 = 1.4
    row.mass2 = 1.4
    row.mchirp = 1.2
    row.eta = 0.25
    row.distance = 100.0
    row.longitude = 0.0
    row.latitude = 0.0
    row.inclination = 0.0
    row.coa_phase = 0.0
    row.polarization = 0.0
    row.psi0 = 0.0
    row.psi3 = 0.0
    row.alpha = 0.0
    row.alpha1 = 0.0
    row.alpha2 = 0.0
    row.alpha3 = 0.0
    row.alpha4 = 0.0
    row.alpha5 = 0.0
    row.alpha6 = 0.0
    row.beta = 0.0
    row.spin1x = 0.0
    row.spin1y = 0.0
    row.spin1z = 0.0
    row.spin2x = 0.0
    row.spin2y = 0.0
    row.spin2z = 0.0
    row.theta0 = 0.0
    row.phi0 = 0.0
    row.f_lower = 30.0
    row.f_final = 0.0
    row.eff_dist_h = 100.0
    row.eff_dist_l = 100.0
    row.eff_dist_g = 100.0
    row.eff_dist_t = 100.0
    row.eff_dist_v = 100.0
    row.numrel_mode_min = 0
    row.numrel_mode_max = 0
    row.numrel_data = ""
    row.amp_order = 0
    row.taper = ""
    row.bandpass = 0

    return row


class TestContentHandler:
    """Tests for ContentHandler class."""

    def test_content_handler_exists(self):
        """Test that ContentHandler class exists and is usable."""
        assert simulation.ContentHandler is not None
        assert issubclass(simulation.ContentHandler, ligolw.LIGOLWContentHandler)


class TestSimInspiralToSegmentList:
    """Tests for sim_inspiral_to_segment_list function."""

    def test_sim_inspiral_to_segment_list_basic(self, tmp_path):
        """Test converting sim_inspiral XML to segment list."""
        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())

        sim_table = lsctables.SimInspiralTable.new()
        xmldoc.childNodes[0].appendChild(sim_table)

        row = create_sim_inspiral_row(sim_table, 1000000000, 500000000)
        sim_table.append(row)

        xml_file = tmp_path / "sim_inspiral.xml"
        ligolw_utils.write_filename(xmldoc, str(xml_file))

        result = simulation.sim_inspiral_to_segment_list(str(xml_file), pad=1)

        assert isinstance(result, segments.segmentlist)
        assert len(result) >= 1

    def test_sim_inspiral_to_segment_list_with_padding(self, tmp_path):
        """Test segment list creation with custom padding."""
        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())

        sim_table = lsctables.SimInspiralTable.new()
        xmldoc.childNodes[0].appendChild(sim_table)

        row = create_sim_inspiral_row(sim_table, 1000000000, 0)
        sim_table.append(row)

        xml_file = tmp_path / "sim_inspiral.xml"
        ligolw_utils.write_filename(xmldoc, str(xml_file))

        result = simulation.sim_inspiral_to_segment_list(str(xml_file), pad=5)

        assert isinstance(result, segments.segmentlist)
        if len(result) > 0:
            seg = result[0]
            assert float(seg[1]) - float(seg[0]) >= 10

    def test_sim_inspiral_to_segment_list_verbose(self, tmp_path):
        """Test with verbose output."""
        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())

        sim_table = lsctables.SimInspiralTable.new()
        xmldoc.childNodes[0].appendChild(sim_table)

        row = create_sim_inspiral_row(sim_table, 1000000000, 0)
        sim_table.append(row)

        xml_file = tmp_path / "sim_inspiral.xml"
        ligolw_utils.write_filename(xmldoc, str(xml_file))

        result = simulation.sim_inspiral_to_segment_list(str(xml_file), verbose=True)

        assert isinstance(result, segments.segmentlist)

    def test_sim_inspiral_to_segment_list_multiple_injections(self, tmp_path):
        """Test with multiple injections that coalesce."""
        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())

        sim_table = lsctables.SimInspiralTable.new()
        xmldoc.childNodes[0].appendChild(sim_table)

        for i in range(3):
            row = create_sim_inspiral_row(sim_table, 1000000000 + i, 0)
            row.simulation_id = i
            sim_table.append(row)

        xml_file = tmp_path / "sim_inspiral.xml"
        ligolw_utils.write_filename(xmldoc, str(xml_file))

        result = simulation.sim_inspiral_to_segment_list(str(xml_file), pad=2)

        assert isinstance(result, segments.segmentlist)
        assert len(result) <= 3

    def test_sim_inspiral_to_segment_list_empty(self, tmp_path):
        """Test with empty sim_inspiral table."""
        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())

        sim_table = lsctables.SimInspiralTable.new()
        xmldoc.childNodes[0].appendChild(sim_table)

        xml_file = tmp_path / "sim_inspiral_empty.xml"
        ligolw_utils.write_filename(xmldoc, str(xml_file))

        result = simulation.sim_inspiral_to_segment_list(str(xml_file))

        assert isinstance(result, segments.segmentlist)
        assert len(result) == 0

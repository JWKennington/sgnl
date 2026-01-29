"""Tests for sgnl.viz module."""

import pytest
from matplotlib import pyplot as plt

from sgnl import viz
from sgnl.viz import Section, b64, logo_data, page


class TestB64:
    """Tests for b64 function."""

    def test_b64_with_no_argument(self):
        """Test b64 with default argument (uses current pyplot figure)."""
        # Create a simple plot
        plt.figure()
        plt.plot([1, 2, 3], [1, 2, 3])

        result = b64()

        assert isinstance(result, str)
        # Base64 encoded PNG starts with specific bytes
        assert len(result) > 0
        plt.close()

    def test_b64_with_plot_argument(self):
        """Test b64 with a plot argument."""
        fig, ax = plt.subplots()
        ax.plot([1, 2, 3], [4, 5, 6])

        result = b64(plot=fig)

        assert isinstance(result, str)
        assert len(result) > 0
        plt.close(fig)


class TestPage:
    """Tests for page function."""

    def test_page_with_no_sections(self):
        """Test page with no sections (default argument)."""
        result = page()

        assert isinstance(result, str)
        assert "<!DOCTYPE html>" in result
        assert "SGNL result page" in result
        assert "Streaming Graph Navigator" in result

    def test_page_with_empty_sections_list(self):
        """Test page with an empty sections list."""
        result = page(sections=[])

        assert isinstance(result, str)
        assert "<!DOCTYPE html>" in result

    def test_page_with_sections(self):
        """Test page with actual sections."""
        # Create a section with an image
        section1 = Section(title="Test Section 1", nav="Tab1")
        plt.figure()
        plt.plot([1, 2], [1, 2])
        img_data = b64()
        plt.close()
        section1.append({"img": img_data, "title": "Test Image", "caption": "Caption"})

        # Create a section with a table
        section2 = Section(title="Test Section 2", nav="Tab2")
        section2.append(
            {
                "table": [{"col1": "val1", "col2": "val2"}],
                "title": "Test Table",
                "caption": "Table Caption",
            }
        )

        result = page(sections=[section1, section2])

        assert isinstance(result, str)
        assert "Test Section 1" in result
        assert "Test Section 2" in result
        assert "Tab1" in result
        assert "Tab2" in result
        # First tab should be active
        assert 'class="nav-link active"' in result


class TestSection:
    """Tests for Section class."""

    def test_section_init(self):
        """Test Section initialization."""
        section = Section(title="My Title", nav="My Nav")

        assert section.title == "My Title"
        assert section.nav == "My Nav"
        assert len(section) == 0

    def test_section_html_with_image(self):
        """Test Section.html property with image."""
        section = Section(title="Test", nav="Test Nav")

        plt.figure()
        plt.plot([1, 2], [1, 2])
        img_data = b64()
        plt.close()

        section.append(
            {
                "img": img_data,
                "title": "Image Title",
                "caption": "Image Caption",
            }
        )

        html = section.html

        assert isinstance(html, str)
        assert "Image Title" in html
        assert "Image Caption" in html
        assert "card-img-top" in html

    def test_section_html_with_table(self):
        """Test Section.html property with table."""
        section = Section(title="Test", nav="Test Nav")

        section.append(
            {
                "table": [
                    {"col1": "row1val1", "col2": "row1val2"},
                    {"col1": "row2val1", "col2": "row2val2"},
                ],
                "title": "Table Title",
                "caption": "Table Caption",
            }
        )

        html = section.html

        assert isinstance(html, str)
        assert "Table Title" in html
        assert "Table Caption" in html
        assert "row1val1" in html
        assert "row2val2" in html
        # Column headers default to key names
        assert "col1" in html
        assert "col2" in html

    def test_section_html_with_table_headers(self):
        """Test Section.html property with custom table headers."""
        section = Section(title="Test", nav="Test Nav")

        section.append(
            {
                "table": [{"col1": "val1", "col2": "val2"}],
                "table-headers": {"col1": "Column One", "col2": "Column Two"},
                "title": "Table Title",
                "caption": "Table Caption",
            }
        )

        html = section.html

        assert "Column One" in html
        assert "Column Two" in html

    def test_section_html_invalid_dict_raises(self):
        """Test Section.html raises ValueError for invalid dict."""
        section = Section(title="Test", nav="Test Nav")

        # Dict without 'img' or 'table' key
        section.append(
            {
                "title": "No Image or Table",
                "caption": "This should fail",
            }
        )

        with pytest.raises(ValueError, match="must contain one of"):
            _ = section.html

    def test_section_html_with_empty_table(self):
        """Test Section.html with empty table (table key exists but empty)."""
        section = Section(title="Test", nav="Test Nav")

        section.append(
            {
                "table": [],  # Empty table
                "title": "Empty Table",
                "caption": "This should raise",
            }
        )

        # Empty table should trigger the else branch (ValueError)
        with pytest.raises(ValueError, match="must contain one of"):
            _ = section.html


class TestLogoData:
    """Tests for logo_data function."""

    def test_logo_data_returns_string(self):
        """Test logo_data returns a non-empty string."""
        result = logo_data()

        assert isinstance(result, str)
        assert len(result) > 0

    def test_logo_data_is_base64(self):
        """Test logo_data returns valid base64."""
        import base64

        result = logo_data()

        # Should be valid base64 - this will raise if invalid
        decoded = base64.b64decode(result)
        assert len(decoded) > 0


class TestIfoColors:
    """Tests for IFO color constants."""

    def test_ifo_color_exists(self):
        """Test IFO_COLOR dict has expected keys."""
        assert "H1" in viz.IFO_COLOR
        assert "L1" in viz.IFO_COLOR
        assert "V1" in viz.IFO_COLOR
        assert "K1" in viz.IFO_COLOR

    def test_ifo_combo_color_exists(self):
        """Test IFO_COMBO_COLOR dict is populated."""
        assert len(viz.IFO_COMBO_COLOR) > 0
        # Check some expected combinations
        assert "H1,L1" in viz.IFO_COMBO_COLOR
        assert "H1,L1,V1" in viz.IFO_COMBO_COLOR

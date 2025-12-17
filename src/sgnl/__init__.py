"""Signal package for gravitational wave data analysis."""

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "dev"

import igwn_ligolw.ligolw


def _patch_igwn_ligolw_compatibility():
    """Patch igwn-ligolw < 2.1.0 for compatibility with lalsuite 7.26.

    lalsuite 7.26's lal.series functions pass encoding='Text' to ligolw.Array.build(),
    and XML files created with igwn-ligolw >= 2.1.0 have encoding attributes,
    but igwn-ligolw < 2.1.0 doesn't support the encoding parameter.

    This patch makes the older igwn-ligolw accept and ignore the encoding parameter.
    This can be removed when upgrading to igwn-ligolw >= 2.1.0.
    """

    original_array_build = igwn_ligolw.ligolw.Array.build

    @staticmethod
    def patched_array_build(name, array, *args, encoding=None, **kwargs):
        """Wrapper that accepts and ignores encoding parameter."""
        return original_array_build(name, array, *args, **kwargs)

    igwn_ligolw.ligolw.Array.build = patched_array_build

    def patched_stream_init(self, *args):
        """Wrapper that removes encoding attribute after init."""
        super(igwn_ligolw.ligolw.Array.Stream, self).__init__(*args)
        try:
            del self.Encoding
        except AttributeError:
            pass
        self._tokenizer = igwn_ligolw.ligolw.tokenizer.Tokenizer(self.Delimiter)

    igwn_ligolw.ligolw.Array.Stream.__init__ = patched_stream_init


_patch_igwn_ligolw_compatibility()

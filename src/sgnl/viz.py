import io
import base64
import itertools
import matplotlib

matplotlib.use("agg")
from cycler import cycler

matplotlib.rcParams.update(
    {
        "font.size": 12.0,
        "axes.titlesize": 12.0,
        "axes.labelsize": 12.0,
        "xtick.labelsize": 12.0,
        "ytick.labelsize": 12.0,
        "legend.fontsize": 8.0,
        "figure.dpi": 300,
        "figure.figsize": (8, 4),
        "savefig.dpi": 300,
        "path.simplify": True,
        "font.family": "serif",
        "axes.prop_cycle": cycler("color", ["r", "g", "b", "c", "m", "orange", "aqua"]),
    }
)
from matplotlib import pyplot as plt

IFO_COLOR = {"H1": "#e74c3c", "L1": "#3498db", "V1": "#9b59b6", "K1": "#f1c40f"}


# https://stackoverflow.com/questions/61488790/how-can-i-proportionally-mix-colors-in-python
def __combine_hex_values(colors):
    tot_weight = len(colors)
    r = int(sum([int(c[1:3], 16) for c in colors]) / len(colors))
    g = int(sum([int(c[3:5], 16) for c in colors]) / len(colors))
    b = int(sum([int(c[5:7], 16) for c in colors]) / len(colors))
    zpad = lambda x: x if len(x) == 2 else "0" + x
    return "#" + zpad(hex(r)[2:]) + zpad(hex(g)[2:]) + zpad(hex(b)[2:])


IFO_COMBO_COLOR = {
    ",".join(sorted(combo)): __combine_hex_values([IFO_COLOR[c] for c in combo])
    for level in range(len(IFO_COLOR), 0, -1)
    for combo in itertools.combinations(IFO_COLOR, level)
}
# Overrides
IFO_COMBO_COLOR.update(
    {"H1,L1,V1": "#e67e22", "H1,L1": "#16a085", "L1,V1": "#7f8c8d", "H1,V1": "#bdc3c7"}
)


def b64():
    """
    Using pyplots global variable references to figures, save the current
    figure as a base64 encoded png and return that
    """
    # Save the plot to a BytesIO buffer
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png")
    buffer.seek(0)

    # Encode the image in base64
    return base64.b64encode(buffer.read()).decode("utf-8")


def page(sections=[]):
    """Given a list of Section classes, return the html suitable for a standalone web page"""
    out = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SGNL result page</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" crossorigin="anonymous">
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js" integrity="sha384-YvpcrYf0tY3lHB60NNkmXc5s9fDVZLESaAA55NDzOxhy9GkcIdslK1eN7N6jIeHz" crossorigin="anonymous"></script>
</head>

<body>"""
    logo = logo_data()
    out += f"""
  <div class="container-fluid mb-4">
    <div class=row>
      <div class="col-md-1 ">
        <img  height=75px src="data:image/png;base64,{logo}">
      </div>
      <div class=col-md-8>
        <h1>Streaming Graph Navigator</h1>
      </div>
      <div class=col-md-3>
      
      </div>
    </div>
  </div>
"""
    out += """
  <ul class="nav nav-tabs" id="myTab" role="tablist">
    """
    for n, section in enumerate(sections):
        active = "active" if n == 0 else ""
        selected = "true" if n == 0 else "false"
        out += f"""
    <li class="nav-item" role="presentation">
      <button class="nav-link {active}" id="tab{n}" data-bs-toggle="tab" data-bs-target="#tab-pane{n}" type="button" role="tab" aria-controls="tab-pane{n}" aria-selected={selected}>{section.nav}</button>
    </li>
        """
    out += """
  </ul>

  <div class="tab-content" id="myTabContent">
    """
    for n, section in enumerate(sections):
        active = "active" if n == 0 else ""
        out += f"""
    <div class="tab-pane fade show {active}" id="tab-pane{n}" role="tabpanel" aria-labelledby="tab-pane{n}" tabindex="0">
      <div class="container mt-4">
        <h1 class="text-center mb-4">{section.title}</h1>
        <div class="row">
          {section.html}
        </div>
      </div>
    </div>
        """.format(
            section=section
        )
    out += """
  </div>

</body>
</html>
    """
    return out


class Section(list):
    """Hold a list of dictionaries to describe images in a section of a webpage, e.g.,

    [{'img': ..., 'title': ..., 'caption': ...}, {'img': ..., 'title': ..., 'caption': ...}]

    or also use a list of dictionaries with keys as the columns to include a table

    [{'table': [{'col1':..., 'col2':...,},{'col1':..., 'col2':...,}], 'title': ..., 'caption': ...}, {'img': ..., 'title': ..., 'caption': ...}]

    You cannot have both tables and images in the same dictionary, e.g., this is NOT allowed

    [{'table': [{'col1':..., 'col2':...,},{'col1':..., 'col2':...,}], 'img':..., 'title': ..., 'caption': ...}, {'img': ..., 'title': ..., 'caption': ...}]
    """

    cnt = 0

    def __init__(self, title, nav):
        self.title = title
        self.nav = nav
        super().__init__()

    @property
    def html(self):
        # Generate HTML for the images
        images_html = ""
        for d in self:
            Section.cnt += 1
            if "img" in d:
                assert "table" not in d
                images_html += f"""
          <div class="col-md-6 mb-4">
            <div class="card shadow bg-light bg-gradient">
              <button type="button" class="btn btn-outline-secondary" data-bs-toggle="modal" data-bs-target="#Modal{Section.cnt}">
                <img src="data:image/png;base64,{d['img']}" class="card-img-top" alt="Image {Section.cnt}">
              </button>
              <div class="card-body">
                <p class="lead">
                  {d['title']}
                </p>
                <p class="card-text">{d['caption']}</p>
              </div>
            </div>
          </div>
          <div class="modal modal-xl fade" id="Modal{Section.cnt}" tabindex="-1" aria-labelledby="exampleModalLabel{Section.cnt}" aria-hidden="true">
            <div class="modal-dialog">
              <div class="modal-content">
                <div class="modal-header">
                  <h5 class="modal-title" id="exampleModalLabel{Section.cnt}">{d['title']}</h5>
                  <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body">
                  <img src="data:image/png;base64,{d['img']}" class="card-img-top" alt="Image {Section.cnt}">
                </div>
              </div>
            </div>
          </div>
                """
            elif "table" in d and d["table"]:
                images_html += f"""
          <div class="col-md-12 mb-4">
            <div class="card shadow bg-light bg-gradient">
              <div class="card-body">
                <p class="lead">
                  {d['title']}
                </p>
                <table class="table">
                  <thead>
                    <tr>
                """
                for key in d["table"][0]:
                    if "table-headers" in d:
                        images_html += f"""
                          <th scope="col">{d["table-headers"][key]}</th>
                          """
                    else:
                        images_html += f"""
                          <th scope="col">{key}</th>
                          """
                images_html += f"""
                    </tr>
                  </thead>
                  <tbody class="table-group-divider">
                """
                for row in d["table"]:
                    images_html += f"""
                    <tr>"""
                    for k, v in row.items():
                        images_html += f"""
                      <td>{v}</td>"""
                    images_html += f"""
                    </tr>"""
                images_html += f"""
                  </tbody>
                </table>
                """

                images_html += f""" 
                <p class="card-text">{d['caption']}</p>
              </div>
            </div>
          </div>
                """
            else:
                raise ValueError("%s must contain one of ('img','table')" % d)

        return images_html

def logo_data():
    return """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
wyLDYp3FeYv3llaWiZaLLBsse6x0rdhWhVa1Vvetqda+1tOsq62v2xBtGDZZNhts
2m1RW1fbDNtK2yt2qJ2bncBug13HKMIoj1HCUdWjbtmr2DPt8+1r7R856DiEOxQ5
NDi8HG0+Onn0ytHnR39zdHXMdtzmeM9J0ynUqcipyem1s60z17nS+foY6pigMXPH
NI555WLnwnfZ6HLbleY6znWRa7PrVzd3N7FbnVuvu7l7inuV+y2GFiOKsZRxwYPg
4e8x1+OYx0dPN888zwOef3nZe2V57fLqGWs1lj9229gub1NvjvcW704fuk+Kz2af
Tl8TX45vte9jPzM/nt92v2dMG2Ymczfzpb+jv9j/sP97lidrNutUABYQHFAS0Bao
GRgfWBH4MMg0KD2oNqg/2DV4ZvCpEEJIWMjKkFtsQzaXXcPuD3UPnR3aEqYSFhtW
EfY43DZcHN40Dh0XOm71uPsRFhHCiIZIEMmOXB35IMoqalrU0WhidFR0ZfTTGKeY
WTHnY2mxU2J3xb6L849bHncv3jpeEt+coJYwMaEm4X1iQOKqxM7xo8fPHn85ST9J
kNSYTEpOSN6ePDAhcMLaCd0TXScWT7w5yWpSwaSLk/UnZ08+PkVtCmfKwRRCSmLK
rpQvnEhONWcglZ1aldrPZXHXcV/w/HhreL18b/4q/rM077RVaT3p3umr03szfDPK
MvoELEGF4FVmSOamzPdZkVk7sgazE7P35ijlpOQcEWoKs4QtU42mFkztENmJikWd
0zynrZ3WLw4Tb89FciflNuZpwQ/5Vom15BfJo3yf/Mr8D9MTph8s0CgQFrTOsJ2x
ZMazwqDC32biM7kzm2eZzJo/69Fs5uwtc5A5qXOa55rNXTi3e17wvJ3zKfOz5v9e
5Fi0qujtgsQFTQsNF85b2PVL8C+1xarF4uJbi7wWbVqMLxYsblsyZsn6Jd9KeCWX
Sh1Ly0q/LOUuvfSr06/lvw4uS1vWttxt+cYVxBXCFTdX+q7cuUpjVeGqrtXjVtev
oa8pWfN27ZS1F8tcyjato6yTrOssDy9vXG++fsX6LxUZFTcq/Sv3VhlULal6v4G3
4epGv411mww3lW76tFmw+faW4C311ZbVZVuJW/O3Pt2WsO38b4zfarbrby/d/nWH
cEfnzpidLTXuNTW7DHYtr0VrJbW9uyfubt8TsKexzr5uy16dvaX7wD7Jvuf7U/bf
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
2frxE8YXnzp58uWPP/6YFvebNGnSlMrKyiMdHR2/iEQiWMK7mOqcgp755dKlTaFQ
2AYOGDglGAxOt9lsn9M0rUgFnk6n219UVDTh4MGDu/t6HzNnziyjKGrZuSqaMjIy
njMajSccDsf7mZmZG4cMGfJum71tRSgUUqYu20aj8dVdu3Z1AAD4fL7fsyyLq1TK
9iFDhnzQl2vCzxWDKygoAL/ffzXLsmV5lry/RuiIPkhRE1NMHghQ1HCX0zV06tSp
az78cGWsvb19Y36+Nep0OlcMGDAgYP8eKfVXzZuXrVKpHjl56uRyJhodo1KpXi+7
ouyBrVu+3Vt9ojrRweL6G29Q4Tj+vL2t7W06TGciJAGM9EHwPTuZiY5PJDDDaDSa
U8VFRfM7OjufcDgcv+JYtpsAVZBYWVlZ7wwfNvym9RvWd/XnfkiSXBEIBktQzwhK
AvRGo/HbiRMn3Xv8+PEveR5yioqK5tfX1/+ps6NjcRK4MQwUSoVzSOngm2vraiMV
FRWjnE7naxzHIb1e/6vt27f1aRU6Z9HekiVL1IcOHWqkKMqck5Pz+8LCwterqqrO
0KGwPiH2UVLfli0lJSU3iWvznDlzzG1tbc9iGHaFyWR6cseOHVv+I47NO+8gT506
Nc/n9d7J8fx0kiTXms3m17d9u/VAWuX9ysmzXS7Xmz6fbxASvAgCI1XPwDHfk7JS
Gp/T6/W7BwwY8FBTU9Ob3i5PeUL3EeaNIAg6Nzf3p5WV8fL6/ozx48cvampqWt1N
ApKi08X9ZI2DBw+e0N7e+Su32/1oYWHhLJoODWhpaXmf53lJYmE8XGS1Wm+rrKxc
9cwzz6CVK1du9Xq90zQaTW1FRUXZihUrohcFWIKF+DObzfaSQqEIjhgxotRuty92
tNlf745FJbPKqVSqpvz8/Jv27NlzQLKcjOzs7Pw9hmFWlUr1RklJyWefffZZ8FKC
6bbbbtNWVVXNoWl6Mcuy12A4VqVUKj8YMHDgZ19+sTpx7iU3XK8BgBEFBQUnT58+
rWhubv5fT2fXzSzLJmXK8cCnTclEaQhGRHYbs9n8scFg+MJut/8jFApliAWwov6j
Vqsb8vLybtizZ0+/8+3nzZtnPHXqVDVFUblJTcUlyzGpVPiLioomh8PhiXa7458W
i+UBhNDptrbWjZFIhERCDb94zUaTaUNNTc18AIDRo0ff3trauhwAoKioaNHBgwf7
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"""

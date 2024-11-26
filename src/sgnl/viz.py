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
  <title>Base64 Images with Bootstrap</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" crossorigin="anonymous">
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js" integrity="sha384-YvpcrYf0tY3lHB60NNkmXc5s9fDVZLESaAA55NDzOxhy9GkcIdslK1eN7N6jIeHz" crossorigin="anonymous"></script>
</head>

<body>

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
            <div class="card">
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
            <div class="card">
              <div class="card-body">
                <p class="lead">
                  {d['title']}
                </p>
                <table class="table">
                  <thead>
                    <tr>
                """
                for key in d["table"][0]:
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

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

def page(_images_html = "", _modals = ""):
    return """
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
  <div class="container mt-4">
    <h1 class="text-center mb-4">SGN Injection Results</h1>
    <div class="row">
      {_images_html}
    </div>
  </div>
{_modals}
</body>
</html>
    """.format(_images_html = _images_html, _modals = _modals)

def image_html(_images = []):
    # Generate HTML for the images
    images_html = ""
    for n, d in enumerate(_images):
        images_html += f"""
      <div class="col-md-6 mb-4">
        <div class="card">
          <button type="button" class="btn btn-outline-secondary" data-bs-toggle="modal" data-bs-target="#Modal{n}">
            <img src="data:image/png;base64,{d['img']}" class="card-img-top" alt="Image {n}">
          </button>
          <div class="card-body">
            <p class="lead">
              {d['title']}
            </p>
            <p class="card-text">{d['caption']}</p>
          </div>
        </div>
      </div>
      <div class="modal modal-xl fade" id="Modal{n}" tabindex="-1" aria-labelledby="exampleModalLabel{n}" aria-hidden="true">
        <div class="modal-dialog">
          <div class="modal-content">
            <div class="modal-header">
              <h5 class="modal-title" id="exampleModalLabel{n}">{d['title']}</h5>
              <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
              <img src="data:image/png;base64,{d['img']}" class="card-img-top" alt="Image {n}">
            </div>
          </div>
        </div>
      </div>
        """.format(n=n,d=d)
    return images_html

#def modal_html(_images = []):
#    # Generate HTML for the images
#    modals = ""
#    for n, d in enumerate(_images):
#        modals += f"""
#<div class="modal modal-xl fade" id="Modal{n}" tabindex="-1" aria-labelledby="exampleModalLabel{n}" aria-hidden="true">
#  <div class="modal-dialog">
#    <div class="modal-content">
#      <div class="modal-header">
#        <h5 class="modal-title" id="exampleModalLabel{n}">{d['title']}</h5>
#        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
#      </div>
#      <div class="modal-body">
#        <img src="data:image/png;base64,{d['img']}" class="card-img-top" alt="Image {n}">
#      </div>
#    </div>
#  </div>
#</div>
#        """.format(n=n, d=d)
#    return modals

#!/usr/bin/env python3
import base64
import io
import os
import sqlite3
import uuid

from flask import (
    Flask,
    Request,
    Response,
    json,
    jsonify,
    render_template_string,
    request,
)
from flask_accept import accept
from ligo.lw import array, ligolw, lsctables, param, table
from ligo.lw import utils as ligolw_utils

from sgnl import viz


class CustomRequest(Request):
    def __init__(self, *args, **kwargs):
        super(CustomRequest, self).__init__(*args, **kwargs)
        self.max_form_parts = 32 * 1024 * 1024
        self.max_form_memory_size = 32 * 1024 * 1024


@lsctables.use_in
@array.use_in
@param.use_in
class LIGOLWContentHandler(ligolw.LIGOLWContentHandler):
    pass


app = Flask(__name__)
app.request_class = CustomRequest
app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024
DATABASE = "gracedb_test.db"


# Initialize the SQLite database
def init_db():
    if not os.path.exists(DATABASE):
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute(
            """CREATE TABLE events (
                        graceid TEXT PRIMARY KEY,
                        "group" TEXT,
                        pipeline TEXT,
                        search TEXT,
                        labels TEXT,
                        offline BOOLEAN,
                        filename TEXT,
                        filecontents TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        instruments TEXT,
                        ifos TEXT,
                        far REAL,
                        likelihood REAL,
                        snr REAL
                    )"""
        )
        c.execute(
            """CREATE TABLE logs (
                        graceid TEXT,
                        N INTEGER,
                        comment TEXT,
                        created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        issuer DEFAULT "sgn mockery",
                        filename TEXT,
                        filecontents TEXT,
                        file_version TEXT,
                        tag_names TEXT,
                        FOREIGN KEY(graceid) REFERENCES events(graceid)
                    )"""
        )
        c.execute(
            """CREATE TABLE event_fields (
                        field_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        graceid TEXT,
                        field_name TEXT,
                        field_value TEXT,
                        FOREIGN KEY(graceid) REFERENCES events(graceid)
                    )"""
        )
        conn.commit()
        conn.close()


def execute_query(query, args=(), fetchone=False, fetchall=False):
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(query, args)
    conn.commit()
    if fetchone:
        result = c.fetchone()
    elif fetchall:
        result = c.fetchall()
    else:
        result = None
    conn.close()
    return result


def parse_ligo_lw(file_contents, graceid):
    """Parse LIGO_LW XML and extract fields to store in the database."""
    fileobj = file_contents
    xmldoc = ligolw_utils.load_fileobj(fileobj, contenthandler=LIGOLWContentHandler)
    coinc_table_row = lsctables.CoincTable.get_table(xmldoc)[0]
    coinc_inspiral_row = lsctables.CoincInspiralTable.get_table(xmldoc)[0]
    sngl_inspiral_table = lsctables.SnglInspiralTable.get_table(xmldoc)

    extra_event_info = {
        k: getattr(coinc_table_row, k) for k in ("instruments", "likelihood")
    }
    extra_event_info.update(
        {
            k: getattr(coinc_inspiral_row, k)
            for k in ("combined_far", "ifos", "snr", "mchirp", "mass")
        }
    )
    fields = []
    for n, row in enumerate(sngl_inspiral_table):
        if n == 0:
            fields.append((graceid, "mass1", row.mass1))
            fields.append((graceid, "mass2", row.mass2))
            fields.append((graceid, "spin1z", row.spin1z))
            fields.append((graceid, "spin2z", row.spin2z))
        fields.append((graceid, "%s snr" % row.ifo, row.snr))
        fields.append((graceid, "%s chisq" % row.ifo, row.chisq))
        fields.append((graceid, "%s time" % row.ifo, float(row.end)))

    return fields, extra_event_info, xmldoc


@app.route("/api/", methods=["GET"])
def api():
    data = {
        "links": {
            "events": "http://127.0.0.1:5000/api/events/",
        },
        "templates": {
            "event-detail-template": "http://127.0.0.1:5000/api/events/{graceid}",
            "event-log-template": "http://127.0.0.1:5000/api/events/{graceid}/log/",
            "event-log-detail-template": "http://127.0.0.1:5000/api/events/{graceid}/log/{N}",
            "files-template": "http://127.0.0.1:5000/api/events/{graceid}/files/{filename}",
            "superevent-log-list-template": "https://gracedb.ligo.org/api/superevents/{superevent_id}/logs/",
        },
        "groups": [
            "CBC",
        ],
        "pipelines": [
            "SGN",
        ],
        "searches": [
            "MOCK",
        ],
        "labels": ["MOCK INJ", "MOCK"],
    }
    response = app.response_class(
        response=json.dumps(data), status=200, mimetype="application/json"
    )
    return response


@app.route("/coinc/<graceid>.xml/", methods=["GET"])
def download_coinc(graceid):
    conn = sqlite3.connect(DATABASE)
    xml = base64.b64decode(
        conn.cursor()
        .execute("SELECT filecontents FROM events WHERE graceid == ?", (graceid,))
        .fetchone()[0]
    )
    return Response(xml, mimetype="application/xml")


@app.route("/api/events/", methods=["POST"])
def create_event():
    data = request.form.to_dict()
    if "labels" not in data:
        data["labels"] = ""
    data["filename"] = data["eventFile"]
    del data["eventFile"]

    conn = sqlite3.connect(DATABASE)
    graceid = (
        "T%05d" % conn.cursor().execute("SELECT COUNT(*) FROM events").fetchone()[0]
    )
    filecontents = request.files["eventFile"]

    # Parse LIGO_LW XML file contents
    fields, extra_event_info, xmldoc = parse_ligo_lw(filecontents, graceid)
    b64xml = io.BytesIO()
    ligolw_utils.write_fileobj(xmldoc, b64xml)
    b64xml.seek(0)

    # Insert the event into the database
    execute_query(
        """INSERT INTO events (graceid, "group", pipeline, search, labels, offline, filename, filecontents, instruments, ifos, far, likelihood, snr) \
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            graceid,
            data["group"],
            data["pipeline"],
            data["search"],
            data["labels"],
            data["offline"],
            data["filename"],
            base64.b64encode(b64xml.read()).decode("utf-8"),
            extra_event_info["instruments"],
            extra_event_info["ifos"],
            extra_event_info["combined_far"],
            extra_event_info["likelihood"],
            extra_event_info["snr"],
        ),
    )

    # Insert extracted fields into the event_fields table
    for field in fields:
        execute_query(
            "INSERT INTO event_fields (graceid, field_name, field_value) VALUES (?, ?, ?)",
            field,
        )

    return jsonify({"graceid": graceid}), 201


@app.route("/api/events/<graceid>/log/", methods=["POST", "GET"])
def write_event_log(graceid):
    if request.method == "POST":
        data = request.form.to_dict()
        filecontents = request.files["upload"]

        execute_query(
            "INSERT INTO logs (graceid, comment, filename, filecontents, tag_names) \
                       VALUES (?, ?, ?, ?, ?)",
            (
                graceid,
                data["comment"],
                filecontents.filename,
                base64.b64encode(filecontents.read()).decode("utf-8"),
                ",".join(data.get("tag_name", [])),
            ),
        )

        return jsonify({"graceid": graceid}), 201

    else:
        return jsonify({"graceid": graceid}), 201


@app.route("/api/superevents/<graceid>/logs", methods=["POST"])
def _dummy_logs(graceid):
    return jsonify({"graceid": graceid}), 201


@app.route("/")
def homepage():
    events = execute_query(
        """SELECT graceid, far, likelihood, ifos, instruments, "group", pipeline, search, labels, offline, filename, created_at FROM events""",
        fetchall=True,
    )
    _logs = execute_query(
        "SELECT graceid, comment, filename, created, filecontents FROM logs",
        fetchall=True,
    )
    event_fields = execute_query(
        "SELECT graceid, field_name, field_value FROM event_fields", fetchall=True
    )
    fields = {e[0]: {} for e in events}
    logs = {e[0]: [] for e in events}
    for gid, fn, fv in event_fields:
        fields[gid][fn] = fv
    for gid, comment, filename, created, filecontents in _logs:
        logs[gid].append(
            dict(
                zip(
                    ("comment", "filename", "created", "filecontents"),
                    (comment, filename, created, filecontents),
                )
            )
        )
        if logs[gid][-1]["filename"] is not None and logs[gid][-1]["filename"].endswith(
            ".png"
        ):
            logs[gid][-1]["image"] = True
        else:
            logs[gid][-1]["image"] = False
    # for (gid, x, y, z, c) in _logs:
    #    logs[gid].append((x,y,z,c))
    logo = viz.logo_data()
    page = """
    <!DOCTYPE html>
    <html>
    <head>
      <title>GraceDB Mock</title>
      <link crossorigin="anonymous" href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" integrity="sha384-1BmE4kWBq78iYhFldvKuhfTAU6auU8tT94WrHftjDbrCEXSU1oBoqyl2QvZ6jIW3" rel="stylesheet">
      <script crossorigin="anonymous" integrity="sha384-ka7Sk0Gln4gmtz2MlQnikT1wXgYsOg+OMhuP+IlRH9sENBO0LRn5q+8nbTov4+1p" src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
      <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.0/font/bootstrap-icons.css">
    </head>
    <body>
      <div class=container>
        <h3><img src="data:image/png;base64, {{ logo }}" width=70px> GraceDB Mock Server</h3>
        <hr>
        <p class=lead>Events</p>
        <table class="table table-striped">
            <tr>
                <th>GraceID</th>
                <th></th>
                <th>FAR</th>
                <th>likelihood</th>
                <th>ifos</th>
                <th>instruments</th>
                <th>Group</th>
                <th>Pipeline</th>
                <th>Search</th>
                <th>Labels</th>
                <th>Created</th>
            </tr>
            {% for event in events %}
            <tr>
                <td>
                  <button type="button" class="btn btn-outline-secondary" data-bs-toggle="modal" data-bs-target="#{{ event['graceid'] }}Modal">
                    {{ event['graceid'] }}
                  </button>
                </td>
                <td><a href="/coinc/{{ event['graceid'] }}.xml/"><i class="bi bi-download"></i></a></td>
                <td>{{ "%.2e" % event['far'] | float }}</td>
                <td>{{ "%.2e" % event['likelihood'] | float }}</td>
                <td>{{ event['ifos'] }}</td>
                <td>{{ event['instruments'] }}</td>
                <td>{{ event['group'] }}</td>
                <td>{{ event['pipeline'] }}</td>
                <td>{{ event['search'] }}</td>
                <td>{{ event['labels'] }}</td>
                <td>{{ event['created_at'] }}</td>
            </tr>
            {% endfor %}
        </table>
        {% for event in events %}
        <!-- Modal -->
        <div class="modal fade" id="{{ event['graceid'] }}Modal" tabindex="-1" aria-labelledby="{{ event['graceid'] }}ModalLabel" aria-hidden="true">
          <div class="modal-dialog modal-xl">
            <div class="modal-content">
              <div class="modal-header">
                <h1 class="modal-title fs-5" id="{{ event['graceid'] }}ModalLabel">Extra information for {{ event['graceid'] }}</h1>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
              </div>
              <div class="modal-body">
                <p class=lead> Sngl Inspiral Table</p>
                <hr>
                <table class="table table-striped">
                    <tr>
                        <th>Field Name</th>
                        <th>Field Value</th>
                    </tr>
                    {% for field, val in fields[event['graceid']].items() %}
                    <tr>
                        <td>{{ field }}</td>
                        <td>{{ val }}</td>
                    </tr>
                    {% endfor %}
                </table>
                <br>
                <p class=lead> Logs</p>
                <hr>
                <table class="table table-striped">
                    <tr>
                        <th></th>
                        <th>Message</th>
                        <th>Filename</th>
                        <th>Created At</th>
                    </tr>
                    {% for log in logs[event['graceid']] %}
                    <tr>
                        <td>
                            {% if log['image'] %}
                            <img src="data:image/png;base64, {{ log['filecontents'] }}" width=200px> 
                            {% endif %}
                        </td>
                        <td>{{ log['comment'] }}</td>
                        <td>{{ log['filename'] }}</td>
                        <td>{{ log['created'] }}</td>
                    </tr>
                    {% endfor %}
                </table>
              </div>
            </div>
          </div>
        </div>
        {% endfor %}
      </div>
    </body>
    </html>
    """
    return render_template_string(
        page, events=events, logs=logs, fields=fields, logo=logo
    )


def main():
    init_db()
    app.run(debug=True)


if __name__ == "__main__":
    main()

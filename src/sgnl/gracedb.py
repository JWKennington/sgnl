# Copyright (C) 2009-2013  Kipp Cannon, Chad Hanna, Drew Keppel
# Copyright (C) 2015       Ryan Lang
# Copyright (C) 2020-2021  Patrick Godwin
# Copyright (C) 2024       Yun-Jing Huang


import http.client
import io
import logging
import os
from urllib.parse import urlparse


class FakeGracedbResp(object):
    def __init__(self):
        self.status = http.client.CREATED

    def json(self):
        return {"graceid": -1}


class FakeGracedbClient(object):
    def __init__(self, service_url):
        # Assumes that service url is a directory to write files to
        self.path = urlparse(service_url).path

    def createEvent(self, group, pipeline, filename, filecontents, search):
        with open(os.path.join(self.path, filename), "w") as f:
            f.write(filecontents)
        return FakeGracedbResp()

    def writeLog(self, gracedb_id, message, filename, filecontents, tagname):
        return FakeGracedbResp()

    def writeLabel(self, gracedb_id, tagname):
        return FakeGracedbResp()


def upload_fig(fig, gracedb_client, graceid, filename, log_message, tagname="psd"):
    plotfile = io.BytesIO()

    fig.savefig(plotfile, format=os.path.splitext(filename)[-1][1:])
    logging.info('uploading "%s" for %s', filename, graceid)
    response = gracedb_client.writeLog(
        graceid,
        log_message,
        filename=filename,
        filecontents=plotfile.getvalue(),
        tagname=tagname,
    )
    if response.status != http.client.CREATED:
        raise Exception(
            'upload of "%s" for %s failed: %s' % (filename, graceid, response["error"])
        )

import config
import logging
import json
import tempfile
from google.cloud import storage
from flask import send_file

storage_client = storage.Client()
storage_bucket = storage_client.get_bucket(config.GCS_BUCKET)


def travelinfo_get():  # noqa: E501
    """travelinfo_get

     # noqa: E501


    :rtype: object
    """
    return 'do some magic!'


def raininfluence_get():
    return 'do more magic!'


def firstblob_get():
    workblob = storage_bucket.blob('2019/9/23/20190923T232007Z.json')
    work_data = json.loads(workblob.download_as_string())
    logging.info(f"Work blob {work_data}")

    return work_data


def raininfo_get():
    rainfile = tempfile.NamedTemporaryFile()
    storage_bucket.get_blob('KNMI_20200119.txt').download_to_file(rainfile)

    return send_file(
        rainfile.name,
        mimetype='text/csv',
        as_attachment=True,
        attachment_filename='precipitation.csv')


def g4pp_get():
    g4ppfile = tempfile.NamedTemporaryFile()
    storage_bucket.get_blob('4pp.csv').download_to_file(g4ppfile)

    return send_file(
        g4ppfile.name,
        mimetype='text/csv',
        as_attachment=True,
        attachment_filename='4pp.csv')

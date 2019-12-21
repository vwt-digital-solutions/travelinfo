import logging
import json
import tempfile
from google.cloud import storage
from flask import send_file

storage_client = storage.Client()
storage_bucket = storage_client.get_bucket('my-bucket')


def travelinfo_get():  # noqa: E501
    """travelinfo_get

     # noqa: E501


    :rtype: object
    """
    return 'do some magic!'


def raininfluence_get():
    return 'do more magic!'


def firstblob_get():
    workblob = storage_bucket.get_blob('source/link2/workitems/20191219T180004Z.json')
    work_data = json.loads(workblob.download_as_string())
    logging.info(f"Work blob {work_data}")

    return work_data


def raininfo_get():
    rainfile = tempfile.NamedTemporaryFile()
    storage_bucket.get_blob('KNMI_20190101_20191220.txt').download_to_file(rainfile)

    return send_file(
        rainfile.name,
        mimetype='text/csv',
        as_attachment=True,
        attachment_filename='precipitation.csv')

from collections import OrderedDict

import config
import logging
import json
import tempfile
from google.cloud import storage
from flask import send_file
import csv

storage_client = storage.Client()
storage_bucket = storage_client.get_bucket(config.GCS_BUCKET)

my_rainfile = tempfile.NamedTemporaryFile()

storage_bucket.get_blob('KNMI_20200119.txt').download_to_file(my_rainfile)

rain_dict = OrderedDict()
with open(my_rainfile.name) as read_file:
    my_reader = csv.reader(read_file)
    for row in my_reader:
        if row[0].startswith('#') or not row[2].strip() or row[2].strip() == '-1':
            continue
        station = row[0].strip()
        if station not in rain_dict:
            rain_dict[station] = OrderedDict()
        rain_dict[station][row[1]] = rain_dict[station].get(row[1], 0) + int(row[2].strip())


work_blobs = list(storage_client.list_blobs(storage_bucket, prefix='2019/10'))
print(work_blobs)


def raininfluence_get():
    return {
          "info": [
            {
              "area": "NL",
              "correlation": None,
              "covariance": None,
              "total_precipitation": None,
              "total_work": None
            }
          ]
        }


def firstblob_get():
    workblob = storage_bucket.blob('2019/9/23/20190923T232007Z.json')
    work_data = json.loads(workblob.download_as_string())
    logging.info(f"Work blob {work_data}")

    return work_data


def raininfo_get():
    return rain_dict


def g4pp_get():
    g4ppfile = tempfile.NamedTemporaryFile()
    storage_bucket.get_blob('4pp.csv').download_to_file(g4ppfile)

    return send_file(
        g4ppfile.name,
        mimetype='text/csv',
        as_attachment=True,
        attachment_filename='4pp.csv')


def travelinfo_get():  # noqa: E501
    """travelinfo_get

     # noqa: E501


    :rtype: object
    """
    return 'do some magic!'

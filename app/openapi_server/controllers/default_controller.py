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
        rain_dict[row[1]] = rain_dict.get(row[1], 0) + int(row[2].strip())

print(rain_dict)

work_blobs = list(storage_client.list_blobs(storage_bucket, prefix='2019/10'))
print(work_blobs)
current_day = ''
dagelijkse_storingen = OrderedDict()
for work_blob in work_blobs:
    if work_blob.name[0:10] != current_day:
        current_day = work_blob.name[0:10]
        work_data = json.loads(work_blob.download_as_string())
        print(f"blob {work_blob.name} current_day {current_day}")

        storingen = list(filter(lambda x: True if "storing" in x["Omschrijving"].lower() or "incident" in x["Omschrijving"].lower() else False, work_data["Rows"]))
        today = work_blob.name[11:19]

        dagelijkse_storingen[today] = len(storingen)

print(dagelijkse_storingen)



def travelinfo_get():  # noqa: E501
    """travelinfo_get

     # noqa: E501


    :rtype: object
    """
    return 'do some magic!'


def raininfluence_get():
    for (key, value) in rain_dict.items():
        
    total_work =

    return {
          "info": [
            {
              "area": "NL",
              "correlation": 0.5,
              "covariance": 0.3,
              "total_precipitation": 203,
              "total_work": 12454
            }
          ]
        }



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

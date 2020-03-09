from collections import OrderedDict

import config
import logging
import json
import tempfile
from google.cloud import storage
from flask import send_file
import csv
import re

storage_client = storage.Client()
storage_bucket = storage_client.get_bucket(config.GCS_BUCKET)

my_rainfile = tempfile.NamedTemporaryFile()

storage_bucket.get_blob('KNMI_20200119.txt').download_to_file(my_rainfile)

rain_dict = OrderedDict()
loc_dict = {}
with open(my_rainfile.name) as read_file:
    my_reader = csv.reader(read_file)
    for row in my_reader:
        if row[0].startswith('#') or not row[2].strip() or row[2].strip() == '-1':
            regroups =  re.search('^# ([0-9]{3}):\s*([0-9,.]+)\s.*\s([0-9,.]+)\s.*[\s,-]([0-9,.]+)\s*(\w+)', row[0])
            if regroups:
                loc_dict[regroups.group(1)] = {"lat" : regroups.group(3),
                                               "lon" : regroups.group(2),
                                               "name": regroups.group(5)}
            continue
        station = row[0].strip()
        if station not in rain_dict:
            rain_dict[station] = OrderedDict()
        rain_dict[station][row[1]] = rain_dict[station].get(row[1], 0) + int(row[2].strip())

#print(loc_dict)


work_blobs = list(storage_client.list_blobs(storage_bucket, prefix='workitems/201910'))
print(work_blobs[-1])

#Inladen laatste file 2019/10
work_data = json.loads(work_blobs[-1].download_as_string())



# Location 330



def raininfluence_get():
    return {
          "info": [
            {
              "area": "330",
              "correlation": None,
              "covariance": None,
              "total_precipitation": None,
              "total_work": None
            }
          ]
        }


def firstblob_get():
    workblob = storage_bucket.blob('workitems/20191023T190005Z.json')
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

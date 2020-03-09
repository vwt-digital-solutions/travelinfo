import math
from collections import OrderedDict

import config
import logging
import json
import tempfile
from google.cloud import storage
from flask import send_file
import csv
import re
import pandas as pd

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


work_blobs = list(storage_client.list_blobs(storage_bucket, prefix='workitems/201911'))
print(work_blobs[-1])

#Inladen laatste file 2019/10
work_data = json.loads(work_blobs[-3].download_as_string())

df = pd.DataFrame(work_data["Rows"])
koperstoringen_df = df[(df["Opdrachttype"].str.startswith("Service Koper")) & (df["Omschrijving"].str.lower().str.contains("storing")|df["Omschrijving"].str.lower().str.contains("incident"))]
koperstoringen_df["PCGetal"] = koperstoringen_df["Postcode"].str.slice(0, 4).astype("string")

g4ppfile=tempfile.NamedTemporaryFile()
storage_bucket.get_blob('4pp.csv').download_to_file(g4ppfile)

postcode_df = pd.read_csv(g4ppfile.name)
postcode_df["postcode_string"] = postcode_df["postcode"].astype(str)

def get_nearest_station(pc_lat, pc_lon):
    lat_const = 1852 * 60
    lon_const = lat_const * 0.7
    nearest_station = None
    nearest_dist = None
    for station, values in loc_dict.items():
        stat_lat = float(values["lat"])
        stat_lon = float(values["lon"])
        dist = math.sqrt(pow((stat_lat - pc_lat) * lat_const, 2) +
                         pow((stat_lon - pc_lon) * lon_const, 2))
        if not nearest_station or nearest_dist > dist:
            nearest_station = station
            nearest_dist = dist

    return nearest_station

postcode_df["station"] = postcode_df.apply(lambda x: get_nearest_station(x.latitude, x.longitude), axis=1)
print(postcode_df)


storingen_met_pc_df = pd.merge(koperstoringen_df, postcode_df, left_on=['PCGetal'], right_on=['postcode_string'])
print(storingen_met_pc_df[["station", "postcode"]].groupby(["station"]).count())
#print(df.columns)
#print(df[["latitude", "longitude", "Postcode"]])

#for i, storing in df.iterrows():
#    storing





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

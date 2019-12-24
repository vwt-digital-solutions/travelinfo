import logging
import json
import tempfile
from google.cloud import storage
from flask import send_file
import pandas as pd

storage_client = storage.Client()
storage_bucket = storage_client.get_bucket('vwt-d-gew1-mendix-wha-dojo-1224')

current_date=20191219



rainfile = tempfile.NamedTemporaryFile()
storage_bucket.get_blob('KNMI_20190101_20191220.txt').download_to_file(rainfile)
raininfo = pd.read_csv(rainfile.name, skiprows=61, names=['STN','DATE','RH'])

raininfo = raininfo[raininfo.RH.apply(lambda x : not x.isspace())]
raininfo['RH'] = raininfo['RH'].str.strip()
raininfo = raininfo.to_dict(orient='records')

#print(raininfo)
#print(raininfo['RH'].sum(numeric_only=True))
sumrain=0
for rainday in raininfo:
    if  int(rainday['DATE']) == current_date -5:
        sumrain += int(rainday['RH'])


storingdag = {}
workfiles = storage_bucket.list_blobs(prefix='source/link2/workitems/201912')
for workblob in workfiles:
   storingen = []
   work_data = json.loads(workblob.download_as_string())
   for melding in work_data['Rows']:
       if 'Omschrijving' in melding and melding['Omschrijving'].lower().find('storing') >= 0:
           storingen.append(melding)
   storingdag[workblob.name] = len(storingen)


print(storingdag)
           # print(melding)

def travelinfo_get():  # noqa: E501
    """travelinfo_get

     # noqa: E501


    :rtype: object
    """
    return 'do some magic!'


def raininfluence_get():

    return {"area" : "NL", "total_work" : len(storingen), "total_precipitation" : sumrain}


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


def g4pp_get():
    g4ppfile = tempfile.NamedTemporaryFile()
    storage_bucket.get_blob('4pp.csv').download_to_file(g4ppfile)

    return send_file(
        g4ppfile.name,
        mimetype='text/csv',
        as_attachment=True,
        attachment_filename='4pp.csv')

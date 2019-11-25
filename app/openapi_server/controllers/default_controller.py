import logging
import json
from google.cloud import storage


storage_client = storage.Client()
storage_bucket = storage_client.get_bucket('vwt-d-gew1-mendix-wha-dojo')


def travelinfo_get():  # noqa: E501
    """flightplan_get

     # noqa: E501


    :rtype: object
    """
    return 'do some magic!'


def firstblob_get():
    firstblob = storage_bucket.get_blob('source/hyrde/devices-locations/2019/11/01/20191101T000001Z.json')
    json_data = json.loads(firstblob.download_as_string())
    logging.info(f"First blob {json_data}")
    return json_data

import logging
from google.cloud import storage


storage_client = storage.Client()
storage_bucket = storage_client.get_bucket('vwt-d-gew1-mendix-wha-dojo')
first_blob = storage_bucket.get_blob('source/hyrde/devices-locations/2019/11/01/20191101T000001Z.json')
logging.info(f"First blob {first_blob.download_as_string()}")


def travelinfo_get():  # noqa: E501
    """flightplan_get

     # noqa: E501


    :rtype: object
    """
    return 'do some magic!'

import logging
import json
from google.cloud import storage


storage_client = storage.Client()
storage_bucket = storage_client.get_bucket('my-bucket')


def travelinfo_get():  # noqa: E501
    """flightplan_get

     # noqa: E501


    :rtype: object
    """
    return 'do some magic!'


def firstblob_get():
    carblob = storage_bucket.get_blob('source/hyrde/devices-locations/2019/12/11/20191211T000001Z.json')
    car_data = json.loads(carblob.download_as_string())
    workblob = storage_bucket.get_blob('source/link2/workitems/workitems_geo.json')
    work_data = json.loads(workblob.download_as_string())
    logging.info(f"Car blob {car_data}")
    logging.info(f"Work blob {work_data}")

    return {
        "cars": car_data,
        "work": work_data
    }

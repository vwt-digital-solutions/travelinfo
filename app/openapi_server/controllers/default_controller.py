import logging
import json
from google.cloud import storage


storage_client = storage.Client()
storage_bucket = storage_client.get_bucket('vwt-d-gew1-mendix-wha-dojo-1212')

carblob = storage_bucket.get_blob('source/hyrde/devices-locations/2019/12/11/20191211T110001Z.json')
car_data = json.loads(carblob.download_as_string())
workblob = storage_bucket.get_blob('source/link2/workitems/workitems_geo.json')
work_data = json.loads(workblob.download_as_string())

LAT_DIST = 2.0 / 40000 * 360
LONG_DIST = 2.0 / 40000 * 0.8 * 360
print(f"LONG_DIST {LONG_DIST} LAT_DIST {LAT_DIST}")


def travelinfo_get():  # noqa: E501
    """flightplan_get

     # noqa: E501


    :rtype: object
    """

    unlocated_count = 0
    dichtbij_count = 0

    for work in work_data:
        work['nearby_cars'] = []
        if 'geometry' in work:
            for car in car_data:
                if abs(float(car['latitude'])-float(work['geometry']['coordinates'][1])) < LAT_DIST and \
                   abs(float(car['longitude'])-float(work['geometry']['coordinates'][0])) < LONG_DIST:
                    work['nearby_cars'].append(car)
                    print("Dichtbij {}".format(work))

                    dichtbij_count += 1
        else:
            unlocated_count += 1
            # print(work)

    work_with_cars_nearby = [work for work in work_data if len(work['nearby_cars']) > 1]
    return {
        "count_of_work": len(work_data),
        "count_of_unlocated_work": unlocated_count,
        "count of nearby work": dichtbij_count,
        "count of cars": len(car_data),
        "count of cars nearby": len(work_with_cars_nearby)
    }


def firstblob_get():
    logging.info(f"Car blob {car_data}")
    logging.info(f"Work blob {work_data}")

    return {
        "cars": car_data,
        "work": work_data
    }

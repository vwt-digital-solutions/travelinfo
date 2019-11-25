from datetime import datetime
from datetime import timedelta
import copy
import logging
import json
from google.cloud import storage
import math


def sort_by(key: str, data: list, keep_key: bool = True) -> dict:
    s = {}
    for d in data:
        k = copy.deepcopy(d[key])
        if k not in s.keys():
            s[k] = list()
        if not keep_key:
            del d[key]
        s[k].append(d)
    return s


storage_client = storage.Client()
storage_bucket = storage_client.get_bucket('vwt-d-gew1-mendix-wha-dojo')

cars_data = {}

basepath: str = 'source/hyrde/devices-locations/2019/11/04/'

for blob in storage_bucket.list_blobs(prefix=basepath):
    element = json.loads(blob.download_as_string())
    print(f'Element {element}')
    for loc in element:
        car = cars_data.get(loc['serial'], None)
        if car:
            car["locations"].append(loc)
        else:
            cars_data[loc['serial']] = {"distance": 0,
                                        "duration": 0,
                                        "locations": [loc],
                                        }

min_latitude, max_latitude = 180, 0


for serial, car in cars_data.items():
    prev_location = None
    duration = timedelta()
    distance = 0
    for location in car["locations"]:
        min_latitude = min(min_latitude, float(location["latitude"]))
        max_latitude = max(max_latitude, float(location["latitude"]))
        if datetime.strptime(location['dateTime'], '%Y-%m-%dT%H:%M:%S.%fZ') < datetime(2019, 11, 1):
            continue

        if prev_location:
            d_latitude = (float(location["latitude"]) - float(prev_location["latitude"])) * 111.12
            d_longitude = (float(location["longitude"]) - float(prev_location["longitude"])) * 68.
            distance_delta = math.sqrt(d_latitude**2 + d_longitude**2)
            duration_delta = datetime.strptime(location['dateTime'], '%Y-%m-%dT%H:%M:%S.%fZ') - \
                             datetime.strptime(prev_location['dateTime'], '%Y-%m-%dT%H:%M:%S.%fZ')

            if distance_delta > 0 and location['status'] == 'Moving' or location['status'] != prev_location['status']:
                distance += distance_delta
                duration += duration_delta
                #if duration_delta.seconds > 900:
                    #print(f'This car misses updates driving {distance_delta} during {duration_delta.seconds / 60} minutes at {distance_delta / duration_delta.seconds * 3600} km/h! {car} between {prev_location} and {location}')
                if duration_delta.seconds < 0:
                    print(f'This car travels in time driving {distance_delta} during {duration_delta.seconds / 60} minutes at {distance_delta / duration_delta.seconds * 3600} km/h! {car} between {prev_location} and {location}')
                if (distance_delta / duration_delta.seconds * 3600 > 120) or \
                    distance_delta > 15:
                    print(f'This car drives {distance_delta} at {distance_delta / duration_delta.seconds * 3600} km/h! {car} between {prev_location} and {location}')
        prev_location = location
    car["duration"] = duration
    car["distance"] = distance



print(min_latitude, max_latitude)

#print(cars_data["172782"])
#print(cars_data.keys())


#firstblob = storage_bucket.get_blob('source/hyrde/devices-locations/2019/11/01/20191101T000001Z.json')


def travelinfo_get():  # noqa: E501
    """flightplan_get

     # noqa: E501


    :rtype: object
    """

    total_duration = timedelta()
    total_distance = 0
    l = 0

    for _, car in cars_data.items():
        total_duration += car["duration"]
        total_distance += car["distance"]
        l += 1 if car["distance"] > 0 else 0

    return {"travel_duration": total_duration.total_seconds() / 60,
            "travel_distance": total_distance,
            "nr_cars": len(cars_data),
            "nr_moving_cars": l,
            "avg_distance": total_distance / l,
            "avg_duration": total_duration.total_seconds() / l / 60,
            "avg_speed": total_distance / total_duration.total_seconds() * 3600
            }

    return 'do some magic!'


def firstblob_get():
    firstblob = storage_bucket.get_blob('source/hyrde/devices-locations/2019/11/01/20191101T000001Z.json')
    json_data = json.loads(firstblob.download_as_string())
    logging.info(f"First blob {json_data}")
    return json_data

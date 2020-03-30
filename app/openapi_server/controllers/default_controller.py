from datetime import datetime
from datetime import timedelta
from datetime import date
import gzip
import json
from google.cloud import storage
import math
import config

storage_client = storage.Client()
storage_bucket = storage_client.get_bucket(config.GCS_BUCKET)

start_date = datetime.strptime(config.ANALYZE_START_DATE, '%Y-%m-%d').date()


def retrieve_and_parse_carlocations(msg_store_prefix):
    cars_data = {}

    for blob in storage_bucket.list_blobs(prefix=msg_store_prefix):
        blob_content = gzip.decompress(blob.download_as_string()).decode()
        carloc_msgs = json.loads(blob_content)
        for carloc_msg in carloc_msgs:
            carlocations_list = carloc_msg['carlocations']
            for loc in carlocations_list:
                car = cars_data.get(loc['token'], None)
                if car:
                    car["locations"].append(loc)
                else:
                    cars_data[loc['token']] = {
                        "distance": 0,
                        "duration": 0,
                        "locations": [loc],
                        "stops": []
                    }

    min_latitude, max_latitude = 180, 0

    for token, car in cars_data.items():
        car['locations'] = sorted(car['locations'], key=lambda k: k['when'])
        prev_location = None
        prev_latitude = None
        prev_longitude = None
        first_stationary_location = None
        duration = timedelta()
        distance = 0
        for location in car["locations"]:
            latitude = float(location["geometry"]["coordinates"][1])
            longitude = float(location["geometry"]["coordinates"][0])
            min_latitude = min(min_latitude, latitude)
            max_latitude = max(max_latitude, latitude)
            when = datetime.strptime(location['when'], "%Y-%m-%dT%H:%M:%SZ")
            if when.date() < start_date:
                continue

            if prev_location:
                # Calculate distance
                d_latitude = (latitude - prev_latitude) * 111.12
                d_longitude = (longitude - prev_longitude) * 68.
                distance_delta = math.sqrt(d_latitude ** 2 + d_longitude ** 2)
                duration_delta = when - datetime.strptime(prev_location['when'], '%Y-%m-%dT%H:%M:%SZ')

                if distance_delta > 0 and location['what'] == 'Moving' or location['what'] != prev_location['what']:
                    distance += distance_delta
                    duration += duration_delta
                    # if duration_delta.total_seconds() > 900:
                    # print(f'This car misses updates driving {distance_delta} during {duration_delta.seconds / 60}'
                    #       ' minutes at {distance_delta / duration_delta.seconds * 3600} km/h! {car} between'
                    #       ' {prev_location} and {location}')
                    if duration_delta.total_seconds() < 0:
                        print(f'This car travels in time driving {distance_delta} during {duration_delta.seconds / 60}'
                              ' minutes at {distance_delta / duration_delta.seconds * 3600} km/h! {car}'
                              ' between {prev_location} and {location}')
                    if (distance_delta / duration_delta.total_seconds() * 3600 > 140):
                        print(f'This car drives {distance_delta} km at {distance_delta / duration_delta.seconds * 3600}'
                              'km/h! {car} between {prev_location} and {location}')

                # Determine if at workitem
                # First check if we stopped somewhere
                if not first_stationary_location and prev_location['what'] != 'Stationary' and \
                        location['what'] == 'Stationary':
                    first_stationary_location = location
                # When we start moving again,
                elif first_stationary_location and location['what'] != 'Stationary':
                    # check if time since we stopped exceeds STATIONARY_TIME_ASSUMED_WORK_MINUTES
                    duration_delta = when - datetime.strptime(first_stationary_location['when'], '%Y-%m-%dT%H:%M:%SZ')
                    duration_minutes = duration_delta.total_seconds() / 60
                    if duration_minutes > config.STATIONARY_TIME_ASSUMED_WORK_MINUTES:
                        car["stops"].append({
                            "arrived": first_stationary_location,
                            "left": location,
                            "duration_minutes": duration_minutes
                        })
                    first_stationary_location = None
            prev_location = location
            prev_latitude = latitude
            prev_longitude = longitude
        car["duration"] = duration.total_seconds()
        car["distance"] = distance

    print(f"Parsed {msg_store_prefix}, nr cars {len(cars_data)}, min lat {min_latitude}, max lat {max_latitude}")
    return cars_data


def calculate_totals(cars_data):
    total_duration = 0
    total_distance = 0
    total_stops = 0
    nr_moving_cars = 0

    for _, car in cars_data.items():
        total_duration += car["duration"]
        total_distance += car["distance"]
        total_stops += len(car["stops"])
        nr_moving_cars += 1 if car["distance"] > 0 else 0

    return {"travel_duration": total_duration / 60,
            "travel_distance": total_distance,
            "nr_cars": len(cars_data),
            "nr_moving_cars": nr_moving_cars,
            "nr_stops": total_stops,
            "avg_distance": total_distance / nr_moving_cars if nr_moving_cars > 0 else 0,
            "avg_duration": total_duration / nr_moving_cars / 60 if nr_moving_cars > 0 else 0,
            "avg_speed": total_distance / total_duration * 3600 if total_duration > 0 else 0
            }


def calculate_stops_geo(cars_data):
    feature_collection = {
        "type": "FeatureCollection",
        "features": []
    }
    for car_token, car in cars_data.items():
        for stopitem in car["stops"]:
            feature = {
                "type": "Feature",
                "properties": {
                    "arrival_time": stopitem["arrived"]["when"],
                    "leave_time": stopitem["left"]["when"],
                    "duration_minutes": stopitem["duration_minutes"],
                    "car_token": car_token
                },
                "geometry": stopitem["arrived"]["geometry"]
            }
            feature_collection["features"].append(feature)
    return feature_collection


def analyze_data():
    print("Loading cars_data_total")
    total_cars_blob = storage_bucket.blob(f"{config.BASEPATH}output/cars_data_total.json")
    if total_cars_blob.exists(storage_client):
        total_cars_data = json.loads(total_cars_blob.download_as_string())
    else:
        total_cars_data = {}

    analyze_date = start_date
    while analyze_date <= date.today():
        if analyze_date.weekday() < 5:
            print(f"Checking {analyze_date.strftime('%Y-%m-%d')}")

            cars_data_file = f"{config.BASEPATH}output/cars_data_{analyze_date.strftime('%Y%m%d')}.json"
            cars_data_blob = storage_bucket.blob(cars_data_file)
            analyzed_cars_data = None
            # Always re-analyze yesterday and today or if no data found of analyze_date
            if analyze_date >= date.today() - timedelta(days=1) or not cars_data_blob.exists(storage_client):
                path_prefix = f"{config.BASEPATH}{analyze_date.strftime('%Y/%m/%d')}"
                print(f"Analyzing {path_prefix}")
                analyzed_cars_data = retrieve_and_parse_carlocations(path_prefix)
                if len(analyzed_cars_data) > 0:
                    cars_data_blob.upload_from_string(json.dumps(analyzed_cars_data, indent=2))
                    total_cars_data[analyze_date.strftime('%Y-%m-%d')] = calculate_totals(analyzed_cars_data)

            stops_geo_file = f"{config.BASEPATH}output/stops_geo_file_{analyze_date.strftime('%Y%m%d')}.json"
            stops_geo_blob = storage_bucket.blob(stops_geo_file)
            # Calculate stops_geo if this date was analyzed or stops_geo_blob does not yet exist
            if analyzed_cars_data or not stops_geo_blob.exists(storage_client):
                print(f"Gathering stops geo data for {analyze_date.strftime('%Y-%m-%d')}")
                # If this date was not analyzed, load it from storage
                if not analyzed_cars_data and cars_data_blob.exists(storage_client):
                    analyzed_cars_data = json.loads(cars_data_blob.download_as_string())
                if analyzed_cars_data:
                    stops_geo_json = calculate_stops_geo(analyzed_cars_data)
                    stops_geo_blob.upload_from_string(json.dumps(stops_geo_json))

        analyze_date += timedelta(days=1)

    total_cars_blob.upload_from_string(json.dumps(total_cars_data, indent=2))
    return None, 204


def cars_activity_get():
    total_cars_blob = storage_bucket.blob(f"{config.BASEPATH}output/cars_data_total.json")
    total_cars_data = json.loads(total_cars_blob.download_as_string())
    return total_cars_data


def cars_activity_query_get():
    total_cars_blob = storage_bucket.blob(f"{config.BASEPATH}output/cars_data_total.json")
    total_cars_data = json.loads(total_cars_blob.download_as_string())
    result = [
        {
            "target": "nr_moving_cars",
            "datapoints": []
        },
        {
            "target": "nr_stops",
            "datapoints": []
        },
        {
            "target": "est_travel_distance",
            "datapoints": []
        }
    ]
    for day_date, day_totals in total_cars_data.items():
        epoch_ms = datetime.strptime(day_date, "%Y-%m-%d").timestamp() * 1000.0
        result[0]["datapoints"].append([day_totals["nr_moving_cars"], epoch_ms])
        result[1]["datapoints"].append([day_totals["nr_stops"], epoch_ms])
        result[2]["datapoints"].append([day_totals["travel_distance"], epoch_ms])
    return result


def cars_activity_query_post():
    return cars_activity_query_get()


def cars_activity_date_get(selected_date):
    cars_data_blob = storage_bucket.blob(f"{config.BASEPATH}output/cars_data_{selected_date}.json")
    if cars_data_blob.exists(storage_client):
        return json.loads(cars_data_blob.download_as_string())
    else:
        return "Not found", 404


def stop_locations_get():
    stop_locations_blobs = storage_client.list_blobs(storage_bucket, prefix=f"{config.BASEPATH}output/stops_geo_file")
    result = {
        "stop_locations": []
    }
    for blob in stop_locations_blobs:
        selected_date = blob.name[-13:-5]
        result["stop_locations"].append({
            "date": selected_date,
            "url": f"{config.BASE_URL}/StopLocations/{selected_date}"
        })
    return result


def stop_locations_date_get(selected_date):
    stop_locations_blob = storage_bucket.blob(f"{config.BASEPATH}output/stops_geo_file_{selected_date}.json")
    if stop_locations_blob.exists(storage_client):
        return json.loads(stop_locations_blob.download_as_string())
    else:
        return "Not found", 404

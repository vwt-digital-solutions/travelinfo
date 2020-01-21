# Travelinfo Coding Dojo

This project contains the setup and result of the Travelinfo Coding Dojo.

## Setup
Make sure a ```config.py``` file exists within the ```/app``` directory, based on the [config.example.py](config.example.py), with the correct configuration:
~~~
GCS_BUCKET = The name of the Google Cloud Storage bucket
GCS_STORAGE_BLOB = The name of the Google Cloud Storage Blob object
~~~

## Usage
To run the server, please execute the following from the root directory:

```
cd app
pip3 install -r requirements.txt
python3 -m openapi_server
```

and open your browser to here:

```
http://localhost:8080/ui/
```


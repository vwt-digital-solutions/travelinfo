# coding: utf-8

from __future__ import absolute_import
import unittest

from flask import json

from openapi_server.test import BaseTestCase


class TestDefaultController(BaseTestCase):
    """DefaultController integration test stubs"""

    def test_createflightrequest(self):
        """Test case for createflightrequest

        Create a flightrequest
        """
        trip = {
            "duration": 90,
            "departure": "2020-01-13T14:48",
            "price": 20
        }
        headers = {
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/travelrequest',
            method='POST',
            headers=headers,
            data=json.dumps(trip),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_flightplan_get(self):
        """Test case for flightplan_get

        """
        headers = {
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/travelinfo',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()

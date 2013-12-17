#!/usr/bin/env python
#! -*- coding: utf-8 -*-

from datetime import datetime
import json
import os

from django.test import Client, TestCase
import redis

__doc__ = """

    You need to have redis running locally to have these working.

    To run these:

        (venv)$ python manage.py test dispatch
"""

os.environ['REDIS_DB_NUM'] = '1'

class UberChallenge(TestCase):

    def setUp(self):
        #first seed the redis data store
        self.redis_conn = redis.StrictRedis(host='127.0.0.1',
                               port=7878,
                               db=int(os.environ['REDIS_DB_NUM']))
        self.client = Client()

        #coit tower (1st trip starts)
        response = self.client.post('/trips/', json.dumps({"event":"begin", "lat":37.8025, "lng":-122.4058, "tripId":123}), content_type='application/json')
        #levi strauss office (2nd trip starts)
        self.trip_2_approx_start_time = datetime.utcnow()
        response = self.client.post('/trips/', json.dumps({"event":"begin", "lat":37.80164, "lng":-122.402244, "tripId":456}), content_type='application/json')
        #piperade resturant (1st trip finishes)
        response = self.client.post('/trips/', json.dumps({"event":"end", "lat":37.800619, "lng":-122.401782, "tripId":123, "fare":20}), content_type='application/json')
        #cpmc (3rd trip starts)
        response = self.client.post('/trips/', json.dumps({"event":"begin", "lat":37.790789, "lng":-122.431812, "tripId":789}), content_type='application/json')
        #ucsf cntr, mt zion (3rd trip finishes)
        response = self.client.post('/trips/', json.dumps({"event":"end", "lat":37.785057, "lng":-122.437992, "tripId":789, "fare":40}), content_type='application/json')

        #sample bounding box 1 (trips 1/2 above points are inside this bounding box)
        self.bounding_box1_lat1 = 37.808374 #acquarium of the bay latitude
        self.bounding_box1_lng1 = -122.409196 #acquarium of the bay longitude
        self.bounding_box1_lat2 = 37.7952 #transamerica bldg latitude
        self.bounding_box1_lng2 = -122.4028 #transamerica bldg longitude

        #sample bounding box 2 (contains only trip 3)
        self.bounding_box2_lat1 = 37.791603
        self.bounding_box2_lng1 = -122.439966
        self.bounding_box2_lat2 = 37.785159
        self.bounding_box2_lng2 = -122.43104

    def test_current_trip_count(self):
        """There should be 1 current trip"""
        response = self.client.get('/query/trip_count_right_now/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context['count'], '1')

    def test_time_t_trip_count(self):
        """There would be 1 trip at time self.trip_2_approx_start_time"""
        response = self.client.post('/query/trip_count_at_time_t/', {'time_instant': self.trip_2_approx_start_time.strftime('%Y-%m-%d %H:%M:%S')})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context['count'], '1')

    def test_trips_start_stop1(self):
        """the bounding box 1 contains all the points for trips 1/2"""
        response = self.client.post('/query/trips_start_stop/', {'lat1': self.bounding_box1_lat1,
            'lng1': self.bounding_box1_lng1,
            'lat2': self.bounding_box1_lat2,
            'lng2': self.bounding_box1_lng2,
            'days_back': '0d', #today
            })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context['start_count'], 2)
        self.assertEqual(response.context['stop_count'], 1)
        self.assertEqual(response.context['fare_count'], 20)

    def test_trips_passed_through1(self):
        """the bounding box 1 contains all the points for trips 1/2"""
        response = self.client.post('/query/trips_passed_through/', {'lat1': self.bounding_box1_lat1,
            'lng1': self.bounding_box1_lng1,
            'lat2': self.bounding_box1_lat2,
            'lng2': self.bounding_box1_lng2,
            'days_back': '0d', #today
            })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context['count'], 3)

    def test_trips_start_stop2(self):
        """the bounding box 2 contains only trip 3"""
        response = self.client.post('/query/trips_start_stop/', {'lat1': self.bounding_box2_lat1,
            'lng1': self.bounding_box2_lng1,
            'lat2': self.bounding_box2_lat2,
            'lng2': self.bounding_box2_lng2,
            'days_back': '0d', #today
            })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context['start_count'], 1)
        self.assertEqual(response.context['stop_count'], 1)
        self.assertEqual(response.context['fare_count'], 40)

    def test_trips_passed_through2(self):
        """the bounding box 2 contains only trip 3"""
        response = self.client.post('/query/trips_passed_through/', {'lat1': self.bounding_box2_lat1,
            'lng1': self.bounding_box2_lng1,
            'lat2': self.bounding_box2_lat2,
            'lng2': self.bounding_box2_lng2,
            'days_back': '0d', #today
            })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context['count'], 2)

    def tearDown(self):
        """cleanup redis so subsequent test runs will also work !!!"""
        self.redis_conn.flushall()






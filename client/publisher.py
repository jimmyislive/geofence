#!/usr/bin/env python
#! -*- coding: utf-8 -*-

from datetime import datetime
import hashlib
import hmac
import random

import gevent
from gevent.pool import Pool
import json
import pytz
import requests

from latlng import LAT_LNG_DATA

__author__ = 'Jimmy John'

__doc__ = ''' The following is a client that simulates an event publishing channel.
You can think of this as field mobile devices constantly sending messages to a server.
It is done using gevents(an aynchronous framework). MAX_CLIENTS parallel greenlets(psudo threads)
are created which constantly send updates about it's trip. Once it is done, it starts
over again in a infinite loop, thus providing us with a continuous event publisher
channel.

In this code, we sent events directly to our server as the requirements state that
a pub/sub channel is available to us. (If not we could just as easily send these
messages to a AWS SNS service and have our server endpoint as a subscriber.)

To use this, first ensure you are in the virtualenv python i.e. do:

    $source venv/bin/activate
    $(venv)python client/publisher.py
'''

MAX_CLIENTS = 500
#for dev use:
#POST_URL = 'http://127.0.0.1:6789/trips/'
#for prod use:
POST_URL = 'http://ec2-50-18-87-192.us-west-1.compute.amazonaws.com/trips/'

def _get_random_lat_long():
    """Helper function to pick random lat/lng from seeded static data."""
    point = random.choice(LAT_LNG_DATA)
    return point[1], point[2]

def create_event(client_number):
    """Helper(generator function) to create publish messages for a trip.

    We create a start event, then generate update messages for this event
    and finally an end event. The only purpose of this helper is to create events
    and return it.
    """

    message = {}

    #first generate a start event
    message['tripId'] = random.randrange(100, 1000000)
    message['lat'], message['lng'] = _get_random_lat_long()
    message['event'] = 'begin'
    yield message

    #even numbered clients will have 50 'events' in their trip and odd numbered
    #clients will have 5 'events' in their trip (so we can see the data changing
    #quickly in real-time)
    if client_number % 2:
        final_range = 5
    else:
        final_range = 50

    for i in range(random.randrange(1,final_range)):
        message['event'] = 'update'
        message['lat'], message['lng'] = _get_random_lat_long()
        yield message

    #finally the end event
    message['event'] = 'end'
    message['lat'], message['lng'] = _get_random_lat_long()
    #assuming fare is b/w $3.00 and $100.00
    message['fare'] = random.randrange(3, 100)

    yield message

def publish_events(client_number):
    """Helper function that publishes events to a central server."""

    while 1:

        for event in create_event(client_number):

            r = requests.post(POST_URL, data = json.dumps(event))
            if r.status_code == 200:
                print 'published event {0} at approx {1}'.format(str(event), datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
            else:
                print '*** ERROR ***'
                print str(r)

            gevent.sleep(1)

        #ok, we are done with the first trip, now just keep going in an infinite
        #loop to simulate another trip

def main():
    p = Pool(MAX_CLIENTS)
    for i in range(MAX_CLIENTS):
        p.spawn(publish_events, i)
    p.join()

if __name__ == '__main__':
    main()









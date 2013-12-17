#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import calendar
from datetime import datetime, timedelta
import logging
import os
import json
import traceback

from django.http import HttpResponse, HttpResponseNotAllowed, HttpResponseBadRequest
from django.shortcuts import render_to_response
from django.utils.timezone import utc
import geohash
import redis
import requests

__doc__ = """
        This is the view code that gets executed when users visit the url on the website.
"""

logger = logging.getLogger(__name__)

try:
    #while running unit tests, set the env var REDIS_DB_NUM to something b/w 1-15
    #so you leave the actual redis db(typically 0) untouched.
    redis_db_num = int(os.environ['REDIS_DB_NUM'])
except:
    redis_db_num = 0

redis_conn = redis.StrictRedis(host='127.0.0.1',
                               port=7878,
                               db=redis_db_num)

def index(request):
    """Main page with all the questions."""

    if request.method == 'GET':
        return render_to_response('index.html')

def _update_counters(current_trips_counter_key, trips_counter_key, increment_flag=True):
    """
    Helper function to update counters when an evet arrives. Since two distinct
    counters are updated, we use a transaction mechanism.

    Reference: https://github.com/andymccurdy/redis-py [Section on pipelines]
    """
    with redis_conn.pipeline() as pipe:
        while 1:
            try:
                # put a WATCH on the key that holds our sequence value
                pipe.watch(current_trips_counter_key)
                # after WATCHing, the pipeline is put into immediate execution
                # mode until we tell it to start buffering commands again.
                # this allows us to get the current value of our sequence
                current_value = pipe.get(current_trips_counter_key)
                if increment_flag:
                    #this is a 'begin' event
                    if current_value:
                        next_value = int(current_value) + 1
                    else:
                        #current_value could be returned a None
                        next_value = 1
                else:
                    #this is a 'end' event
                    if current_value:
                        next_value = int(current_value) - 1
                    else:
                        next_value = -1

                # now we can put the pipeline back into buffered mode with MULTI
                pipe.multi()
                pipe.set(current_trips_counter_key, next_value)
                pipe.set(trips_counter_key, next_value)
                pipe.expire(trips_counter_key, 90*24*60*60)
                # and finally, execute the pipeline (the set command)
                pipe.execute()
                # if a WatchError wasn't raised during execution, everything
                # we just did happened atomically.
                break
            except redis.WatchError:
                # another client must have changed 'OUR-SEQUENCE-KEY' between
                # the time we started WATCHing it and the pipeline's execution.
                # our best bet is to just retry.
                continue

def trips(request):
    """
    This is the endpoint that acts as the subscriber for messages in the pub/sub channel.

    When messages arrive here, they are inserted into redis as per the schema
    described in the README.
    """

    if request.method == 'POST':
        try:
            #get the current timestamp as we assume this is the timestamp of the message
            #see 'assumptions' section in README
            now = datetime.utcnow()
            now_seconds = calendar.timegm(now.timetuple())

            message = json.loads(request.raw_post_data)

            #basic sanity checking...
            if not (message.has_key('event') and message.has_key('tripId') and
               message.has_key('lat') and message.has_key('lng')):
                return HttpResponseBadRequest('Input json is not in correct format')
            if message['event'] == 'end' and not message.has_key('fare'):
                return HttpResponseBadRequest('Input json is not in correct format (fare missing in "end" event)')

            geohash_string = geohash.encode(message['lat'], message['lng'])

            #extract the date this timestamp corresponds to
            current_date = '{0}-{1}-{2}'.format(now.year, now.month, now.day)
            current_week = now.strftime('%U')

            #keys for counters for the geohash, by day
            geohash_day_tripset_key = 'geohash:{0}:days:{1}:tripids'.format(geohash_string, current_date)
            geohash_day_startcounter_key = 'geohash:{0}:days:{1}:tot_start_counter'.format(geohash_string, current_date)
            geohash_day_stopcounter_key = 'geohash:{0}:days:{1}:tot_stop_counter'.format(geohash_string, current_date)
            geohash_day_farecounter_key = 'geohash:{0}:days:{1}:tot_fare_counter'.format(geohash_string, current_date)

            #keys for counters for the geohash, by week
            geohash_week_tripset_key = 'geohash:{0}:weeks:{1}:tripids'.format(geohash_string, current_week)
            geohash_week_startcounter_key = 'geohash:{0}:weeks:{1}:tot_start_counter'.format(geohash_string, current_week)
            geohash_week_stopcounter_key = 'geohash:{0}:weeks:{1}:tot_stop_counter'.format(geohash_string, current_week)
            geohash_week_farecounter_key = 'geohash:{0}:weeks:{1}:tot_fare_counter'.format(geohash_string, current_week)

            #we are passing thorugh this geohash, so update the sorted set
            #NOTE that it should be a set so that if there are multiple updates
            #within a geohash, only one tripid is added into the set.
            redis_conn.zadd(geohash_day_tripset_key, 0, message['tripId'])
            redis_conn.zadd(geohash_week_tripset_key, 0, message['tripId'])

            #are we an event that impacts start/stop counts?
            if message['event'].lower() in ['begin', 'end']:

                #build out the redis keys
                current_trips_counter_key = 'current_trips_counter'
                #trips_counter:<epoch time>
                trips_counter_key = 'trips_counter:{0}'.format(calendar.timegm(now.timetuple()))

                event_times_key = 'event_times:{0}'.format(current_date)

                if message['event'].lower() == 'begin':

                    _update_counters(current_trips_counter_key, trips_counter_key)

                    #begin event within a geohash, update it's counter
                    redis_conn.incr(geohash_day_startcounter_key)
                    redis_conn.incr(geohash_week_startcounter_key)

                elif message['event'].lower() == 'end':

                    _update_counters(current_trips_counter_key, trips_counter_key, False)

                    #end event within a geohash, update it's counter
                    redis_conn.incr(geohash_day_stopcounter_key)
                    redis_conn.incr(geohash_week_stopcounter_key)
                    redis_conn.incrbyfloat(geohash_day_farecounter_key, float(message['fare']))
                    redis_conn.incrbyfloat(geohash_week_farecounter_key, float(message['fare']))

                #add the timestamp to a sorted set. This is used in case a query comes
                #in for a timestamp for which we don't have an exact key at
                #trips_counter:<timestamp>. So we will then choose the first value from
                #this sorted set which is sligthly lesser than <timestamp> as that is the
                #last recorded value we have closest to <timestamp>
                redis_conn.zadd(event_times_key, 0, calendar.timegm(now.timetuple()))

                #set it's expiry to 90 days since when it was last accessed
                redis_conn.expire(event_times_key, 90*24*60*60)

            #we now have to store the geohash_string for this lat/lng in such a
            #way, so that it is quickly retrivable during search. But the user
            #can type in any bounding box. The property of geohashes is that if
            #they are close together they will share the same prefix. Also as you
            #move left in the geohash, you loose accuracy i.e. the bounding box becomes bigger.
            #Thus we need an efficient way, once given the bounding box, find the common
            #prefix of the two edges and then use this common prefix for all the
            #geohashes that share the same prefix. Here's a solution using
            #sorted sets


            for i in range(1, len(geohash_string)):
                #the score is the timestamp, value is the actual geohash
                #done it this way caus we can then expire elements after a period
                #of time. I'm not going to implement that here, but using this pattern you
                #can. See https://groups.google.com/forum/#!topic/redis-db/rXXMCLNkNSs
                redis_conn.zadd('geohash_prefixes:{0}'.format(geohash_string[:i]),
                                now_seconds, #score
                                geohash_string)

            return HttpResponse()

        except Exception, e:
            logger.error(str(e))
            logger.error(traceback.print_exc())
            raise e

    else:
        return HttpResponseNotAllowed(['GET', 'DELETE', 'PUT'])


def current_trip_count(request):
    """Returns info on the number of trips right now"""

    if request.method == 'GET':
        t1 = datetime.utcnow()
        count = redis_conn.get('current_trips_counter')
        t2 = datetime.utcnow()
        query_time = t2 - t1
        return render_to_response('current_trip_count.html', {'count': count,
            'query_time': query_time,
            })

def time_t_trip_count(request):
    """Returns info on the number of trips at time t"""

    if request.method == 'GET':
        return render_to_response('time_t_trip_count.html')

    elif request.method == 'POST':

        #POST payload validation
        time_instant = request.POST.get('time_instant', None)
        if not time_instant:
            return render_to_response('time_t_trip_count.html', {'error': 'Please enter a time'})

        try:
            time_instant_datetime = datetime.strptime(time_instant, '%Y-%m-%d %H:%M:%S').replace(tzinfo=utc)
        except ValueError:
            return render_to_response('time_t_trip_count.html', {'error': 'Please enter a valid time in format YYYY-MM-DD HH:MM:SS'})

        time_instant_seconds = calendar.timegm(time_instant_datetime.timetuple())

        #first lets see if there is a key called trips_counter:<timestamp>
        t1 = datetime.utcnow()

        count = redis_conn.get('trips_counter:{0}'.format(time_instant_seconds))
        t2 = datetime.utcnow()

        if count:
            query_time = t2 - t1
            return render_to_response('time_t_trip_count.html', {'count': count,
                'query_time': query_time, 't': str(time_instant_datetime)
            })
        else:
            #ok, so this key is not there. so we have to search the appropriate bucket
            #for the closest timestamp
            current_date = '{0}-{1}-{2}'.format(time_instant_datetime.year,
                                                time_instant_datetime.month,
                                                time_instant_datetime.day)

            event_times_key = 'event_times:{0}'.format(current_date)

            if redis_conn.exists(event_times_key):
                #so now we have to find the closest timestamp in the sorted set
                #but iterating over the set in a O(n) operation. Also, if the set
                #is large it can hold up redis while it is returning the set

                #here's a quick hack to find the closest element...

                t1 = datetime.utcnow()
                #first add the time you are looking for into the sorted set
                redis_conn.zadd(event_times_key, 0, time_instant_seconds) #O(log(n))
                #since it is a sorted set we know that what we added went into the right position
                #so get it's index
                rank = int(redis_conn.zrank(event_times_key, time_instant_seconds)) #O(log(n))

                #now we have the rank, just get the previous index
                required_time_key = redis_conn.zrange(event_times_key, rank-1, rank-1)[0] #O(log(N)+1)
                #thus required_time_key is the key we are looking for and we can get the
                #final trip count by GETing trips_counter:<required_time_key>

                count = redis_conn.get('trips_counter:{0}'.format(required_time_key))

                #cleanup event_times_key sorted set
                redis_conn.zrem(event_times_key, time_instant_seconds) #O(1*log(N))

                t2 = datetime.utcnow()

                return render_to_response('time_t_trip_count.html', {'count': count,
                    'query_time': t2 - t1, 't': str(time_instant_datetime)
                })

            else:
                #so the event_times_key sorted set does not even exist. Since the
                #requirements state that there are an average of 500 trips hapenning
                #at any given time, the only explanation for a day bucket not being
                #present is that it is aged out (i.e. expired). Maybe it has passed
                #90 days or something...
                #rather than complicate the logic and go backwards and see if there is
                #a previous day bucket etc, based on the problem statement, this is
                #a simplifying assumption
                return render_to_response('time_t_trip_count.html', {'count': 0,
                    'query_time': None, 'error': 'No info available for this time'
                })

        return HttpResponse()

def _valiate_input(request):
    """Helper function to validate input fields like bounding box co-ordinates etc"""

    did_not_validate = 0
    err_msg = ''

    lat1 = request.POST.get('lat1', None)
    lng1 = request.POST.get('lng1', None)
    lat2 = request.POST.get('lat2', None)
    lng2 = request.POST.get('lng2', None)

    days_back = request.POST.get('days_back', None)

    if not days_back:
        did_not_validate = 1
        err_msg = 'Please enter how far back do you want to look into'

    if not (lat1 and lng1 and lat2 and lng2):
        did_not_validate = 1
        err_msg = 'Please enter all lat/lng values'

    try:
        upper_left_geohash_string = geohash.encode(float(lat1), float(lng1))
    except Exception:
        upper_left_geohash_string = ''
        did_not_validate = 1
        err_msg = 'Please enter correct values for top left lat/lng'

    try:
        lower_right_geohash_string = geohash.encode(float(lat2), float(lng2))
    except Exception:
        lower_right_geohash_string = ''
        did_not_validate = 1
        err_msg = 'Please enter correct values for bottom right lat/lng'

    return (did_not_validate, err_msg, lat1, lng1, lat2, lng2, days_back, upper_left_geohash_string, lower_right_geohash_string)

def _helper_get_target_geohashes(days_back, upper_left_geohash_string, lower_right_geohash_string):
    """Helper function to calculate the geohashes that lie within the bounding box
    and that had trips within the specified timeframe.
    """

    common_prefix = ''
    #see assumptions section in README
    for i in range(min(len(upper_left_geohash_string), len(lower_right_geohash_string))):
        if upper_left_geohash_string[i] != lower_right_geohash_string[i]:
            common_prefix = upper_left_geohash_string[:i]
            break
    else:
        common_prefix = upper_left_geohash_string

    #so now we have to get all geocodes which have the prefix <common_prefix>
    #as all those geocodes will be contained in the bounding box
    target_geohashes = redis_conn.zrange('geohash_prefixes:{0}'.format(common_prefix), 0, -1)

    candidate_sub_keys = []

    #we need to know how far back in time we need to go
    now = datetime.utcnow()
    current_date = '{0}-{1}-{2}'.format(now.year, now.month, now.day)
    current_week = now.strftime('%U')

    duration = int(days_back[:-1])

    if days_back.endswith('d'):
        if not duration:
            candidate_sub_keys.append('days:{0}'.format(current_date))
        else:
            #accumulate the info from now until the 'days_back'
            for i in range(duration):
                new_date = now - timedelta(days=i)
                new_date_str = '{0}-{1}-{2}'.format(new_date.year, new_date.month, new_date.day)
                candidate_sub_keys.append('days:{0}'.format(new_date_str))

    elif days_back.endswith('w'):
        if not duration:
            candidate_sub_keys.append('weeks:{0}'.format(current_week))
        else:
            #accumulate the info from now until the 'weeks_back'
            for i in range(duration):
                new_date = now - timedelta(days=i*7)
                new_week_str = new_date.strftime('%U')
                candidate_sub_keys.append('weeks:{0}'.format(new_week_str))

    return (target_geohashes, candidate_sub_keys)

def trips_passed_through(request):
    """Calculates the number of trips through a geo-rect, within a specified time-frame.

    1. does some basic input validation.
    2. gets the common prefix of the geohashes of the input lat/lng. This gives
       the common prefixes of all geohashes contained in the geo-rect
    3. Get all the constituent geohashes by extracting elements of set at
        geohash_prefixes:<common prefix>
    4. Now that we have the target geohashes, iterate through them and extract
       necessary info from the set at geohash:<target-geohash>:<[days|weeks]>:<[day|week]>:tripids
    """
    if request.method == 'GET':
        return render_to_response('trips_passed_through.html')
    elif request.method == 'POST':

        t1 = datetime.utcnow()

        did_not_validate, err_msg, lat1, lng1, lat2, lng2, days_back, upper_left_geohash_string, lower_right_geohash_string = _valiate_input(request)

        if did_not_validate:
            return render_to_response('trips_passed_through.html', {'error': err_msg})

        target_geohashes, candidate_sub_keys = _helper_get_target_geohashes(days_back, upper_left_geohash_string, lower_right_geohash_string)

        count = 0
        #now iterate through all the geohashes in the geo-rect and extract the
        #values from the appripriate time bucketed keys
        for target in target_geohashes:
            for candidate_sub_key in candidate_sub_keys:
                count += redis_conn.zcard('geohash:{0}:{1}:tripids'.format(target, candidate_sub_key))

        t2 = datetime.utcnow()

        return render_to_response('trips_passed_through.html', {'count': count,
            'query_time': t2 - t1,
            'lat1': lat1,
            'lng1': lng1,
            'lat2': lat2,
            'lng2': lng2
            })

def trips_start_stop(request):
    """Calculates the number of trips that started/stopped and the sum of their fares
    within a bounding box and time range."""

    if request.method == 'GET':
        return render_to_response('trips_start_stop.html')
    elif request.method == 'POST':
        t1 = datetime.utcnow()

        did_not_validate, err_msg, lat1, lng1, lat2, lng2, days_back, upper_left_geohash_string, lower_right_geohash_string = _valiate_input(request)

        if did_not_validate:
            return render_to_response('trips_passed_through.html', {'error': err_msg})

        target_geohashes, candidate_sub_keys = _helper_get_target_geohashes(days_back, upper_left_geohash_string, lower_right_geohash_string)

        start_count = 0
        stop_count = 0
        fare_count = 0

        #now iterate through all the geohashes in the geo-rect and extract the
        #values from the appripriate time bucketed keys
        for target in target_geohashes:
            for candidate_sub_key in candidate_sub_keys:
                try:
                    start_count += int(redis_conn.get('geohash:{0}:{1}:tot_start_counter'.format(target, candidate_sub_key)))
                except TypeError:
                    #got None or something, probably caus this key is not there
                    pass

                try:
                    stop_count += int(redis_conn.get('geohash:{0}:{1}:tot_stop_counter'.format(target, candidate_sub_key)))
                except TypeError:
                    #got None or something, probably caus this key is not there
                    pass

                try:
                    fare_count += float(redis_conn.get('geohash:{0}:{1}:tot_fare_counter'.format(target, candidate_sub_key)))
                except TypeError:
                    #got None or something, probably caus this key is not there
                    pass

        t2 = datetime.utcnow()

        return render_to_response('trips_start_stop.html', {'start_count': start_count,
            'stop_count': stop_count,
            'fare_count': fare_count,
            'query_time': t2 - t1,
            'lat1': lat1,
            'lng1': lng1,
            'lat2': lat2,
            'lng2': lng2
            })














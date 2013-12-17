#!/usr/bin/env python
#! -*- coding: utf-8 -*-

__doc__ = """Config settings for Gunicorn."""

import multiprocessing
import os

port = 6789

bind = '127.0.0.1:{0}'.format(port)
#how many workers to start
workers = multiprocessing.cpu_count() * 2 + 1
#workers = 2
#The maximum number of pending connections.
backlog = 2048
#we want workers to be asynchronous
worker_class = 'gevent'
#The maximum number of simultaneous clients.
worker_connections = 1000
#The maximum number of requests a worker will process before restarting.
#If this is set to zero (the default) then the automatic worker restarts are disabled.
max_requests = 0
#in seconds
timeout = 30
#max req line length to prevent DDOS
limit_request_line = 4094
limit_request_fields = 100
limit_request_field_size = 8190

loglevel = 'info'
#we are controlling gunicorn via supervisord, which will daemonize for you
daemon = False


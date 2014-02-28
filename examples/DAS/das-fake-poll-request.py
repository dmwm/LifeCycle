#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : das-fake-poll-request.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: 
"""

# system modules
import json
import time
import random

# local modules
from utils.options import options

def poll_request():
    inp, out = options()

    with open(inp, 'r') as istream:
        payload = json.loads(istream.read())

    # Emulate polling for the PID by waiting a random amount of time since
    # making the request
    # You would obviously poll DAS with the PID in question to see if it had
    # finished, instead
    host = payload['workflow']['das_server']
    query = payload['workflow']['query']
    print "poll_request, host=%s, query=%s" % (host, query)
    pid = payload['workflow']['pid']
    timestamp = payload['workflow']['timestamp']

    now = int(time.time())
    if ( now - timestamp > random.randint(10, 30) ):
        print "poll_request: pid=",payload['workflow']['pid']," Complete!"
    else: #Push the event for this script back on the stack, so it loops...
        print "poll_request: pid=",payload['workflow']['pid']," Still waiting..."
        payload['workflow']['Events'].insert(0,'das_poll_request')

    with open(out, 'w') as ostream:
        ostream.write(json.dumps(payload))

if __name__ == '__main__':
    poll_request()

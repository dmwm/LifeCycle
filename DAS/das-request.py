#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : das-request.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: 
"""

# system modules
import json

# local modules
from utils.options import options
from utils.das_utils import get_data

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
    timestamp = payload['workflow']['timestamp']
    data = get_data(host, query)
    payload['workflow']['das_data'] = data

    with open(out, 'w') as ostream:
        ostream.write(json.dumps(payload))

if __name__ == '__main__':
    poll_request()

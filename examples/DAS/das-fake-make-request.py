#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : das-fake-make-request.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: 
"""

# system modules
import re
import json
import time
import random

# local modules
from utils.options import options

# Use this to generate a fake PID
def generate_uid(base, seed='qwertyuiopasdfghjklzxcvbnm1234567890',
                    fixed=False):
    "Generate UID for given base"
    if  fixed:
        while base > len(seed):
            seed += seed
    uid = ''
    for idx in range(0, base):
        if  fixed:
            uid += seed[idx]
        else:
            uid += random.choice(seed)
    return uid

def make_request():
    "Make request action"
    inp, out = options()

    with open(inp, 'r') as istream:
        payload = json.loads(istream.read())

    host = payload['workflow']['das_server']
    query = payload['workflow']['query']
    # emulate fake request which returns uid and we assign it
    payload['workflow']['pid'] = generate_uid(32, query)
    payload['workflow']['timestamp'] = int(time.time())

    print "make_request: pid=%s" % payload['workflow']['pid']

    with open(out, 'w') as ostream:
        ostream.write(json.dumps(payload))

if __name__ == '__main__':
    make_request()

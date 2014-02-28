#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : das-spawn-workflows.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: spawn workflow module
"""

# system modules
import json
import copy

# local modules
from utils.options import options

def spawn_workflow():
    inp, out = options()

    with open(inp, 'r') as istream:
        payload = json.loads(istream.read())

    # This just does a repeated clone of the worklow and modifies a parameter
    # (which isn't used, it's just for show). You could adapt this to fetch a
    # bunch of dataset names, for example, and create a new payload for each one
    host = payload['workflow'].get('das_server', 'https://cmsweb.cern.ch')
    payloads = []
    for idx in range(2):
        pld = copy.deepcopy(payload)
        query = 'das-query-%s' % idx
        pld['workflow']['query'] = query
        pld['workflow']['das_server'] = host
        print "spawn-workflows: query=%s" % query
        payloads.append(pld)

    with open(out, 'w') as ostream:
        ostream.write(json.dumps(payloads))

if __name__ == '__main__':
    spawn_workflow()

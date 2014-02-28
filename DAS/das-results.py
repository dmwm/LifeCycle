#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : das-whats-next.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: DAS results module
"""

# system modules
import json

# local modules
from utils.options import options

def das_results():
    inp, out = options()

    with open(inp, 'r') as istream:
        payload = json.loads(istream.read())

    query = payload['workflow']['query']
    das_data = payload['workflow']['das_data']
    status = das_data['status']
    nresults = das_data['nresults']
    print "query=%s, status=%s, nres=%s" % (query, status, nresults)

if __name__ == '__main__':
   das_results()

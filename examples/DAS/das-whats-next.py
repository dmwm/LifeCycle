#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : das-whats-next.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: next action module
"""

# system modules
import json

# local modules
from utils.options import options

def next_action():
    inp, out = options()

    with open(inp, 'r') as istream:
        payload = json.loads(istream.read())

    print "Query with pid=%s is completed, fetch results" % payload['workflow']['pid']

if __name__ == '__main__':
   next_action()

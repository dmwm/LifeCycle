#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : das-queries.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: spawn workflow module
"""

# system modules
import json
import copy
import time

# local modules
from utils.options import options
from utils.das_utils import get_data, gen_queries

def spawn_workflow():
    inp, out = options()

    with open(inp, 'r') as istream:
        payload = json.loads(istream.read())

    host = payload['workflow'].get('DASServer', 'https://cmsweb.cern.ch')
    query = payload['workflow'].get('DASSeedQuery', '/A*/*/*')
    ntests = int(payload['workflow'].get('DASNtests', 5))
    lkeys = payload['workflow'].get('DASLookupKeys', \
        ['dataset', 'block', 'file', 'summary', 'file,lumi', 'file,run,lumi'])
    print "host=%s, seed=%s, ntests=%s, lkeys=%s" \
            % (host, query, ntests, lkeys)
    sys.exit(0)

    # Fetch list of dataset from DAS server
    res = get_data(host, query)
    if  res['status'] != 'ok':
        msg  = 'Unable to fetch list of datasets from DAS server, '
        msg += 'status=%s' % res['status']
        raise Exception(msg)

    # parse resput data and extract list of datasets
    datasets = [r['dataset'][0]['name'] for r in res['data']]

    # Generate workflows with DAS queries
    payloads = []
    for query in gen_queries(datasets[:ntests], lkeys):
        pld = copy.deepcopy(payload)
        pld['workflow']['query'] = query
        pld['workflow']['das_server'] = host
        pld['workflow']['timestamp'] = int(time.time())
        print query
        payloads.append(pld)

    with open(out, 'w') as ostream:
        ostream.write(json.dumps(payloads))

if __name__ == '__main__':
    spawn_workflow()

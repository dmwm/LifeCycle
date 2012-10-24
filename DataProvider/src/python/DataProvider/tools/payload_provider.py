#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
"""
File       : payload_provider.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: Payload provider is a tool to generate LifeCycle workflows
             It takes input workflow provided by LifeCycle and generate
             output payload for DBS/Phedex systems.
"""
# system modules
import os
import sys
import cjson
import pprint
from optparse import OptionParser

# data provider modules
from DataProvider.core.dbs_provider import DBSProvider
from DataProvider.core.phedex_provider import PhedexProvider
from DataProvider.utils.utils import deepcopy

if sys.version_info < (2, 6):
    raise Exception("To run this tool please use python 2.6")

class GeneratorOptionParser:
    "Generator option parser"
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store",
            type="int", default=0, dest="verbose",
            help="verbose output")
        self.parser.add_option("--in", action="store", type="string",
            default="", dest="workflow",
            help="specify input workflow JSON file")
        self.parser.add_option("--out", action="store", type="string",
            default="", dest="output",
            help="specify output JSON file")
    def get_opt(self):
        "Returns parse list of options"
        return self.parser.parse_args()

def workflow(fin, fout, verbose=None):
    "LifeCycle workflow"

    initial_payload = None # initial payload, should be provided by LifeCycle
    new_payload = [] # newly created payloads will be returned by LifeCycle

    with open(fin, 'r') as source:
        initial_payload = cjson.decode(source.read())

    if  verbose:
        print "\n### input workflow"
        print pprint.pformat(initial_payload)

    ### read inputs from payload
    number_of_datasets = initial_payload['workflow']['NumberOfDatasets']
    number_of_blocks = initial_payload['workflow']['NumberOfBlocks']
    number_of_files = initial_payload['workflow']['NumberOfFiles']
    number_of_runs = initial_payload['workflow']['NumberOfRuns']
    number_of_lumis = initial_payload['workflow']['NumberOfLumis']

    ###read error rate from payload
    try:
        failure_rates = dict(PhedexSkipFileFail = initial_payload['workflow']['PhedexSkipFileFail'])
        failure_rates.update(dict(PhedexChangeCksumFail = initial_payload['workflow']['PhedexChangeCksumFail']))
        failure_rates.update(dict(PhedexChangeSizeFail = initial_payload['workflow']['PhedexChangeSizeFail']))
        failure_rates.update(dict(DBSSkipFileFail = initial_payload['workflow']['DBSSkipFileFail']))
        failure_rates.update(dict(DBSChangeCksumFail = initial_payload['workflow']['DBSChangeCksumFail']))
        failure_rates.update(dict(DBSChangeSizeFail = initial_payload['workflow']['PhedexChangeSizeFail']))
    except KeyError:
        failure_rates = None

    phedex_provider = PhedexProvider(failure_rates=failure_rates)
    dbs_provider = DBSProvider(failure_rates=failure_rates)

    for _ in xrange(number_of_datasets):
        #clone initial payload
        payload = deepcopy(initial_payload)
        phedex_provider.generate_dataset()
        phedex_provider.add_blocks(number_of_blocks)
        phedex_provider.add_files(number_of_files)
        payload['workflow']['Phedex'] = phedex_provider.dataset()
        payload['workflow']['DBS'] = dbs_provider.block_dump(number_of_runs,
                                                             number_of_lumis)
        phedex_provider.reset()
        new_payload.append(payload)

    with open(fout, 'w') as output:
        output.write(cjson.encode(new_payload))

    if  verbose:
        print "\n### output workflow"
        print pprint.pformat(new_payload)

def main():
    "Main function"
    optmgr  = GeneratorOptionParser()
    opts, _ = optmgr.get_opt()
    workflow(os.path.expanduser(opts.workflow), opts.output, opts.verbose)

if __name__ == '__main__':
    main()

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
import pprint
from optparse import OptionParser

# data provider modules
from DataProvider.core.dbs_provider import DBSProvider
from DataProvider.core.phedex_provider import PhedexProvider
from DataProvider.utils.utils import deepcopy
from DataProvider.utils.config import read_configparser
import DataProvider.utils.jsonwrapper as json

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
        initial_payload = json.load(source)

    if  verbose:
        print "\n### input workflow"
        print pprint.pformat(initial_payload)

    ### read inputs from payload
    cfg   = initial_payload['workflow']['DataProviderCfg']
    cdict = read_configparser(cfg)
    process_cfg = cdict['process']
    dbs_cfg = cdict['dbs']
    phedex_cfg = cdict['phedex']
    workflow = initial_payload['workflow']

    phedex_dbs_name = phedex_cfg.get('PhedexDBSName', workflow['PhedexDBSName'])
    number_of_datasets = process_cfg.get('NumberOfDatasets', workflow['NumberOfDatasets'])
    number_of_blocks = process_cfg.get('NumberOfBlocks', workflow['NumberOfBlocks'])
    number_of_files = process_cfg.get('NumberOfFiles', workflow['NumberOfFiles'])
    number_of_runs = process_cfg.get('NumberOfRuns', workflow['NumberOfRuns'])
    number_of_lumis = process_cfg.get('NumberOfLumis', workflow['NumberOfLumis'])

    phedex_file  = phedex_cfg.get('PhedexSkipFileFail', float(workflow['PhedexSkipFileFail']))
    phedex_cksum = phedex_cfg.get('PhedexChangeCksumFail', float(workflow['PhedexChangeCksumFail']))
    phedex_size  = phedex_cfg.get('PhedexChangeSizeFail', float(workflow['PhedexChangeSizeFail']))

    dbs_file  = phedex_cfg.get('DBSSkipFileFail', float(workflow['DBSSkipFileFail']))
    dbs_cksum = phedex_cfg.get('DBSChangeCksumFail', float(workflow['DBSChangeCksumFail']))
    dbs_size  = phedex_cfg.get('DBSChangeSizeFail', float(workflow['DBSChangeSizeFail']))

    try:
        failure_rates = dict(PhedexSkipFileFail=phedex_file)
        failure_rates = dict(PhedexChangeCksumFail=phedex_chksum)
        failure_rates = dict(PhedexChangeSizeFail=phedex_size)
        failure_rates = dict(DBSSkipFileFail=dbs_file)
        failure_rates = dict(DBSChangeCksumFail=dbs_chksum)
        failure_rates = dict(DBSChangeSizeFail=dbs_size)
    except KeyError:
        failure_rates = None

#    phedex_dbs_name = initial_payload['workflow']['PhedexDBSName']
#    number_of_datasets = initial_payload['workflow']['NumberOfDatasets']
#    number_of_blocks = initial_payload['workflow']['NumberOfBlocks']
#    number_of_files = initial_payload['workflow']['NumberOfFiles']
#    number_of_runs = initial_payload['workflow']['NumberOfRuns']
#    number_of_lumis = initial_payload['workflow']['NumberOfLumis']

    ###read error rate from payload
    ### cast to float necessary because perl input is interpreted as a string
#    try:
#        failure_rates = dict(PhedexSkipFileFail = float(initial_payload['workflow']['PhedexSkipFileFail']))
#        failure_rates.update(dict(PhedexChangeCksumFail = float(initial_payload['workflow']['PhedexChangeCksumFail'])))
#        failure_rates.update(dict(PhedexChangeSizeFail = float(initial_payload['workflow']['PhedexChangeSizeFail'])))
#        failure_rates.update(dict(DBSSkipFileFail = float(initial_payload['workflow']['DBSSkipFileFail'])))
#        failure_rates.update(dict(DBSChangeCksumFail = float(initial_payload['workflow']['DBSChangeCksumFail'])))
#        failure_rates.update(dict(DBSChangeSizeFail = float(initial_payload['workflow']['DBSChangeSizeFail'])))
#    except KeyError:
#        failure_rates = None

    phedex_provider = PhedexProvider(dbs_name=phedex_dbs_name, failure_rates=failure_rates)
    dbs_provider = DBSProvider(failure_rates=failure_rates)

    for _ in xrange(number_of_datasets):
        #clone initial payload
        payload = deepcopy(initial_payload)
        phedex_provider.generate_dataset()
        phedex_provider.add_blocks(number_of_blocks)
        phedex_provider.add_files(number_of_files)
        payload['workflow']['Phedex'] = [phedex_provider.dataset()]
        payload['workflow']['DBS'] = dbs_provider.block_dump(number_of_runs,
                                                             number_of_lumis)
        phedex_provider.reset()
        new_payload.append(payload)

    with open(fout, 'w') as output:
        output.write(json.dump(new_payload))

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

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
    workflow = initial_payload['workflow']
    # check if input are read from configuration file
    try:
        cfg   = workflow['DataProviderCfg']
    except KeyError:
        #No configuration, try to use values provided in the workflow
        #for backward compatibility
        #values using get are optional
        cdict = { 'process' :
                  {'NumberOfDatasets' : workflow['NumberOfDatasets'],
                   'NumberOfBlocks' : workflow['NumberOfBlocks'],
                   'NumberOfFiles' : workflow['NumberOfFiles'],
                   'NumberOfRuns' : workflow['NumberOfRuns'],
                   'NumberOfLumis' : workflow['NumberOfLumis']},
                  'dbs' :
                  {'DBSSkipFileFail': workflow.get('DBSSkipFileFail', None),
                   'DBSChangeCksumFail': workflow.get('DBSChangeCksumFail', None),
                   'DBSChangeSizeFail': workflow.get('DBSChangeSizeFail', None)},
                  'phedex' :
                  {'PhedexSkipFileFail' : workflow.get('PhedexSkipFileFail', None),
                   'PhedexChangeCksumFail' : workflow.get('PhedexChangeCksumFail', None),
                   'PhedexChangeSizeFail' : workflow.get('PhedexChangeSizeFail', None),
                   'PhedexDBSName' : workflow['PhedexDBSName']}
                  }
    else:
        cdict = read_configparser(cfg)

    process_cfg = cdict['process']
    dbs_cfg = cdict['dbs']
    phedex_cfg = cdict['phedex']

    phedex_dbs_name = phedex_cfg.get('PhedexDBSName')
    number_of_datasets = int(process_cfg.get('NumberOfDatasets'))
    number_of_blocks = int(process_cfg.get('NumberOfBlocks'))
    number_of_files = int(process_cfg.get('NumberOfFiles'))
    number_of_runs = int(process_cfg.get('NumberOfRuns'))
    number_of_lumis = int(process_cfg.get('NumberOfLumis'))

    try:
        phedex_file  = float(phedex_cfg.get('PhedexSkipFileFail'))
        phedex_cksum = float(phedex_cfg.get('PhedexChangeCksumFail'))
        phedex_size  = float(phedex_cfg.get('PhedexChangeSizeFail'))

        dbs_file  = float(dbs_cfg.get('DBSSkipFileFail'))
        dbs_cksum = float(dbs_cfg.get('DBSChangeCksumFail'))
        dbs_size  = float(dbs_cfg.get('DBSChangeSizeFail'))
    # if value is None, the cast will fail, which means no failures are used
    except TypeError:
        failure_rates = None
    else:
        failure_rates = dict(PhedexSkipFileFail=phedex_file)
        failure_rates.update(PhedexChangeCksumFail=phedex_cksum)
        failure_rates.update(PhedexChangeSizeFail=phedex_size)
        failure_rates.update(DBSSkipFileFail=dbs_file)
        failure_rates.update(DBSChangeCksumFail=dbs_cksum)
        failure_rates.update(DBSChangeSizeFail=dbs_size)
    print failure_rates
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

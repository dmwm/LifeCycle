#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
"""
File: dataprovider.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: Main tool which generates CMS meta-data
"""

# system modules
import sys
import inspect
import pprint
from optparse import OptionParser

# data provider modules
from DataProvider.core.data_provider import DataProvider
from DataProvider.core.dbs_provider import DBSDataProvider
from DataProvider.core.phedex_provider import PhedexDataProvider
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
        self.parser.add_option("--fixed", action="store_true",
            default=False, dest="fixed", help="use fixed seed")
        self.parser.add_option("--print", action="store_true",
            default=False, dest="toprint", help="print output on stdout")
        self.parser.add_option("--system", action="store", type="string",
            default="", dest="system",
            help="specify CMS system: dbs or phedex")
        msg  = "specify meta-data generator, system generators list"
        msg += " can be obtained with --system=<system> option"
        self.parser.add_option("--generate", action="store", type="string",
            default="", dest="generate", help=msg)
        self.parser.add_option("--in", action="store", type="string",
            default="", dest="input",
            help="specify input JSON file")
        self.parser.add_option("--out", action="store", type="string",
            default="", dest="output",
            help="specify output JSON file")
        self.parser.add_option("--number", action="store", type="int",
            default=1, dest="number",
            help="specify number of entities to generate/add")
        self.parser.add_option("--prim", action="store", type="string",
            default="", dest="prim",
            help="specify primary dataset name")
        self.parser.add_option("--proc", action="store", type="string",
            default="", dest="proc",
            help="specify processed dataset name")
        self.parser.add_option("--tier", action="store", type="string",
            default="", dest="tier",
            help="specify data-tier name")
        msg  = 'specify action to be applied for given input.\n'
        msg += 'Supported actions can be obtained with --system=<system> option'
        self.parser.add_option("--action", action="store", type="string",
            default="", dest="action", help=msg)
        self.parser.add_option("--runs", action="store",
            type="int", default=5, dest="runs",
            help="number of runs per file, default is 5")
        self.parser.add_option("--lumis", action="store",
            type="int", default=5, dest="lumis",
            help="number of lumis per run, default is 5")
    def get_opt(self):
        "Returns parse list of options"
        return self.parser.parse_args()

def main():
    "Main function"
    optmgr = GeneratorOptionParser()
    opts, _args = optmgr.get_opt()

    if  opts.system == 'dbs':
        mgr = DBSDataProvider(opts.fixed, opts.runs, opts.lumis)
    elif opts.system == 'phedex':
        mgr = PhedexDataProvider(opts.fixed)
    else:
        mgr = DataProvider(opts.fixed)
    if  opts.system and not opts.action and not opts.generate:
        members = [n for n, _ in inspect.getmembers(mgr) if n[0] != '_']
        actions = [m for m in members if m.find('gen_') != -1 or m.find('add_') != -1]
        generators = set(members) - set(actions)
        print opts.system, 'actions    :', ', '.join(actions)
        print opts.system, 'generators :', ', '.join(generators)
        sys.exit(0)
    number  = opts.number   # number of entities to generate/add
    infile  = opts.input    # input JSON file
    action  = opts.action   # action to apply, e.g. add_blocks
    what    = opts.generate # method to gererate, e.g. datasets
    outdata = []            # output data going to output JSON
    attrs   = {}            # additional attributes
    if  opts.prim:
        attrs.update({'prim': opts.prim})
    if  opts.proc:
        attrs.update({'proc': opts.proc})
    if  opts.tier:
        attrs.update({'tier': opts.tier})
    if  infile and what:
        msg  = 'You cannot mix --generate and --input options, '
        msg += 'they are exclusive'
        print msg
        sys.exit(1)
    if  what:
        outdata = getattr(mgr, what)(number, **attrs)
    if  infile:
        if  not action:
            msg = 'Please provide action to use'
            print msg
            sys.exit(1)
        with open(infile, 'r') as data_file:
            indata  = json.load(data_file)
            for row in indata:
                res = getattr(mgr, action)(row, number)
                if  isinstance(res, list):
                    outdata += res
                else:
                    outdata.append(res)
    outfile = what + '.json' if what else action + '.json'
    outfile = outfile.replace('add_', '').replace('gen_', '')
    if  outdata:
        fname = opts.output if opts.output else outfile
        if  infile and infile == fname:
            print "Input and output file names are identical, exit 1"
            sys.exit(1)
        with open(fname, 'w') as json_file:
            json_file.write(json.dumps(outdata))
    if  opts.toprint:
        pprint.pprint(outdata)
#
# Main
#
if __name__ == '__main__':
    main()

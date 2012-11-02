#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable-msg=
"""
File       : config.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: Configuration module for DataProvider
"""

# system modules
import os
import ConfigParser

def read_configparser(fname):
    "Read configuration"
    configdict = {
        'process':{ 'NumberOfDatasets':1,
                    'NumberOfBlocks':2,
                    'NumberOfFiles':10,
                    'NumberOfRuns':5,
                    'NumberOfLumis':5},
        'dbs':{ 'DBSSkipFileFail':0.01,
                'DBSChangeCksumFail':0.01,
                'DBSChangeSizeFail':0.01,
                'DBSInstance':'dev'},
        'phedex': { 'PhedexSkipFileFail':0.01,
                    'PhedexChangeCksumFail':0.01,
                    'PhedexChangeSizeFail':0.01,
                    'PhedexDBSName':'global'},
        }
    if  not os.path.isfile(fname):
        return configdict
    config = ConfigParser.ConfigParser()
    config.read(fname)
    for section in configdict.keys():
        for key, default in configdict[section].items():
            configdict[section][key] = config.get(section, key, default)
    return configdict

def test():
    "test function"
    fname = os.path.join(os.getcwd(), 'etc/dataprovider.cfg')
    print read_configparser(fname)

if __name__ == '__main__':
    test()

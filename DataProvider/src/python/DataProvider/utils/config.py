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
                    'NumberOfBlocks':1,
                    'NumberOfFiles':1,
                    'NumberOfRuns':1,
                    'NumberOfLumis':1},
        'dbs':{ 'DBSSkipFileFail':0,
                'DBSChangeCksumFail':0,
                'DBSChangeSizeFail':0},
        'phedex': { 'PhedexSkipFileFail':0,
                    'PhedexChangeCksumFail':0,
                    'PhedexChangeSizeFail':0}
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

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
from ConfigParser import ConfigParser, NoOptionError

def read_configparser(fname):
    "Read configuration"
    configdict = {
        'process':{ 'NumberOfDatasets':1,
                    'NumberOfBlocks':2,
                    'NumberOfFiles':10,
                    'NumberOfRuns':5,
                    'NumberOfLumis':5},
        'dbs':{ 'DBSSkipFileFail':None,
                'DBSChangeCksumFail':None,
                'DBSChangeSizeFail':None},
        'phedex': { 'PhedexSkipFileFail':None,
                    'PhedexChangeCksumFail':None,
                    'PhedexChangeSizeFail':None,
                    'PhedexDBSName':'global'},
        }
    if  not os.path.isfile(fname):
        return configdict
    config = ConfigParser()
    config.read(fname)
    for section in configdict.keys():
        for key, default in configdict[section].items():
            try:
                configdict[section][key] = config.get(section, key)
            except NoOptionError:
                ### use default value from dict above
                pass
    return configdict

def test():
    "test function"
    fname = os.path.join(os.getcwd(), 'etc/dataprovider.cfg')
    print read_configparser(fname)

if __name__ == '__main__':
    test()

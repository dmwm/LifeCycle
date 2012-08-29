#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
#pylint: disable-msg=W0201,R0902
"""
File: phedex_provider.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: Phedex data-provider
"""

# system modules
import random

# package modules
from DataProvider.core.data_provider import DataProvider
from DataProvider.core.data_provider import BaseProvider
from DataProvider.core.data_provider import generate_uid, generate_block_uid
from DataProvider.utils.utils import deepcopy

class PhedexProvider(BaseProvider):
    "Phedex data provider class with persistent storage"
    def __init__(self):
        super(PhedexProvider, self).__init__()

    def dataset(self):
        "return dataset object"
        if not hasattr(self, '_dataset'):
            self.generate_dataset()

        data = deepcopy(self._dataset)
        data.update({'is-open': self.dataset_is_open})

        for block in data['blocks']:
            for this_file in block['block']['files']:
                #update file information
                this_file['file'].update({"checksum": "cksum:6551,adler32:5040",
                                          "bytes": 1703432715})

            #update block information
            size = sum([f['file']['bytes'] for f in block['block']['files']])
            block['block'].update({"nfiles": len(block['block']['files']),
                                   "is-open": self.block_is_open,
                                   "size": size})

            return dict(dataset=data)

    @property
    def dataset_is_open(self):
        "return dataset_is_open flag"
        if not hasattr(self, "_dataset_is_open"):
            self._dataset_is_open = 'y'
        return self._dataset_is_open

    @property
    def dbs_name(self):
        "return dbs name"
        if not hasattr(self, "_dbs_name"):
            self._dbs_name = 'global'
        return self._dbs_name

class PhedexDataProvider(DataProvider):
    "PhedexDataProvider"
    def __init__(self, fixed=False):
        DataProvider.__init__(self, fixed)

    def datasets(self, number, **attrs):
        "Generate Phedex datasets meta-data"
        output = super(PhedexDataProvider, self).datasets(number, **attrs)
        name = 'global'
        for row in output:
            doc = {'is-open': 'y', 'dbs_name': name, 'blocks':[]}
            row['dataset'].update(doc)
        return output

    def blocks(self, number, **attrs):
        "Generate Phedex block meta-data"
        output = super(PhedexDataProvider, self).blocks(number, **attrs)
        for row in output:
            doc = {'is-open': 'y', 'files':[]}
            row['block'].update(doc)
        return output

    def files(self, number, **attrs):
        "Generate Phedex file meta-data"
        prim = attrs.get('prim', 'prim')
        proc = attrs.get('proc', 'proc')
        tier = attrs.get('tier', 'tier')
        output = super(PhedexDataProvider, self).files(number)
        # /store/data/acq_era/prim_dataset/data_tier/proc_version/lfn_counter/f.root
        idx = 0
        gbyte = 1024*1024*1024
        for row in output:
            era, proc_ds_name, ver = proc.split('-')
            ver = ver[1:] #remove v from v4711
            counter = str(idx).zfill(9)
            prefix = '/store/data/%s/%s/%s/%s/%s/' % (era, prim, tier, ver, counter)
            name = prefix + row['file']['name']
            checksum = 'cksum:%s,adler32:%s' \
                    % (generate_uid(4, '1234567890'), \
                        generate_uid(4, '1234567890'))
            size = random.randint(1*gbyte, 2*gbyte)
            doc = {'checksum': checksum, 'bytes': size, 'name': name}
            row['file'].update(doc)
        return output

    def add_blocks(self, input_dataset, number_of_blocks=1):
        "Add blocks to a given dataset"
        dataset = deepcopy(input_dataset)
        name = dataset['dataset']['name']
        res  = self.blocks(number_of_blocks)
        for row in res:
            buid = generate_block_uid()
            row['block']['name'] = '%s#%s' % (name, buid)
        if  dataset['dataset']['is-open'] == 'y':
            blocks = dataset['dataset']['blocks']
            blocks += res
        return dataset

    def add_files(self, input_dataset, number_of_files=1):
        "Add files to a given dataset"
        dataset = deepcopy(input_dataset)
        for block in dataset['dataset']['blocks']:
            if  block['block']['is-open'] != 'y':
                continue
            block_name = block['block']['name']
            _, prim, proc, tier = block_name.split('#')[0].split('/')
            attrs = {'prim':prim, 'proc':proc, 'tier':tier}
            res  = self.files(number_of_files, **attrs)
            size = 0
            for row in res:
                size += row['file']['bytes']
            if  block['block']['files']:
                block['block']['files'] += res
                block['block']['size'] += size
                block['block']['nfiles'] += len(res)
            else:
                block['block']['files'] = res
                block['block']['size'] = size
                block['block']['nfiles'] = len(res)
        return dataset

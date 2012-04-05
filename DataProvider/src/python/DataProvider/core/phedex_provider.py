#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: phedex_provider.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: Phedex data-provider
"""

# system modules
import random

# package modules
from DataProvider.core.data_provider import DataProvider
from DataProvider.core.data_provider import generate_uid, generate_block_uid
from DataProvider.utils.utils import deepcopy

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
            ver = '%s-v1' % proc
            counter = str(idx).zfill(9)
            prefix = '/store/data/era/%s/%s/%s/%s/' % (prim, tier, ver, counter)
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

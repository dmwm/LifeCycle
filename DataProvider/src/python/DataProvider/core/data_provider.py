#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
"""
File: DataProvider.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: Main class DataProvider is a core meta-data generator.
It has basic APIs: datasets, blocks, files, runs, lumis. Those
can be overwritten by high level classes to adopt to concrete
use cases, e.g. PhedexDataProvider. The generated meta-data
has basic attributes, e.g. size.
"""

# system modules
import random

def generate_uid(base, seed='qwertyuiopasdfghjklzxcvbnm1234567890', fixed=False):
    "Generate UID for given base"
    if  fixed:
        while base > len(seed):
            seed += seed
    uid  = ''
    for idx in range(0, base):
        if  fixed:
            uid += seed[idx]
        else:
            uid += random.choice(seed)
    return uid

def generate_block_uid():
    "Generate block UID, example 020c640c-c0f7-11e0-8b63-00221959e72f"
    uid = generate_uid(8) + '-' + generate_uid(4) + '-' \
        + generate_uid(4) + '-' + generate_uid(4) + '-' + generate_uid(12)
    return uid

class DataProvider(object):
    "DataProvider class generates CMS meta-data"

    def __init__(self, fixed=False):
        self._seed  = 'qwertyuiopasdfghjklzxcvbnm'
        self._tiers = ['RAW', 'GEN', 'SIM', 'RECO', 'AOD']
        self._fixed = fixed

    def datasets(self, number, **attrs):
        "Generate datasets"
        output = []
        for _ in range(0, number):
            prim = attrs.get('prim', generate_uid(3, self._seed, self._fixed))
            proc = attrs.get('proc', generate_uid(3, self._seed, self._fixed))
            tier = attrs.get('tier', generate_uid(1, self._tiers, self._fixed))
            name = '/%s/%s/%s' % (prim, proc, tier)
            doc  = dict(name=name)
            for key, val in attrs.items():
                if  key in ['prim', 'proc', 'tier']:
                    continue
                doc.update({key:val})
            output.append(dict(dataset=doc))
        return output

    def blocks(self, number, **attrs):
        "Generate blocks"
        output = []
        for _ in range(0, number):
            prim = attrs.get('prim', generate_uid(3, self._seed, self._fixed))
            proc = attrs.get('proc', generate_uid(3, self._seed, self._fixed))
            tier = attrs.get('tier', generate_uid(1, self._tiers, self._fixed))
            name = '/%s/%s/%s' % (prim, proc, tier)
            uid  = generate_block_uid()
            doc  = dict(name = "%s#%s" % (name, uid))
            for key, val in attrs.items():
                if  key in ['prim', 'proc', 'tier']:
                    continue
                doc.update({key:val})
            output.append(dict(block=doc))
        return output

    def files(self, number, **attrs):
        "Generate files"
        output = []
        for _ in range(0, number):
            name = '%s.root' % generate_uid(5, self._seed, self._fixed)
            doc  = dict(name = name)
            for key, val in attrs.items():
                doc.update({key:val})
            output.append(dict(file=doc))
        return output

    def runs(self, number, **attrs):
        "Generate runs"
        output = []
        for _ in range(0, number):
            run_number = int( '1' + generate_uid(5, '1234567890', self._fixed))
            doc  = dict(number = run_number)
            for key, val in attrs.items():
                doc.update({key:val})
            output.append(dict(block=doc))
        return output

    def lumis(self, number, **attrs):
        "Generate lumis"
        output = []
        for _ in range(0, number):
            run_number = int( '1' + generate_uid(5, '1234567890', self._fixed))
            lumi_number = random.randint(1, 100)
            doc  = dict(number = lumi_number, run_number = run_number)
            for key, val in attrs.items():
                doc.update({key:val})
            output.append(dict(lumi=doc))
        return output

#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
#pylint: disable-msg=W0201,R0902
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

def generate_uid(base, seed='qwertyuiopasdfghjklzxcvbnm1234567890',
                    fixed=False):
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

class BaseProvider(object):
    "BaseProvider class generates and holds CMS meta-data"
    _shared_information = {}
    _preserve_information = ('_seed', '_tiers', '_fixed')

    def __new__(cls, *args, **kwds):
        inst = object.__new__(cls, *args, **kwds)
        inst.__dict__ = cls._shared_information
        return inst

    @classmethod
    def reset(cls):
        "Reset internal data structures"
        for key in cls._shared_information.keys():
            if key not in cls._preserve_information:
                del cls._shared_information[key]

    def __init__(self, fixed=False):
        "constructor"
        self._seed  = 'qwertyuiopasdfghjklzxcvbnm'
        self._tiers = ['RAW', 'GEN', 'SIM', 'RECO', 'AOD']
        self._fixed = fixed

    def generate_dataset(self):
        "generate dataset object"
        self._dataset = {'blocks': [],
                         'name': self.dataset_name}

    def add_blocks(self, number_of_blocks):
        "add blocks to existing dataset object"
        for _ in xrange(number_of_blocks):
            block = {'block': {'files': [], 'name': self.block_name}}
            self._dataset['blocks'].append(block)

    def add_files(self, number_of_files):
        "add files to existing block object"
        for block in self._dataset["blocks"]:
            for _ in xrange(number_of_files):
                filedict = {'file': {'name': self.file_name}}
                block['block']['files'].append(filedict)

    @property
    def block_name(self):
        "return block name"
        return '/%s/%s/%s#%s' % (self.primary_dataset_name,
                                 self.processed_dataset,
                                 self.tier,
                                 generate_block_uid())

    @property
    def block_is_open(self):
        "return block_is_open flag"
        if not hasattr(self, "_block_is_open"):
            self._block_is_open = 'y'
        return self._block_is_open

    @property
    def dataset_name(self):
        "return dataset name"
        if not hasattr(self, "_dataset_name"):
            self._dataset_name = '/%s/%s/%s' % \
                    (self.primary_dataset_name,
                     self.processed_dataset,
                     self.tier)
        return self._dataset_name

    @property
    def file_name(self):
        "return file name"
        counter = str(0).zfill(9)
        return '/store/data/%s/%s/%s/%s/%s/%s.root' % \
            (self.acquisition_era_name,
             self.primary_dataset_name,
             self.tier,
             self.processing_version,
             counter,
             generate_uid(5, self._seed, self._fixed))

    @property
    def primary_dataset_name(self):
        "return primary dataset name"
        if not hasattr(self, '_primary_dataset_name'):
            self._primary_dataset_name = \
                generate_uid(3, self._seed, self._fixed)
        return self._primary_dataset_name

    @property
    def processed_dataset(self):
        "return processed dataset path"
        if not hasattr(self, '_processed_dataset'):
            self._processed_dataset = '%s-%s-v%s' % \
                (self.acquisition_era_name,
                self.processed_dataset_name,
                self.processing_version)
        return self._processed_dataset

    @property
    def acquisition_era_name(self):
        "return acquisition era name"
        if not hasattr(self, '_acquisition_era_name'):
            self._acquisition_era_name = 'pms'
        return self._acquisition_era_name

    @property
    def processed_dataset_name(self):
        "return processed dataset name"
        if not hasattr(self, '_processed_dataset_name'):
            self._processed_dataset_name = \
                generate_uid(3, self._seed, self._fixed)
        return self._processed_dataset_name

    @property
    def processing_version(self):
        "return processing version"
        if not hasattr(self, '_processing_version'):
            self._processing_version = '63'
        return self._processing_version

    @property
    def tier(self):
        "return tier name"
        if not hasattr(self, '_tier'):
            self._tier = generate_uid(1, self._tiers, self._fixed)
        return self._tier

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
            proc = attrs.get('proc', "%s-%s-v%i" % \
                    (generate_uid(3, self._seed, self._fixed),
                        generate_uid(3, self._seed, self._fixed),
                        random.randint(1, 100)))
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

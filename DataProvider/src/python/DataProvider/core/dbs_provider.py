#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
"""
File: dbs_provider.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: DBS meta-data provider
"""

# system modules
import time
import random

# package modules
from DataProvider.core.data_provider import DataProvider
from DataProvider.core.data_provider import generate_block_uid, generate_uid
from DataProvider.utils.utils import deepcopy

class DBSDataProvider(DataProvider):
    "DBSDataProvider"
    def __init__(self, fixed=False, runs=5, lumis=5):
        DataProvider.__init__(self, fixed)
        self.runs_per_file = runs
        self.lumis_per_run = lumis

    def prim_ds(self, number, **attrs):
        "Generate DBS primary dataset meta-data"
        output = []
        for _ in range(0, number):
            prim = attrs.get('prim', generate_uid(3, self._seed, self._fixed))
            data_type = generate_uid(1, ['mc', 'data'], self._fixed)
            rec = dict(prim_ds=dict(primary_ds_name=prim, primary_ds_type=data_type))
            output.append(rec)
        return output

    def proc_eras(self, number, **attrs):
        "Generate DBS processing era meta-data"
        output = []
        desc = 'Test_proc_era'
        for _ in range(0, number):
            ver  = int(generate_uid(4, '123456789', self._fixed))
            rec  = dict(processing_era=dict(processing_version=ver, description=desc))
            output.append(rec)
        return output

    def acq_eras(self, number, **attrs):
        "Generate DBS acquisition era meta-data"
        output = []
        desc = 'Test_acquisition_era'
        for _ in range(0, number):
            ver  = generate_uid(4, self._seed, self._fixed)
            rec  = dict(acquisition_era=dict(acquisition_era_name=ver, description=desc))
            output.append(rec)
        return output

    def tiers(self, number, **attrs):
        "Generate DBS data tier meta-data"
        output = []
        for _ in range(0, number):
            tier = attrs.get('tier', generate_uid(1, self._tiers, self._fixed))
            rec = dict(tier=dict(data_tier_name=tier))
            output.append(rec)
        return output

    def configs(self, number, **attrs):
        "Generate DBS output config meta-data"
        output = []
        app = 'cmsRun'
        rel = 'CMSSW_1_2_3'
        tag = 'TAG'
        lab = 'Ouput_module_label'
        for _ in range(0, number):
            phash = generate_uid(32)
            rec  = dict(configs=dict(release_version=rel, pset_hash=phash, app_name=app,
                        output_module_label=lab, global_tag=tag))
            output.append(rec)
        return output

    def prim_proc_acq_tier_config(self, number, **attrs):
        "Generate DBS prim/proc/acq/tier/output config meta-data in one step"
        output = []
        if  number != 1:
            msg = "This action prohibits generation of prim_proc_acq_tier_config in one step"
            raise Exception(msg)
        for _ in range(0, number):
            doc = {}
            doc.update(self.prim_ds(1)[0])
            doc.update(self.proc_eras(1)[0])
            doc.update(self.acq_eras(1)[0])
            doc.update(self.tiers(1)[0])
            doc.update(self.configs(1)[0])
            output.append(doc)
        return output

    def datasets(self, number, **attrs):
        "Generate DBS datasets meta-data"
        output = super(DBSDataProvider, self).datasets(number, **attrs)
        for row in output:
            name = row['dataset']['name']
            proc_ver = row['dataset'].get('processing_version', 123)
            acq_era = row['dataset'].get('acquisition_era_name', 'test')
            prim_type = row['dataset'].get('primary_ds_type', 'mc')
            _, prim, proc, tier = name.split('/')
            group = generate_uid(1, ['Top', 'QCD', 'RelVal'], self._fixed)
            def_config = [{'release_version':'CMSSW_TEST',
                           'pset_hash':'NO_PSET_HASH', 'global_tag':'TAG',
                           'app_name':'Generator', 'output_module_label':'TEST'}]
            oconfig = row['dataset'].get('output_configs', def_config)
            doc = {'primary_ds_name': prim, 'processing_ds_name': proc,
                'data_tier_name': tier, 'physics_group_name': group,
                'acquisition_era_name': acq_era, 'processing_version': proc_ver,
                'xtcrosssection': 0.1, 'output_configs':oconfig,
                'primary_ds_type':'mc', 'dataset_access_type': 'valid',
                'prep_id':1, 'dataset': name}
            row['dataset'].update(doc)
            del row['dataset']['name']
        return output

    def blocks(self, number, **attrs):
        "Generate DBS blocks meta-data"
        output = super(DBSDataProvider, self).blocks(number, **attrs)
        for row in output:
            doc = {'block_name': row['block']['name'],
                'original_site_name': 'UNKNOWN'}
            del row['block']['name']
            row['block'].update(doc)
        return output

    def block_dump(self, number_of_files=1):
        "Generate block with multiple files in it"

        # generate dataset configuration info
        rel   = 'CMSSW_1_2_3'
        app   = 'cmsRun'
        tag   = 'TAG'
        label = 'Merge'
        phash = generate_uid(32)
        info  = dict(release_version=rel, pset_hash=phash, app_name=app,
                        output_module_label=label, global_tag=tag)

        # generate prim/proc/era
        prim = self.prim_ds(1)[0]
        proc_era = self.proc_eras(1)[0]
        acq_era = self.acq_eras(1)[0]
        tier = self.tiers(1)[0]

        # generate datasets
        proc = 'proc-%s' % proc_era['processing_era']['processing_version']
        attrs = {'prim':prim['prim_ds']['primary_ds_name'],
                 'processing_version':proc_era['processing_era']['processing_version'],
                 'acquisition_era_name': acq_era['acquisition_era']['acquisition_era_name'],
                 'proc': proc,
                 'tier':tier['tier']['data_tier_name']}
        dataset = self.datasets(1, **attrs)[0]

        # generate blocks
        block = self.blocks(1)[0]

        # generate files
        files = self.files(number_of_files)

        # generate file config info
        file_info = []
        for lfn in files:
            doc = dict(info)
            doc.update({'lfn':lfn['file']['logical_file_name']})
            file_info.append(doc)
        rec = dict(dataset_conf_list=[info], file_conf_list=file_info,
                dataset=dataset['dataset'], block=block['block'],
                primds=prim['prim_ds'], processing_era=proc_era['processing_era'],
                acquisition_era=acq_era['acquisition_era'], files=files)
        return dict(blockDump=rec)

    def file_lumi_list(self):
        "Generate file lumi list"
        output = []
        for _ in range(0, self.runs_per_file):
            run_num  = int('1' + generate_uid(5, '1234567890', self._fixed))
            for _ in range(0, self.lumis_per_run):
                lumi_num = random.randint(1, 100)
                row = dict(run_num=run_num, lumi_section_num=lumi_num)
                output.append(row)
        return output

    def files(self, number, **attrs):
        "Generate DBS files meta-data"
        prim = attrs.get('prim', 'prim')
        proc = attrs.get('proc', 'proc')
        tier = attrs.get('tier', 'tier')
        for key in ['prim', 'proc', 'tier']:
            if  attrs.has_key(key):
                del attrs[key]
        path = '/%s/%s/%s' % (prim, proc, tier)
        output = super(DBSDataProvider, self).files(number, **attrs)
        oconfig = {'release_version':'CMSSW_TEST', 'pset_hash':'NO_PSET_HASH',
                   'app_name':'Generator', 'output_module_label':'TEST',
                   'global_tag':'TAG'}
        # /store/data/acq_era/prim_dataset/data_tier/proc_version/lfn_counter/f.root
        idx = 0
        for row in output:
            ver = '%s-v1' % proc
            counter = str(idx).zfill(9)
            prefix = '/store/data/era/%s/%s/%s/%s/' % (prim, tier, ver, counter)
            name = prefix + row['file']['name']
            size = random.randint(1000, 1000000)
            ftype = generate_uid(1, ['EDM', 'ROOT'], self._fixed)
            doc = {'logical_file_name': name,
                'file_size': size, 'file_type': ftype,
                'check_sum': generate_uid(8), 'adler32': generate_uid(8),
                'file_output_config_list': [oconfig],
                'file_lumi_list': self.file_lumi_list(),
                'file_parent_list': [], 'auto_cross_section': 0.0,
                'event_count': random.randint(10, 10000),
                'dataset': path,
                'file_type_id': 1, 'md5':'NOTSET'}
            row['file'].update(doc)
            for att in ['name']:
                del row['file'][att]
            idx += 1
        return output

    def gen_blocks(self, dataset, number_of_blocks=1):
        "Generate blocks for a given dataset"
        if  not isinstance(dataset, dict) or \
            not dataset.has_key('dataset') or \
            not dataset['dataset'].has_key('dataset'):
            msg = 'To generate blocks please provide valid dataset record/JSON file'
            raise Exception(msg)
        name = dataset['dataset']['dataset']
        buid = generate_block_uid()
        res  = self.blocks(number_of_blocks)
        return res

    def gen_files(self, block, number_of_files=1):
        "Generate files for a given block"
        if  not isinstance(block, dict) or \
            not block.has_key('block') or \
            not block['block'].has_key('block_name'):
            msg = 'To generate files please provide valid block record/JSON file'
            raise Exception(msg)
        block_name = block['block']['block_name']
        _, prim, proc, tier = block_name.split('#')[0].split('/')
        attrs = {'prim':prim, 'proc':proc, 'tier':tier}
        res  = self.files(number_of_files, **attrs)
        for row in res:
            row['file']['dataset'] = block_name.split('#')[0]
            row['file']['block_name'] = block_name
        return res

    def gen_runs(self, file_record, number_of_runs=1):
        "Generate run/lumis for a given file record"
        if  not isinstance(file_record, dict) or \
            not file_record.has_key('file') or \
            not file_record['file'].has_key('logical_file_name'):
            msg = 'To generate run/lumis please provide valid file record/JSON file'
            raise Exception(msg)
        row = deepcopy(file_record)
        records = []
        for idx in range(0, number_of_runs):
            run  = random.randint(100000, 200000)
            for _ in range(0, random.randint(1, 10)):
                lumi = random.randint(1, 100)
                rec = {'run_num': run, 'lumi_section_num': lumi}
                records.append(rec)
        row['file']['file_lumi_list'] = records
        return row

    def add_datasets(self, input_prim_proc_acq_tier_config, number=1):
        "Add blocks to a given primary dataset"
        idict    = deepcopy(input_prim_proc_acq_tier_config)
        prim_val = idict['prim_ds']
        proc_ver = idict['processing_era']
        acq_era  = idict['acquisition_era']
        tier     = idict['tier']
        config   = idict['configs']
        func     = lambda x: isinstance(x,dict) and [x] or x
        prim_val = func(prim_val)
        proc_ver = func(proc_ver)
        acq_era  = func(acq_era)
        tier     = func(tier)
        config   = func(config)
        output   = []
        for item_prim, item_proc, item_acq, item_tier, item_config \
        in zip(prim_val, proc_ver, acq_era, tier, config):
            prim = item_prim['primary_ds_name']
            acq  = item_acq['acquisition_era_name']
            tier = item_tier['data_tier_name']
            proc_ver = item_proc['processing_version']
            attrs = {'prim':prim,
                     'processing_version':proc_ver,
                     'acquisition_era_name':acq,
                     'tier':tier,
                     'output_configs':item_config}
            res  = self.datasets(number, **attrs)
            for row in res:
                output.append(row['dataset'])
        idict['dataset'] = output
        return idict

    def add_blocks(self, input_dataset, number=1):
        "Add blocks to a given dataset"
        idict = deepcopy(input_dataset)
        datasets = idict['dataset']
        if  isinstance(datasets, dict):
            datasets = [datasets]
        output = []
        for item in datasets:
            _, prim, proc, tier = item['dataset'].split('/')
            attrs = {'prim':prim, 'proc':proc, 'tier':tier}
            res   = self.blocks(number, **attrs)
            for row in res:
                output.append(row['block'])
        idict['block'] = output
        return idict

    def add_files(self, input_dataset, number_of_files=1):
        "Add files to a given dataset"
        record  = deepcopy(input_dataset)
        block   = record['block']
        if  not isinstance(block, list):
            block = [block]
        for rec in block:
            _, prim, proc, tier = rec['block_name'].split('#')[0].split('/')
            attrs = {'prim':prim, 'proc':proc, 'tier':tier, 'block_name':rec['block_name']}
            res  = self.files(number_of_files, **attrs)
        return res

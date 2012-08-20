#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
"""
File: mgr_t.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: unit tests for DBS DataProvider
"""

# system modules
import json
import time
import unittest

# data provider modules
from core.dbs_provider import DBSDataProvider

class testUtils(unittest.TestCase):
    "test class for the DataProvider module"
    def setUp(self):
        self.mgr = DBSDataProvider()
        self.number = 2

    def test_datasets(self):
        attrs = {}
        datasets = self.mgr.datasets(self.number, **attrs)
        print "\ndatasets:\n", datasets
        self.assertEqual(self.number, len(datasets))

        for row in datasets:
            res = self.mgr.gen_blocks(row, 2)
            print "\nblocks:\n", res

    def test_blocks(self):
        attrs = {}
        blocks = self.mgr.blocks(self.number, **attrs)
        print "\nblocks:\n", blocks
        self.assertEqual(self.number, len(blocks))

    def test_files(self):
        files = self.mgr.files(self.number)
        print "\nfiles:\n", files
        self.assertEqual(self.number, len(files))

if __name__ == '__main__':
    unittest.main()

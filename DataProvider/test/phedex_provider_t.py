#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: mgr_t.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: unit tests for Phedex DataProvider
"""

# system modules
import json
import time
import unittest

# data provider modules
from core.phedex_provider import PhedexDataProvider

class testUtils(unittest.TestCase):
    "test class for the DataProvider module"
    def setUp(self):
        self.mgr = PhedexDataProvider()
        self.number = 2

    def test_datasets(self):
        attrs = {}
        datasets = self.mgr.datasets(self.number, **attrs)
        self.assertEqual(self.number, len(datasets))

        for row in datasets:
            print "\ndataset:", row
            res = self.mgr.add_blocks(row, 2)
            print "\ndatasets with blocks:\n", res

    def test_blocks(self):
        attrs = {}
        blocks = self.mgr.blocks(self.number, **attrs)
        self.assertEqual(self.number, len(blocks))

        for row in blocks:
            print "\nblock:", row
            res = self.mgr.add_files(row, 2)
            print "\nblocks with files:\n", blocks

    def test_files(self):
        files = self.mgr.files(self.number)
        print "\nfiles:\n", files
        self.assertEqual(self.number, len(files))

if __name__ == '__main__':
    unittest.main()

#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: data_provider_t.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: unit test for DataProvider
"""

# system modules
import json
import time
import unittest

# data provider modules
from core.data_provider import DataProvider

class testUtils(unittest.TestCase):
    "test class for the DataProvider module"
    def setUp(self):
        self.data_provider = DataProvider()
        self.number = 2

    def test_datasets(self):
        attrs = dict(size=1, files=2)
        datasets = self.data_provider.datasets(self.number, **attrs)
        print "\ndatasets:\n", datasets
        self.assertEqual(self.number, len(datasets))

    def test_blocks(self):
        attrs = {'is-open':'y'}
        blocks = self.data_provider.blocks(self.number, **attrs)
        print "\nblocks:\n", blocks
        self.assertEqual(self.number, len(blocks))

    def test_files(self):
        attrs = dict(adler32='abcd')
        files = self.data_provider.files(self.number, **attrs)
        print "\nfiles:\n", files
        self.assertEqual(self.number, len(files))

    def test_lumis(self):
        attrs = {}
        lumis = self.data_provider.lumis(self.number, **attrs)
        print "\nlumis:\n", lumis
        self.assertEqual(self.number, len(lumis))

if __name__ == '__main__':
    unittest.main()

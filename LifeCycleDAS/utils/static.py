#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : static.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: static module contains list of static variables
"""

# local modules
from utils.url_utils import get_key_cert

DAS_CLIENT = 'LifeCycleDASAgent'
CKEY, CERT = get_key_cert()

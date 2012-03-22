#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: utils.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: Set of common utilities used by this package
"""

# system modules
import copy

def deepcopy(obj):
    """Perform full copy of given object into new object"""
    if  isinstance(obj, dict):
        newobj = {}
        for key, val in obj.iteritems():
            if  isinstance(val, dict):
                newobj[key] = deepcopy(val)
            elif isinstance(val, list):
                newobj[key] = list(val)
            else:
                newobj[key] = val
    else:
        newobj = copy.deepcopy(obj)
    return newobj

#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable-msg=
"""
File       : options.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: option module
"""

# system modules
import sys
import getopt

def options():
    "Load options"
    try:
        opts, args = getopt.getopt(sys.argv[1:], "io", ["in=", "out="])
    except getopt.GetoptError, err:
        print "Error parsing arguments"
        sys.exit(2)
    out = 'none'
    inp  = 'none'
    for opt, val in opts:
        if  opt == "--out":
            out = val
        elif opt == "--in":
            inp = val
        else:
            assert False, "unhandled option"
    return inp, out

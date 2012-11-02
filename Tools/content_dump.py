#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File       : content_dump.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: Dump content of given LifeCycle JSON file
"""

# system modules
import os
import sys
import json
import pprint

def main(fname, system=None):
    "Main function"
    with open(fname, 'r') as stream:
        jsondict = json.load(stream)
    if  system:
        pprint.pprint(jsondict['workflow'][system])
    else:
        pprint.pprint(jsondict)

if __name__ == '__main__':
    msg = 'Usage: content_dump.py <JSON file> <system>'
    if  len(sys.argv) == 2:
        main(sys.argv[1])
    elif  len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    else:
        print msg
        sys.exit(1)

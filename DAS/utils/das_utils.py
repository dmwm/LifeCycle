#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : das_utils.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: 
"""

# system modules
import re
import json
import time
import random
import urllib
import urllib2
import cookielib

# local modules
from utils.static import DAS_CLIENT, CKEY, CERT
from utils.url_utils import HTTPSClientAuthHandler

def gen_queries(datasets, lkeys=['file', 'dataset', 'block', 'summary']):
    "Generate random queries from provided dataset list"
    for dataset in datasets:
        jdx   = random.randint(0, len(lkeys)-1)
        skey  = lkeys[jdx] # get random select key
        query = '%s dataset=%s' % (skey, dataset)
        yield query

def fullpath(path):
    "Expand path to full path"
    if  path and path[0] == '~':
        path = path.replace('~', '')
        path = path[1:] if path[0] == '/' else path
        path = os.path.join(os.environ['HOME'], path)
    return path

# request DAS server
def get_data(host, query, pid=None, threshold=300, debug=None):
    """Place request to DAS server and retrieve results"""
    idx     = 0
    limit   = 10
    params  = {'input':query, 'idx':idx, 'limit':limit}
    if  pid:
        params.update({'pid':pid})
    path    = '/das/cache'
    pat     = re.compile('http[s]{0,1}://')
    if  not pat.match(host):
        msg = 'Invalid hostname: %s' % host
        raise Exception(msg)
    url = host + path
    headers = {"Accept": "application/json", "User-Agent": DAS_CLIENT}
    encoded_data = urllib.urlencode(params, doseq=True)
    url += '?%s' % encoded_data
    req  = urllib2.Request(url=url, headers=headers)
    ckey = fullpath(CKEY)
    cert = fullpath(CERT)
    http_hdlr  = HTTPSClientAuthHandler(ckey, cert, debug)
    proxy_handler  = urllib2.ProxyHandler({})
    cookie_jar     = cookielib.CookieJar()
    cookie_handler = urllib2.HTTPCookieProcessor(cookie_jar)
    opener = urllib2.build_opener(http_hdlr, proxy_handler, cookie_handler)
    fdesc = opener.open(req)
    data = fdesc.read()
    fdesc.close()

    pat = re.compile(r'^[a-z0-9]{32}')
    if  data and isinstance(data, str) and pat.match(data) and len(data) == 32:
        pid = data
    else:
        pid = None
    iwtime  = 2  # initial waiting time in seconds
    wtime   = 20 # final waiting time in seconds
    sleep   = iwtime
    time0   = time.time()
    while pid:
        params.update({'pid':data})
        encoded_data = urllib.urlencode(params, doseq=True)
        url  = host + path + '?%s' % encoded_data
        req  = urllib2.Request(url=url, headers=headers)
        try:
            fdesc = opener.open(req)
            data = fdesc.read()
            fdesc.close()
        except urllib2.HTTPError as err:
            return {"status":"fail", "reason":str(err)}
        if  data and isinstance(data, str) and pat.match(data) and len(data) == 32:
            pid = data
        else:
            pid = None
        print "Waiting for pid=%s, sleep=%s" % (pid, sleep)
        time.sleep(sleep)
        if  sleep < wtime:
            sleep *= 2
        elif sleep == wtime:
            sleep = iwtime # start new cycle
        else:
            sleep = wtime
        if  (time.time()-time0) > threshold:
            reason = "client timeout after %s sec" % int(time.time()-time0)
            return {"status":"fail", "reason":reason}
    jsondict = json.loads(data)
    return jsondict



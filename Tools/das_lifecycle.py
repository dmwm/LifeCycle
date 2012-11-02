#!/usr/bin/env python
#pylint: disable-msg=

"""
DAS command line tool
"""
__author__ = "Valentin Kuznetsov"

import sys
if  sys.version_info < (2, 6):
    raise Exception("DAS requires python 2.6 or greater")

import os
import re
import time
import json
import urllib
import urllib2
import httplib
from   optparse import OptionParser

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    """
    Simple HTTPS client authentication class based on provided
    key/ca information
    """
    def __init__(self, key=None, cert=None, level=0):
        if  level:
            urllib2.HTTPSHandler.__init__(self, debuglevel=1)
        else:
            urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        """Open request method"""
        #Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.get_connection, req)

    def get_connection(self, host, timeout=300):
        """Connection method"""
        if  self.key:
            return httplib.HTTPSConnection(host, key_file=self.key,
                                                cert_file=self.cert)
        return httplib.HTTPSConnection(host)

class DASOptionParser: 
    """
    DAS cache client option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store", 
                               type="int", default=0, dest="verbose",
             help="verbose output")
        self.parser.add_option("--query", action="store", type="string", 
                               default=False, dest="query",
             help="specify query for your request")
        msg  = "host name of DAS server, default is https://dasdbs3.cern.ch"
        self.parser.add_option("--host", action="store", type="string", 
                       default='https://dasdbs3.cern.ch', dest="host", help=msg)
        msg  = "start index for returned result set, aka pagination,"
        msg += " use w/ limit (default is 0)"
        self.parser.add_option("--idx", action="store", type="int", 
                               default=0, dest="idx", help=msg)
        msg  = "number of returned results (default is 0, show all results)"
        self.parser.add_option("--limit", action="store", type="int", 
                               default=0, dest="limit", help=msg)
        msg  = 'query waiting threshold in sec, default is 5 minutes'
        self.parser.add_option("--threshold", action="store", type="int",
                               default=300, dest="threshold", help=msg)
        msg  = 'specify private key file name'
        self.parser.add_option("--key", action="store", type="string",
                   default=os.getenv("X509_USER_PROXY"), dest="ckey", help=msg)
        msg  = 'specify private certificate file name'
        self.parser.add_option("--cert", action="store", type="string",
                   default=os.getenv("X509_USER_PROXY"), dest="cert", help=msg)
        msg = 'specify input JSON file, e.g. LifeCycle workflow'
        self.parser.add_option("--in", action="store", type="string",
                               default="", dest="infile", help=msg)
        msg = 'specify output JSON file for LifeCycle workflow'
        self.parser.add_option("--out", action="store", type="string",
                           default="das_output.json", dest="outfile", help=msg)
        msg  = 'specify DBS instance to use (default is dev instance)'
        self.parser.add_option("--dbsinst", action="store", type="string",
                   default="dev", dest="dbsinst", help=msg)
    def get_opt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

def fullpath(path):
    "Expand path to full path"
    if  path and path[0] == '~':
        path = path.replace('~', '')
        path = path[1:] if path[0] == '/' else path
        path = os.path.join(os.environ['HOME'], path)
    return path

def get_data(host, query, idx, limit, debug,
        threshold=300, ckey=None, cert=None):
    """Contact DAS server and retrieve data for given DAS query"""
    params  = {'input':query, 'idx':idx, 'limit':limit}
    path    = '/das/cache'
    pat     = re.compile('http[s]{0,1}://')
    if  not pat.match(host):
        msg = 'Invalid hostname: %s' % host
        raise Exception(msg)
    url = host + path
    headers = {"Accept": "application/json"}
    encoded_data = urllib.urlencode(params, doseq=True)
    url += '?%s' % encoded_data
    req  = urllib2.Request(url=url, headers=headers)
    if  ckey and cert:
        ckey = fullpath(ckey)
        cert = fullpath(cert)
        hdlr = HTTPSClientAuthHandler(ckey, cert, debug)
    else:
        hdlr = urllib2.HTTPHandler(debuglevel=debug)
    opener = urllib2.build_opener(hdlr)
    fdesc = opener.open(req)
    data = fdesc.read()
    fdesc.close()

    pat = re.compile(r'^[a-z0-9]{32}')
    if  data and isinstance(data, str) and pat.match(data) and len(data) == 32:
        pid = data
    else:
        pid = None
    sleep   = 1  # initial waiting time in seconds
    wtime   = 30 # final waiting time in seconds
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
            return json.dumps({"status":"fail", "reason":str(err)})
        if  data and isinstance(data, str) \
                and pat.match(data) and len(data) == 32:
            pid = data
        else:
            pid = None
        time.sleep(sleep)
        if  sleep < wtime:
            sleep *= 2
        else:
            sleep = wtime
        if  (time.time()-time0) > threshold:
            reason = "client timeout after %s sec" % int(time.time()-time0)
            return json.dumps({"status":"fail", "reason":reason})
    return data

def das_query(host, query, idx=0, limit=0, debug=0,
        thr=300, ckey=None, cert=None):
    "Execute das query"
    data = get_data(host, query, idx, limit, debug, thr, ckey, cert)
    return json.loads(data)

def phedex_queries(dashost, phedex):
    "Execute DAS queries against phedex"
    for item in phedex:
        for obj in item['dataset']['blocks']:
            files = obj['block']['files']
            block = obj['block']
            del block['files']
            query = 'block=%s' % block['name']
            print "query", query
            das_data = das_query(dashost, query)
            yield das_data

def dbs_queries(dashost, dbslist, dbsinst):
    "Execute DAS queries against dbs"
    for item in dbslist:
        dbs_dataset = item['dataset']
        query = "dataset=%s instance=%s" % (dbs_dataset['dataset'], dbsinst)
        print "query", query
        das_data = das_query(dashost, query)
        yield das_data

def main():
    """Main function"""
    optmgr  = DASOptionParser()
    opts, _ = optmgr.get_opt()
    host    = opts.host
    debug   = opts.verbose
    query   = opts.query
    idx     = opts.idx
    limit   = opts.limit
    thr     = opts.threshold
    ckey    = opts.ckey
    cert    = opts.cert
    dbsinst = opts.dbsinst
    infile  = os.path.expanduser(opts.infile)
    outfile = opts.outfile
    if  query:
        data = das_query(host, query, idx, limit, debug, thr, ckey, cert)
        print data
    elif infile:
        with open(infile, 'r') as stream:
            injson = json.load(stream)
        phedex_data = None
        dbs_data    = None
        if  isinstance(injson, dict):
            phedex_data = injson['workflow']['Phedex']
            dbs_data = injson['workflow']['DBS']
            output = []
            for data in dbs_queries(host, dbs_data, dbsinst):
                output.append(data)
            for data in phedex_queries(host, phedex_data):
                output.append(data)
            injson['workflow']['DAS'] = output
            with open(outfile, 'w') as stream:
                stream.write(json.dumps(injson))

#
# main
#
if __name__ == '__main__':
    main()

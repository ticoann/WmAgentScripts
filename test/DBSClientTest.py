#!/usr/bin/env python
"""

 DBS 3 Client
 Ports all the functionality from previously used
 dbsTest.py to use DBS3 directly.

"""


import urllib2,urllib, httplib, sys, re, os, json, datetime
from xml.dom.minidom import getDOMImplementation
from dbs.apis.dbsClient import DbsApi

#das_host='https://das.cern.ch'
das_host='https://cmsweb.cern.ch'
#das_host='https://cmsweb-testbed.cern.ch'
#das_host='https://das-dbs3.cern.ch'
#das_host='https://dastest.cern.ch'
dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

dbsapi = DbsApi(url=dbs3_url)

parentLFN = '/store/data/Commissioning2015/Cosmics/RAW/v1/000/232/956/00000/82AB6593-3BA8-E411-AE6B-02163E011DC7.root'
print dbsapi.listFiles(logical_file_name = parentLFN, detail = True)
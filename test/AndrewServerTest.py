import httplib
import os
import json
import requests
import time
import urllib
from pprint import pprint
from itertools import islice


def grouper(iterable, n):
    """
    :param iterable: List of other iterable to slice
    :type: iterable
    :param n: Chunk size for resulting lists
    :type: int
    :return: iterator of the sliced list

    Source: http://stackoverflow.com/questions/3992735/python-generator-that-groups-another-iterable-into-groups-of-n
    """
    iterable = iter(iterable)
    return iter(lambda: list(islice(iterable, n)), [])


def get_req_info2(url, reqs):
    
    if isinstance(reqs, str):
        reqs = [reqs]
    query = ""
    for name in reqs:
        query += "name=%s" % name
        query += "&"
    query.rstrip("&")
    
    r = requests.get("%s/reqmgr2/data/request?%s" % (url, query),
                     cert=(os.getenv('X509_USER_CERT'), os.getenv('X509_USER_KEY')),
                    headers={"Accept": "application/json"})
    try:
        s = r.json()
    except: 
        print reqs
        print r.text
#     else:
#         try:
#             if req not in s['result'][0]:
#                 print "%s : %s" % (req, s['result'][0].keys())
#                 print r.status_code
#         except:
#             print req
#             raise
    
def get_req_info(conn, reqs):
    
    if isinstance(reqs, str):
        reqs = [reqs]
    query = ""
    for name in reqs:
        query += "name=%s" % name
        query += "&"
    query.rstrip("&")
        
    r1=conn.request('GET','/reqmgr2/data/request?%s' % query, headers={"Accept": "application/json"})
    r2=conn.getresponse()
    data = r2.read()
    try:
        s = json.loads(data)
    except: 
        print reqs
        print data
#     else:
#         try:
#             if reqs not in s['result'][0]:
#                 print "%s : %s" % (req, s['result'][0].keys())
#                 print r2.status
#         except:
#             print reqs
#             raise
     

def get_req_info_from_couch(conn, reqs):
    
    
    if isinstance(reqs, str):
        reqs = [reqs]
    query = ""
    reqs = json.dumps({"keys": reqs})
    r1=conn.request('POST','/couchdb/reqmgr_workload_cache/_all_docs?include_docs=true', reqs, headers={"Accept": "application/json"})
    r2=conn.getresponse()
    data = r2.read()
#    import pdb
#    pdb.set_trace()
    try:
        s = json.loads(data)
        resultDict = {}
        for row in s['rows']:
            resultDict[row['key']] = row['doc']
        #pprint(resultDict)
    except Exception as ex:
        import traceback
        msg = traceback.format_exc()
        print row['key']
        print row['doc']
        print str(msg)
        print reqs
        print data
#     else:
#         try:
#             if reqs not in s['result'][0]:
#                 print "%s : %s" % (req, s['result'][0].keys())
#                 print r2.status
#         except:
#             print reqs
#             raise

def getActiveRequestNames(url="cmsweb-testbed.cern.ch"):
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_CERT'), 
                                 key_file = os.getenv('X509_USER_KEY'))
    conn.request('GET','/reqmgr2/data/request?status=ACTIVE&detail=false', headers={"Accept": "application/json"})
    r2=conn.getresponse()
    data = r2.read()
    print data
    s = json.loads(data)
    print len(s["result"])
    return s["result"]

def getAndrewFileProductionData():
    requests = []
    with open("andrew_files.txt") as f:
        for line in f:
            req = line.strip().strip('|').strip()
            requests.append(req)
    return requests
        
def testTestbed(url ="cmsweb-testbed.cern.ch"):
    test_url = "https://%s" % url
    
    requests = getActiveRequestNames(url)
    count = 0
    
    for reqs in grouper(requests, 10):
        get_req_info2(test_url, reqs)
        count += len(reqs)
        print count 
    return count

def testProduction(requests):
    url = "cmsweb.cern.ch"
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_CERT'), 
                                   key_file = os.getenv('X509_USER_KEY'))
    count = 0
    
    for reqs in grouper(requests, 10):
        get_req_info(conn, reqs)
        count += len(reqs)
        print count
        #time.sleep(1)
    return count

def testProductionCouch(requests):
    url = "cmsweb.cern.ch"
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_CERT'), 
                                   key_file = os.getenv('X509_USER_KEY'))
    count = 0
    
    for reqs in grouper(requests, 20):
        get_req_info_from_couch(conn, reqs)
        count += len(reqs)
        print count
        #time.sleep(1)
    return count

def testProductionWMCore(requests):
    
    from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
    reqmgr_db_service = RequestDBWriter("https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache", couchapp="ReqMgr")
    
    count = 0
    
    for reqs in grouper(requests, 20):
        result = reqmgr_db_service.getRequestByNames(reqs)
        pprint(result)
        count += len(reqs)
        print count
        #time.sleep(1)
    return count

def testActiveDataProduction():
    url = "cmsweb-testbed.cern.ch"
    test_url = "https://%s" % url
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_CERT'), 
                                 key_file = os.getenv('X509_USER_KEY'))
    a1=conn.request('GET','/reqmgr2/data/request?status=ACTIVE&detail=false', headers={"Accept": "application/json"})
    r2=conn.getresponse()
    data = r2.read()
    s = json.loads(data)
    print len(s["result"])
    
    count = 0
    
    for req in s["result"]:
        #get_req_info(conn, req)
        get_req_info2(test_url, req)
        count += 1
        print count 
    return count

           
#count = testTestbed()
#count = testTestbed("reqmgr2-dev.cern.ch")    
#count = testProduction()

#requests = getActiveRequestNames(url="cmsweb.cern.ch")
requests = getAndrewFileProductionData()
count = testProduction(requests)
#count = testProductionCouch(requests)    
#count = testProductionWMCore(requests)    
print "%s queryed" % count  
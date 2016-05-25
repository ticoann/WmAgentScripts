import httplib
import os
import json
import requests

def get_req_info2(url, req):
    r = requests.get("%s/reqmgr2/data/request?name=%s" % (url, req),
                     cert=(os.getenv('X509_USER_CERT'), os.getenv('X509_USER_KEY')),
                    headers={"Accept": "application/json"})
    try:
        s = r.json()
    except: 
        print req
        print r.text
    else:
        try:
            if req not in s['result'][0]:
                print "%s : %s" % (req, s['result'][0].keys())
                print r.status_code
        except:
            print req
            raise
    
def get_req_info(conn, req):
    
    r1=conn.request('GET','/reqmgr2/data/request?name='+req,headers={"Accept": "application/json"})
    r2=conn.getresponse()
    data = r2.read()
    try:
        s = json.loads(data)
    except: 
        print req
        print data
    else:
        try:
            if req not in s['result'][0]:
                print "%s : %s" % (req, s['result'][0].keys())
                print r2.status
        except:
            print req
            raise
     


def testTestbed():
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

def testProduction():
    url = "cmsweb.cern.ch"
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_CERT'), 
                                   key_file = os.getenv('X509_USER_KEY'))
    count = 0
    with open("andrew_files.txt") as f:
        for line in f:
            req = line.strip().strip('|').strip()
            get_req_info(conn, req)
            count += 1
            print count
    return count
            
count = testTestbed()    
#count = testProduction()        
print "%s queryed" % count  
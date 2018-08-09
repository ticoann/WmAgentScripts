import httplib
import os
import json

def testReqMgr2VM():
    url = "reqmgr2-dev.cern.ch"
    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_CERT'),
                                   key_file=os.getenv('X509_USER_KEY'))
    encodedParams = json.dumps({"request":{"process":"assignment-approved"}})
    r1 = conn.request('POST', '/microservice/data', encodedParams, headers={"Content-type": "application/json", "Accept": "application/json"})
    r2 = conn.getresponse()
    data = r2.read()
    print data

testReqMgr2VM()
import requests
import os
from pprint import pprint
wmstatsurl = "https://cmsweb-testbed.cern.ch/wmstatsserver/data/requestcache"

certpath = os.getenv("X509_USER_CERT")
keypath = os.getenv("X509_USER_KEY")
r = requests.get(wmstatsurl, cert=(certpath, keypath), verify=False, stream=False)
for req, reqInfo in r.json()['result'][0].iteritems():
    result = reqInfo.get("OutputDatasets", [])
    if len(result) > 0 and not isinstance(result[0], basestring):
        print req
        
print "done"
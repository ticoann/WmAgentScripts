import time
from pprint import pprint
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter


if __name__ == "__main__":
    baseUrl = "https://cmsweb.cern.ch"
    wmstatsUrl = "%s/couchdb/wmstats" % baseUrl
    reqMgrUrl = "%s/couchdb/t0_request" % baseUrl
    
    t0ReqDB = RequestDBWriter(reqMgrUrl, couchapp="T0Request")
    data = t0ReqDB.getRequestByStatus(["completed"], detail=True)
    count = 0
    reqNames = []
    current = int(time.time())
    fiveMonth = 60*60*24*30*5
    print len(data)
    for req, info in data.items(): 
        #if current - info["RequestTransition"][-1]["UpdateTime"] > 60*60*24*30*5:
        if info['Run'] < 271084:
            reqNames.append(req)
    print len(reqNames)        
    for reqname in reqNames:
        t0ReqDB.updateRequestStatus(reqname, "normal-archived")
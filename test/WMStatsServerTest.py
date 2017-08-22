from WMCore.Services.WMStatsServer.WMStatsServer import WMStatsServer
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.WMStats.DataStructs.DataCache import DataCache
import time
from pprint import pprint
baseurl = "https://cmsweb.cern.ch"
wmstats_url = "%s/wmstatsserver" % baseurl
reqmgr2_url = "%s/reqmgr2" % baseurl
wmstats = WMStatsServer(wmstats_url)
reqmgrSvc = ReqMgr(reqmgr2_url)

data = wmstats.getActiveData()
DataCache.setlatestJobData(data)

filter = {"RequestStatus": ["announced"], "AgentJobInfo": "CLEANED"}
count = 0
archiveDelayHours = 24 * 3
threshold = archiveDelayHours * 3600

currentTime = int(time.time())
for a in DataCache.filterDataByRequest(filter, ["AgentJobInfo", "RequestTransition"]):
    if not a["RequestTransition"] or (currentTime - a["RequestTransition"][-1]["UpdateTime"]) > threshold:
        
        try:
            reqmgrSvc.updateRequestStatus(a["RequestName"], "normal-archived")
            print a
            count += 1
        except Exception as ex:
            print "Fail to update %s: %s" %  (a["RequestName"], str(ex))

    
print count

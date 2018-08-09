#!/usr/bin/env python
from WMCore.Services.WMStatsServer.WMStatsServer import WMStatsServer
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.WMStats.DataStructs.DataCache import DataCache
import time
from pprint import pprint

baseurl = "https://cmsweb.cern.ch"
wmstats_url = "%s/wmstatsserver" % baseurl
wmstats = WMStatsServer(wmstats_url)

data = wmstats.getFilteredActiveData({}, ["OutputModulesLFNBases", "RequestPriority"])

for a in data:
    if not a["RequestTransition"] or (currentTime - a["RequestTransition"][-1]["UpdateTime"]) > threshold:

        try:
            reqmgrSvc.updateRequestStatus(a["RequestName"], "normal-archived")
            print a
            count += 1
        except Exception as ex:
            print "Fail to update %s: %s" % (a["RequestName"], str(ex))

print count

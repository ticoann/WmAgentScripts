#!/usr/bin/env python
from WMCore.Services.WMStatsServer.WMStatsServer import WMStatsServer
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.WMStats.DataStructs.DataCache import DataCache
import time
from pprint import pprint

baseurl = "https://cmsweb.cern.ch"
wmstats_url = "%s/wmstatsserver" % baseurl
wmstats = WMStatsServer(wmstats_url)

data = wmstats.getFilteredActiveData({}, ["OutputModulesLFNBases", "RequestPriority", "RequestStatus", "InitialPriority"])
def findWF(data, st):
    print unmerged, size/1000000000000, "TB"
    requests = {}
    for info in data:
        if info["OutputModulesLFNBases"]:
            for t in info.get("OutputModulesLFNBases", []):
                if st in t:
                    if not requests.get(info["RequestName"]):
                        print "     ", info["RequestName"], info["RequestStatus"], info["InitialPriority"], info["RequestPriority"]
                        requests[info["RequestName"]] = True

with open("./umergedData.txt") as um:
    a = um.readlines()
    for info in a:
        line = info.split()
        unmerged = line[0]
        size = int(line[1])
        findWF(data, unmerged)

#pprint(data)
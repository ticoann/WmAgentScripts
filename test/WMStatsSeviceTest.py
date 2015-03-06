from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.WMStats.DataStruct.RequestInfoCollection import RequestInfoCollection

if __name__ == "__main__":
    url = "https://cmsweb-testbed.cern.ch/couchdb/wmstats"
    reqDBURL = "https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache"
    testbedWMStats = WMStatsReader(url, reqDBURL)
    print "start to getting job information from %s" % url
    print "will take a while\n"
    print testbedWMStats.couchServer.listDatabases()
    #print testbedWMStats._getRequestAndAgent()
    #print testbedWMStats._getAllDocsByIDs(["alahiff_HLTCloudTesting_130412_172143_4087"])
    #print testbedWMStats.getRequestByNames(["alahiff_HLTCloudTesting_130412_172143_4087"], jobInfoFlag = True)
    requests = testbedWMStats.getRequestByStatus("assigned", jobInfoFlag = True, legacyFormat = True)
    requestCollection = RequestInfoCollection(requests)
    result = requestCollection.getJSONData()
    print result
    print "\ntotal %s requests retrieved" % len(result)
    requestsDict = requestCollection.getData()
    for requestName, requestInfo in requestsDict.items():
        print requestName + " :"
        print "\ttotalLuims: %s" % requestInfo.getTotalInputLumis()
        print "\ttotalEvents: %s" % requestInfo.getTotalInputEvents()
        print "\ttotal top level jobs: %s" % requestInfo.getTotalTopLevelJobs()
        print "\ttotal top level jobs in wmbs: %s" % requestInfo.getTotalTopLevelJobsInWMBS()
        print "\tProgress by output dataset:"
        summaryDict = requestInfo.getProgressSummaryByOutputDataset()
        for oDataset, summary in summaryDict.items():
            print "\t  %s" % oDataset
            print "\t   %s" %summary.getReport()
        print "\n"
    print "done"
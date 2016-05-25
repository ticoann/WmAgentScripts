#!/usr/bin/env python
from __future__ import print_function, division
import os
from pprint import pprint
from WMCore.Configuration import loadConfigurationFile
from WMCore.WorkQueue.WMBSHelper import freeSlots
from WMCore.WorkQueue.WorkQueueUtils import cmsSiteNames
from WMCore.WorkQueue.WorkQueueUtils import queueFromConfig
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.WorkQueue.DataStructs.WorkQueueElementsSummary import WorkQueueElementsSummary
from WMCore.WorkQueue.WorkQueueUtils import cmsSiteNames

HOST = "https://cmsweb.cern.ch" 
COUCH = "%s/couchdb" % HOST
wmstatDBName = "wmstats"
WEBURL = "%s/workqueue" % COUCH
REQMGR2 = "%s/reqmgr2" % HOST
LOG_DB_URL = "%s/wmstats_logdb" % COUCH
LOG_REPORTER = "global_workqueue"
reqmgrCouchDB = "reqmgr_workload_cache"

queueParams = {'WMStatsCouchUrl': "%s/%s" % (COUCH, wmstatDBName)}
queueParams['QueueURL'] = WEBURL
queueParams['CouchUrl'] = COUCH
queueParams['ReqMgrServiceURL'] = REQMGR2
queueParams['RequestDBURL'] = "%s/%s" % (COUCH, reqmgrCouchDB)
queueParams['central_logdb_url'] = LOG_DB_URL
queueParams['log_reporter'] = LOG_REPORTER

globalQ = globalQueue(**queueParams)

siteSummary = {}

def getAcceptableSites(ele):
    
    if ele['SiteWhitelist']:
        commonSites = set(ele['SiteWhitelist'])
    else:
        commonSites = set()
    
    if ele['Inputs']:
        if commonSites:
            commonSites = commonSites & set([y for x in ele['Inputs'].values() for y in x])
        else:
            commonSites = set([y for x in ele['Inputs'].values() for y in x])
    if ele['PileupData']:
        if commonSites:
            commonSites = commonSites & set([y for x in ele['PileupData'].values() for y in x])
        else:
            commonSites = set([y for x in ele['PileupData'].values() for y in x])

    return commonSites

# change the filter only filters TeamNam
gqElements = globalQ.status("Available", TeamName="production")

testReq = "pdmvserv_TOP-RunIISummer15GS-00078_00297_v0__160409_121122_2613"
gqSummary = WorkQueueElementsSummary(gqElements)
possibleSites = gqSummary.getPossibleSitesByRequest(testReq)
print(set(cmsSiteNames()).intersection(possibleSites))

filteredElements = gqSummary.elementsWithHigherPriorityInSameSites(testReq)

wqSummary = WorkQueueElementsSummary(filteredElements)
wqElements = wqSummary.getWQElementResultsByReauest()


priority = gqSummary.getWQElementResultsByReauest(testReq)['Priority']
#pprint(gqElements)

print("GQ: %s elements available" % len(gqElements)) 
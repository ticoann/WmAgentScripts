#!/usr/bin/env python
from __future__ import print_function, division
import os
from optparse import OptionParser
from pprint import pprint
from WMCore.WorkQueue.DataStructs.WorkQueueElementsSummary import WorkQueueElementsSummary
from WMCore.WorkQueue.WorkQueue import globalQueue

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

def getOptions():
    parser = OptionParser()
    parser.set_usage("check-request-wq-status -r [request_name] -t [status]")
    
    parser.add_option("-r", "--request", dest = "request",
                      help = "resquest name")
    
    parser.add_option("-t", "--team", dest = "team", default="production",
                      help = "set to new status")
    
    (options, args) = parser.parse_args()
    
    return options.request, options.team
    

if __name__ == '__main__':
    #request, team = getOptions())
    team = "production"
    request = "jen_a_ACDC_task_BPH-RunIIFall15DR76-00044__v1_T_160513_043933_6676"
    # change the filter only filters TeamNam
    gqElements = globalQ.status("Available", TeamName=team)
    #pprint(gqElements)
    print("GQ: %s elements available" % len(gqElements))
         
    wqSummary = WorkQueueElementsSummary(gqElements)
    wqSummary.printSummary(request)
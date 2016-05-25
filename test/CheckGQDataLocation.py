#!/usr/bin/env python
from __future__ import print_function, division
import os
from pprint import pprint
from WMCore.Configuration import loadConfigurationFile
from WMCore.WorkQueue.WMBSHelper import freeSlots
from WMCore.WorkQueue.WorkQueueUtils import cmsSiteNames
from WMCore.WorkQueue.WorkQueueUtils import queueFromConfig
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

import pdb
pdb.set_trace()
# change the filter only filters TeamNam
gqElements = globalQ.dataLocationMapper()
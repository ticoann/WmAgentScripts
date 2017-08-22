from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.WMStatsServer.WMStatsServer import WMStatsServer
from WMCore.ReqMgr.CherryPyThreads.StatusChangeTasks import moveForwardStatus
from pprint import pprint
import traceback
import logging
reqmgr2_url = "https://cmsweb.cern.ch/reqmgr2"
workqueue_url = "https://cmsweb.cern.ch/couchdb/workqueue"
wmstats_url = "https://cmsweb.cern.ch/wmstatsserver"

reqmgrSvc = ReqMgr(reqmgr2_url)
gqService = WorkQueue(workqueue_url)
wmstatsSvc = WMStatsServer(wmstats_url)

wfStatusDict = gqService.getWorkflowStatusFromWQE()
pprint(wfStatusDict.get("vlimant_ACDC1_task_HGC-PhaseIITDRSpring17DR-00023__v1_T_170802_143815_4017"))
# moveForwardStatus(reqmgrSvc, wfStatusDict, logging)
#moveToCompletedForNoWQJobs(reqmgrSvc, wfStatusDict, self.logger)
#moveToArchived(wmstatsSvc, reqmgrSvc, config.archiveDelayHours, self.logger)
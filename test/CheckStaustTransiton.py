from __future__ import print_function
import logging
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.ReqMgr.CherryPyThreads.StatusChangeTasks import moveToArchivedForNoJobs

base_url = "https://cmsweb-testbed.cern.ch"
reqmgr2_url = "%s/reqmgr2" % base_url
workqueue_url = "%s/couchdb/workqueue" % base_url
reqmgrSvc = ReqMgr(reqmgr2_url)
gqService = WorkQueue(workqueue_url)

print("Getting GQ data for status check")
wfStatusDict = gqService.getWorkflowStatusFromWQE()


statusTransition = {"aborted": ["aborted-completed", "aborted-archived"],
                       "aborted-completed": ["aborted-archived"],
                       "rejected": ["rejected-archived"]}

# for status, nextStatusList in statusTransition.items():
#     requests = reqmgrSvc.getRequestByStatus([status], detail=False)
#     count = 0
#     for wf in requests:
#         # check whether wq elements exists for given request
#         # if not, it means
#         print(wf)
#         if wf not in wfStatusDict:
#             print(wf)
            
moveToArchivedForNoJobs(reqmgrSvc, wfStatusDict, logging)
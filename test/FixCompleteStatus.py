from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.WorkQueue.WorkQueue import globalQueue
from pprint import pprint
import traceback


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

reqSvc = ReqMgr(REQMGR2)
globalQ = globalQueue(**queueParams)

def fixCompleteStatus(reqSvc, queue, status):
    
    requests = reqSvc.getRequestByStatus(status, detail=False)
    pprint("total requests: %s" % len(requests))
    # getting all workflows which has deleted inbox elements
    deletedRequests = []
    for request in requests:
        elements = queue.backend.getInboxElements(WorkflowName=request)
        if len(elements) == 0:
            deletedRequests.append(request)

    pprint(len(deletedRequests))
    
    # check whether there is wq elements in available states
    
    completedRequests = set()
    somethingWrong = set()
    for request in deletedRequests:
        elements = queue.backend.getElements(WorkflowName=request)
        #pprint(elements)
        completed = True
        for element in elements:
            status = element['Status']
            if status in ['Running', 'Acquired', 'Available', 'CancelRequested']:
                runningElements = False
                break
            
        if completed:
            completedRequests.add(request)
        else:
            somethingWrong.add(request)
    
    pprint(len(completedRequests)) 
    pprint(completedRequests)
    pprint(len(somethingWrong))
    pprint(somethingWrong)
    return completedRequests
        
if __name__ == "__main__":
    
    for status in ["running-closed", "force-complete"]:
        completedRequests = fixCompleteStatus(reqSvc, globalQ, status)
    
        for request in completedRequests:
            reqSvc.updateRequestStatus(request, "completed")
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
url = "https://cmsweb-testbed.cern.ch/reqmgr2"
raux = ReqMgrAux(url)

print raux.getCMSSWVersion()
print raux.getWMAgentConfig("vocms008.cern.ch")

reqmgr2Svc = ReqMgr(url)

abortedAndForceCompleteWorkflowCache = reqmgr2Svc.getAbortedAndForceCompleteRequestsFromMemoryCache(expire=120)

for i in range(10):
    print abortedAndForceCompleteWorkflowCache.getData()
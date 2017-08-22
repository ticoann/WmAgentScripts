from WMCore.Services.ReqMgr.ReqMgr import ReqMgr

base_url = "https://cmsweb-testbed.cern.ch"
reqmgr2_url = "%s/reqmgr2" % base_url
reqmgrSvc = ReqMgr(reqmgr2_url)

requests = ["amaltaro_StepChain_ReDigi3_HG1705_Validation_170425_150822_3902"]
for requestName in requests:
    response = reqmgrSvc.cloneRequest(requestName, {"CMSSWVersion": "CMSSW_7_1_25_patch3"})
    print response
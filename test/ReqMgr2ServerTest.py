from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from pprint import pprint
import traceback
url = "https://cmsweb-testbed.cern.ch/reqmgr2"

reqSvc = ReqMgr(url)
for i in range(100):
    try:
        result = reqSvc.getRequestByStatus(["ACTIVE"], ["RequestName"])
        print i
    except:
        pprint(traceback.format_exc())

print("done")
from pprint import pprint
from WMCore.Wrappers.JsonWrapper import JSONEncoder
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.ReqMgr.ReqMgrReader import ReqMgrReader
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.RequestManager.RequestDB.Settings.RequestStatus import StatusList
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Wrappers import JsonWrapper

if __name__ == "__main__":
    baseUrl = "https://cmsweb.cern.ch"    
    args = {}
    args["endpoint"] = "%s/reqmgr/rest" % baseUrl
    reqMgr = RequestManager(args)
    
    print reqMgr.reportRequestStatus("dmason_task_HIG-RunIISummer15wmLHEGS-00747__v0_HEPCloudII_161108_153251_576", "completed")
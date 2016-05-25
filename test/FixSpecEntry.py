from __future__ import print_function
from WMCore.Wrappers.JsonWrapper import JSONEncoder
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.RequestManager.RequestDB.Settings.RequestStatus import StatusList
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter

baseUrl = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"

# WARNING: this is hard coded change to the correct wrokflow task and acquistionEra
workflow = 'pdmvserv_task_HIG-RunIISpring16DR80-00066__v1_T_160405_075229_6042'
taskNameWithWrongOutputDS = "HIG-RunIISpring16MiniAODv1-00066_0"
correctAcquisitionEra = "RunIISpring16MiniAODv1"


def getSpec(request):
    wh = WMWorkloadHelper()
    reqmgrSpecUrl = "%s/%s/spec" % (baseUrl, request)
    wh.load(reqmgrSpecUrl)
    return wh

def getOutputDSFromSpec(request):
    wh = getSpec(request)
    candidate = wh.listOutputDatasets()
    return candidate

wh = getSpec(workflow)

# WARNING: save original request in case something goes wrong
wh.save("%s.original.pkl" % workflow)
print("Original output datasets")
print(getOutputDSFromSpec(workflow))
#task = wh.getTask("HIG-RunIISpring16MiniAODv1-00066_0")
#print(task.name())

for task in wh.getAllTasks():
    if task.name() == taskNameWithWrongOutputDS:
        print("changing %s aqc: %s to acq %s" % (task.name(), task.getAcquisitionEra(), correctAcquisitionEra))
        task.setAcquisitionEra(correctAcquisitionEra)
        task.updateLFNsAndDatasets()

# just for the validation
#for task in wh.getAllTasks():     
#    print("%s %s %s" % (task.name(), task.getAcquisitionEra(), task.listOutputDatasetsAndModules()))

wh.saveCouchUrl("%s/%s" %(baseUrl, workflow))

print("Updated output datasets")
print(getOutputDSFromSpec(workflow))
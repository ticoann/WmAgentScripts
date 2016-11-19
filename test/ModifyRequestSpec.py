from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

def loadWorkload(url):
    helper = WMWorkloadHelper()
    helper.load(url)
    return helper

urlbase = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/%s/spec"
request = "dmason_task_HIG-RunIISummer15wmLHEGS-00747__v0_HEPCloudII_161108_153251_576"
#request = "bsutar_HLT_newco_RelVal_285090_161117_194553_8910"
url = urlbase % request

helper = loadWorkload(url)
tasks = helper.getTopLevelTask()
for task in tasks:
    print task.getAcquisitionEra()
    print task.getProcessingVersion()
    print task.getProcessingString()
    
print helper.getCampaign()
#print helper.getAcquisitionEra()
#print helper.getProcessingVersion()
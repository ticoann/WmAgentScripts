import requests
import os
from pprint import pprint
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

reqmgrurl = "https://cmsweb.cern.ch/reqmgr2/data/request?status=ACTIVE&request_type=TaskChain&mask=CMSSWVersion&mask=Task1&mask=Task2&mask=Task3&mask=Task4&mask=Task5&mask=Task6&mask=Task7&mask=TaskChain"
# reqmgrurl = "https://cmsweb.cern.ch/reqmgr2/data/request?name=pdmvserv_task_EXO-RunIISummer15GS-10533__v1_T_170602_203815_9168&request_type=TaskChain&mask=CMSSWVersion&mask=Task1&mask=Task2&mask=Task3&mask=Task4&mask=Task5&mask=Task7&mask=Task7&mask=TaskChain"
reqdb_url = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"
certpath = os.getenv("X509_USER_CERT")
keypath = os.getenv("X509_USER_KEY")
r = requests.get(reqmgrurl, cert=(certpath, keypath), verify=False, stream=False)
count = 0

helper = WMWorkloadHelper()
    
for item in r.json()['result']:
    for req, reqInfo in item.iteritems():
        result = reqInfo.get("CMSSWVersion", [])
        if not isinstance(result, basestring) and len(result) > 1:
            # print req
            # pprint(reqInfo)
            helper.loadSpecFromCouch(reqdb_url, req)
            for i in range(reqInfo['TaskChain']):
                try:
                    taskName = reqInfo['Task%s' % (i + 1)]["TaskName"]
                    cmssw = reqInfo['Task%s' % (i + 1)].get("CMSSWVersion", "NOVERSION")
                except KeyError:
                    pprint(req)
                    pprint(reqInfo)
                    continue
                # print taskName
                # print helper.listAllTaskPathNames()
                # print helper.listAllTaskNames()
                taskHelper = helper.getTaskByName(taskName)
                if taskHelper is not None:
                    cmssw2 = taskHelper.getCMSSWVersionsWithMergeTask()
                    if len(cmssw2) != 1 or set([cmssw]) != cmssw2 or req == 'pdmvserv_task_EXO-RunIISummer15GS-10533__v1_T_170602_203815_9168':
                        print req, taskName, cmssw, cmssw2
                else:
                    print "Error no task, request: %s task: %s cmssw: %s" % (req, taskName, cmssw)
            count += 1

print "%s done" % count
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend
import time
from WMCore.WorkQueue.Policy.Start import startPolicy
from pprint import pprint
import datetime

params = {}
params.setdefault('SplittingMapping', {})
params['SplittingMapping'].setdefault('DatasetBlock',
                                                   {'name': 'Block',
                                                    'args': {}}
                                                  )
params['SplittingMapping'].setdefault('MonteCarlo',
                                           {'name': 'MonteCarlo',
                                            'args':{}}
                                           )
params['SplittingMapping'].setdefault('Dataset',
                                           {'name': 'Dataset',
                                            'args': {}}
                                          )
params['SplittingMapping'].setdefault('Block',
                                           {'name': 'Block',
                                            'args': {}}
                                          )
params['SplittingMapping'].setdefault('ResubmitBlock',
                                           {'name': 'ResubmitBlock',
                                            'args': {}}
                                          )

couchurl = "https://cmsweb.cern.ch/couchdb"
dbname = "workqueue"
inboxname = "workqueue_inbox"
backend = WorkQueueBackend(couchurl, dbname, inboxname)
# data = backend.getInboxElements(OpenForNewData = True)
# for item in data:
#     if (item['RequestName'] == 'fabozzi_RVCMSSW_7_2_0_pre3ADDMonoJet_d3MD3_13__CondDBv2_140731_132018_1268'):
#     #if (item['RequestName'] == 'jbadillo_ACDC_HIG-Summer12-02187_00158_v0__140728_102627_3527'):
#         workflowsToCheck = [item]
#         for key, value in item.items():
#             print "%s: %s" % (key, value)
print backend.isAvailable()

teams = set()
for e in backend.getElements():
    teams.add(e["TeamName"])
    if e["TeamName"] == "":
        print e

print teams
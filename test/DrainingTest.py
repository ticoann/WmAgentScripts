from WMCore.Services.WorkQueue.WorkQueue import WorkQueue

base_url = "https://cmsweb.cern.ch"
workqueue_url = "%s/couchdb/workqueue" % base_url
gqService = WorkQueue(workqueue_url)

result = gqService.testQueuesAndStatus("vocms0304.cern.ch")
for req in result:
    print req
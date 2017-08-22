#!/usr/bin/env python
from __future__ import print_function, division

from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
workqueue_url = "https://reqmgr2-dev.cern.ch/couchdb/workqueue"
gq = WorkQueue(workqueue_url)
print(gq.getWorkflowStatusFromWQE())
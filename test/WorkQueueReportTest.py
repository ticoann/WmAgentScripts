#!/usr/bin/env python
"""Helper class for RequestManager interaction
"""
from __future__ import (print_function, division)
from pprint import pprint

from WMCore import Lexicon
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_STATE_LIST
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError, WorkQueueNoWorkError
from WMCore.Database.CMSCouch import CouchError
from WMCore.Database.CouchUtils import CouchConnectionError
from WMCore import Lexicon
from WMCore.WorkQueue.WorkQueue import globalQueue

import os
import time
import socket
import traceback


HOST = "https://cmsweb-testbed.cern.ch" 
COUCH = "%s/couchdb" % HOST
wmstatDBName = "wmstats"
WEBURL = "%s/workqueue" % COUCH
REQMGR2 = "%s/reqmgr2" % HOST
LOG_DB_URL = "%s/wmstats_logdb" % COUCH
LOG_REPORTER = "global_workqueue"
reqmgrCouchDB = "reqmgr_workload_cache"

queueParams = {'WMStatsCouchUrl': "%s/%s" % (COUCH, wmstatDBName)}
queueParams['QueueURL'] = WEBURL
queueParams['CouchUrl'] = COUCH
queueParams['ReqMgrServiceURL'] = REQMGR2
queueParams['RequestDBURL'] = "%s/%s" % (COUCH, reqmgrCouchDB)
queueParams['central_logdb_url'] = LOG_DB_URL
queueParams['log_reporter'] = LOG_REPORTER


class WorkQueueReqMgrInterface(object):
    """Helper class for ReqMgr interaction"""

    def __init__(self, **kwargs):
        # this will break all in one test
        self.reqMgr2 = ReqMgr(kwargs.get("reqmgr2_endpoint", None))

    def report(self, queue, requestName=None):
        """Report queue status to ReqMgr."""
        new_state = {}
        uptodate_elements = []
        now = time.time()

        elements = queue.statusInbox(dictKey="RequestName")
        if not elements:
            return new_state

        for ele in elements:
            ele = elements[ele][0]  # 1 element tuple 
            if requestName is not None and ele['RequestName'] != requestName:
                continue   
            try:
                request = self.reqMgr2.getRequestByNames(ele['RequestName'])
                if not request:
                    msg = 'Failed to get request "%s" from ReqMgr2. Will try again later.' % ele['RequestName']
                    print(msg)
                    continue
                
                request = request[ele['RequestName']]
                # Check only reqmgr1 request (skipping reqmgr2 request update)
                if "ReqMgr2Only" in request and request["ReqMgr2Only"]:
                    continue
                print("handling reqmgr1 request %s: %s" % (ele['RequestName'], request['RequestStatus']))
                if request['RequestStatus'] in ('failed', 'completed', 'announced',
                                                'epic-FAILED', 'closed-out', 'rejected'):
                    # requests can be done in reqmgr but running in workqueue
                    # if request has been closed but agent cleanup actions
                    # haven't been run (or agent has been retired)
                    # Prune out obviously too old ones to avoid build up
                    if queue.params.get('reqmgrCompleteGraceTime', -1) > 0:
                        if (now - float(ele.updatetime)) > queue.params['reqmgrCompleteGraceTime']:
                            # have to check all elements are at least running and are old enough
                            request_elements = queue.statusInbox(WorkflowName=request['RequestName'])
                            if not any([x for x in request_elements if x['Status'] != 'Running' and not x.inEndState()]):
                                last_update = max([float(x.updatetime) for x in request_elements])
                                if (now - last_update) > queue.params['reqmgrCompleteGraceTime']:
                                    print("Finishing request %s as it is done in reqmgr" % request['RequestName'])
                                    queue.doneWork(WorkflowName=request['RequestName'])
                                    continue
                    else:
                        pass  # assume workqueue status will catch up later
                elif request['RequestStatus'] in ['aborted', 'force-complete']:
                    queue.cancelWork(WorkflowName=request['RequestName'])
                # Check consistency of running-open/closed and the element closure status
                elif request['RequestStatus'] == 'running-open' and not ele.get('OpenForNewData', False):
                    self.reportRequestStatus(ele['RequestName'], 'running-closed')
                elif request['RequestStatus'] == 'running-closed' and ele.get('OpenForNewData', False):
                    print("closing work %s" % ele['RequestName'])
                    queue.closeWork(ele['RequestName'])
                # we do not want to move the request to 'failed' status
                elif ele['Status'] == 'Failed':
                    continue
                elif ele['Status'] not in self._reqMgrToWorkQueueStatus(request['RequestStatus']):
                    print("reporting... %s" % ele['RequestName'])
                    self.reportElement(ele)

                uptodate_elements.append(ele)
            except Exception as ex:
                msg = 'Error talking to ReqMgr about request "%s": %s'
                traceMsg = traceback.format_exc()
                print(msg % (ele['RequestName'], traceMsg))

        return uptodate_elements

    def reportRequestStatus(self, request, status, message=None):
        """Change state in RequestManager
           Optionally, take a message to append to the request
        """
        if message:
            print(request, str(message), 'info')
        reqmgrStatus = self._workQueueToReqMgrStatus(status)
        if reqmgrStatus:  # only send known states
            try:
                self.reqMgr2.updateRequestStatus(request, reqmgrStatus)
            except Exception as ex:
                msg = "%s : fail to update status will try later: %s" % (request, str(ex))
                print(request, msg, 'warning')
                raise ex
        return

    def _workQueueToReqMgrStatus(self, status):
        """Map WorkQueue Status to that reported to ReqMgr"""
        statusMapping = {'Acquired': 'acquired',
                         'Running': 'running-open',
                         'Failed': 'failed',
                         'Canceled': 'aborted',
                         'CancelRequested': 'aborted',
                         'Done': 'completed'
                        }
        if status in statusMapping:
            # if wq status passed convert to reqmgr status
            return statusMapping[status]
        elif status in REQUEST_STATE_LIST:
            # if reqmgr status passed return reqmgr status
            return status
        else:
            # unknown status
            return None

    def _reqMgrToWorkQueueStatus(self, status):
        """Map ReqMgr status to that in a WorkQueue element, it is not a 1-1 relation"""
        statusMapping = {'acquired': ['Acquired'],
                         'running': ['Running'],
                         'running-open': ['Running'],
                         'running-closed': ['Running'],
                         'failed': ['Failed'],
                         'aborted': ['Canceled', 'CancelRequested'],
                         'force-complete': ['Canceled', 'CancelRequested'],
                         'completed': ['Done']}
        if status in statusMapping:
            return statusMapping[status]
        else:
            return []

    def reportElement(self, element):
        """Report element to ReqMgr"""
        self.reportRequestStatus(element['RequestName'], element['Status'])

if __name__ == "__main__":
    
    gq = globalQueue(**queueParams)

    wqReqMgr = WorkQueueReqMgrInterface(reqmgr2_endpoint=REQMGR2)
    #import pdb
    #pdb.set_trace()
    wqReqMgr.report(gq, "request prozober_ACDC0_EXO-RunIISpring16DR80-04683_00676_v0__161031_101230_8231")
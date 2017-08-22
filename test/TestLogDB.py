from WMCore.Services.LogDB.LogDB import LogDB
import logging
from pprint import pprint

apps = ["global_workqueue", "reqmgr", "wmstats"]

for appName in apps:
    logdb = LogDB("https://cmsweb.cern.ch/couchdb/wmstats_logdb", appName, 
                  logging, thread_name = "HeartbeatMonitor")
    a = logdb.heartbeat_report()
    pprint(dict(a))
    b = logdb.backend.get_by_thread("HEARTBEAT", "info")
    pprint(dict(b))
    print logdb.delete(mtype="error", this_thread=True)
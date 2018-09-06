#!/usr/bin/env python

import requests
import os
from pprint import pprint
#from WMCore.Services.WMStatsServer.WMStatsServer import WMStatsServer

wmstatsurl = "https://cmsweb.cern.ch/wmstatsserver/data/filtered_requests"

query = "?mask=OutputModulesLFNBases&mask=RequestStatus"

lostFiles = "/Users/sryu/WorkArea/t1_de_kit/t1_de_kit_disk_lost_files_unmerged.txt"
recoveredFiles = "/Users/sryu/WorkArea/t1_de_kit/recovered_files_unmerged.txt"

def getData():
    print(os.getenv('X509_USER_CERT'))
    r = requests.get("%s%s" % (wmstatsurl, query),
                     cert=("/Users/sryu/vmcontrol/usercert.pem",
                           "/Users/sryu/vmcontrol/userkey.pem"),
                     verify=False,
                     headers={"Accept": "application/json"})
    data = r.json()['result']
    pprint(len(data))
    return data



baseurl = "https://cmsweb.cern.ch"
wmstats_url = "%s/wmstatsserver" % baseurl
#wmstats = WMStatsServer(wmstats_url)
#data = wmstats.getFilteredActiveData({}, ["OutputModulesLFNBases","RequestStatus"])


def findWF(data, st, out):
    requests = {}
    for info in data:
        if info["OutputModulesLFNBases"]:
            for t in info.get("OutputModulesLFNBases", []):
                if t in st:
                    if not requests.get(info["RequestName"]):
                        print info["RequestName"], info["RequestStatus"]
                        out.write("%s %s  %s\n" % (info["RequestName"], info["RequestStatus"], st))
                        requests[info["RequestName"]] = True

def getDeletedUnmerged(filePath):
    count = 0
    unmerged = set()
    with open(filePath) as um:
        for line in um:
            a = line.split('/')
            b = '/'. join(a[:-2])
            unmerged.add(b)
            count += 1
    print("Total files : %s" % count)
    #print("Common unmerged base: %s" % len(unmerged))
    return unmerged

def getStillMissingUnmerged(stillMissing):
    count = 0
    unmerged = set()
    for line in stillMissing:
        a = line.split('/')
        b = '/'. join(a[:-2])
        unmerged.add(b)
        count += 1
    print("Total still missing files : %s" % count)
    #print("Common sitll missing unmerged base: %s" % len(unmerged))
    return unmerged

def getFiles(filePath):
    files = set()
    with open(filePath) as um:
        for line in um:
            files.add(line)
    return files

missing = getFiles(lostFiles)
print("Missing %s" % len(missing))

recovered = getFiles(recoveredFiles)
print("Recovered %s" % len(recovered))

stillMissing = missing - recovered
print("Still Missing %s" % len(stillMissing))

data = getData()

#unmerged = getDeletedUnmerged(lostFiles)
#with open("recover_wf.txt", "w") as out:
#    for s in unmerged:
#        findWF(data, s, out)

unmerged = getStillMissingUnmerged(stillMissing)
with open("recover_missing_wf.txt", "w") as out:
    for s in unmerged:
        findWF(data, s, out)

#pprint(data)
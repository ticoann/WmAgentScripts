from pprint import pprint
from Utils.IterTools import grouper
from WMCore.Services.WMArchiver.DataMap import createArchiverDoc
from WMCore.Services.WMArchiver.WMArchiver import WMArchiver
from WMCore.Services.FWJRDB.FWJRDBAPI import FWJRDBAPI

baseURL = "http://localhost:5984"
dbname = "fwjrs_vocms0310"
fwjrAPI = FWJRDBAPI(baseURL, dbname)
wmarchiver = WMArchiver("https://vocms013.cern.ch/wmarchive")
    
def injectDocsToWMArhcive(limit, skip):    
    options = {'limit': limit, 'skip' :skip, 'include_docs':True}
    data = fwjrAPI.couchDB.allDocs(options)['rows']
    
    totalUpdated = 0
    for slicedData in grouper(data, 500):
        jobIDs = []
        archiverDocs = []
        for job in slicedData:
            if job.get('doc') and job['doc']['jobtype'] != "Cleanup":
                #pprint(job['doc']['jobtype'])
                doc = createArchiverDoc(job)
                #pprint(doc)
                archiverDocs.append(doc)
                jobIDs.append(job["id"])
    
        response = wmarchiver.archiveData(archiverDocs)
        totalUpdated += len(jobIDs)
    #pprint(totalUpdated)
    return totalUpdated

numDoc = 100000
limit = 2000
skip = 0
totalUpdated = 0

while totalUpdated < numDoc:
    injected = injectDocsToWMArhcive(limit, skip)
    skip += limit
    totalUpdated += injected
    print "so far %s" % totalUpdated
print "total %s" % totalUpdated
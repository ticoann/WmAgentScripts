from Utils.IterTools import grouper
from WMCore.Services.WMArchiver.DataMap import createArchiverDoc
from WMCore.Services.WMArchiver.WMArchiver import WMArchiver
from WMCore.Services.FWJRDB.FWJRDBAPI import FWJRDBAPI

baseURL = "http://localhost:5984"
dbname = "fwjrs_vocms0308"
fwjrAPI = FWJRDBAPI(baseURL, dbname)
wmarchiver = WMArchiver("https://vocms013.cern.ch/wmarchive")
data = fwjrAPI.getFWJRByArchiveStatus('ready', limit=100)['rows']
print data

for slicedData in grouper(data, 100):
    jobIDs = []
    archiverDocs = []
    for job in slicedData:
        doc = createArchiverDoc(job)
        archiverDocs.append(doc)
        jobIDs.append(job["id"])
        
    response = wmarchiver.archiveData(archiverDocs)

    # Partial success is not allowed either all the insert is successful of none is successful.
    if response[0]['status'] == "ok" and len(response[0]['ids']) == len(jobIDs):
        for docID in jobIDs:
            fwjrAPI.updateArchiveUploadedStatus(docID)
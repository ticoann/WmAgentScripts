from WMCore.ACDC.DataCollectionService import DataCollectionService
from pprint import pprint

acdcUrl = "https://cmsweb.cern.ch/couchdb"
acdcDB = "acdcserver"
dcs = DataCollectionService(url=acdcUrl, database=acdcDB)
collName = "fabozzi_Run2016G-v1-Charmonium-07Aug17_8029_170831_190446_6157"
filesetName = "/fabozzi_Run2016G-v1-Charmonium-07Aug17_8029_170831_190446_6157/DataProcessing"
# for item in dcs._getFilesetInfo(collName, filesetName):
for item in dcs.getChunkFiles(collName, filesetName, None):
    # pprint(item)
    if item["lfn"] == "/store/data/Run2016G/Charmonium/RAW/v1/000/278/875/00000/92E93157-7363-E611-AFF0-02163E011CC8.root":
        pprint(item)
        for run in item['runs']:
            pprint(run.eventsPerLumi)

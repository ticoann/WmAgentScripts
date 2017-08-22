from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
url = "https://cmsweb-testbed.cern.ch/reqmgr2"
raux = ReqMgrAux(url)


from WMCore.Database.CMSCouch import Database
from WMCore.ReqMgr.DataStructs.ReqMgrConfigDataCache import ReqMgrConfigDataCache
couchurl = "https://cmsweb-testbed.cern.ch/couchdb"
aux_db = Database("reqmgr_auxiliary", couchurl)
ReqMgrConfigDataCache.set_aux_db(aux_db)
print ReqMgrConfigDataCache.putDefaultConfig()
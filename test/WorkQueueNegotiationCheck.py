'''
Created on Feb 23, 2015

@author: sryu
'''

wqElementsIDs = ['32691d673f558849783334084780d591', '34babccaf8c4ad0d825dceec22b964ce', '4c9c42430006ddbfa7ad4c2435715b31', 
 '550beb5d42cb85dc04d8ef2d3170254a', '67f9e16e2d9b90c81612fa5d048956c2', '80145859e1128d2d6ab5b2f0e29eeee8', 
 '88fc422d409d06f65c576449ab9e3b0f', 'b26eedc12371b44506dc76116ff854a2', 'ea8499c439ce11d90cb42599732a4151', 
 'ed1a390ec478a61082023e18fd180405', 'ef6c85536c33300fc2554186019c65b3', 'ef7032cd5804876248e6877cf460dce8', 
 'f430064b53a33eb3ef473f667d9d8a97', 'fbf84e5176ba8fefd898f89311f4adfd']


from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue

             
class WorkQueueDebug(object):

    """
    API for dealing with retrieving information from WorkQueue DataService
    """

    def __init__(self, couchURL, dbName = None):
        # if dbName not given assume we have to split
        if not dbName:
            couchURL, dbName = splitCouchServiceURL(couchURL)
        self.hostWithAuth = couchURL
        self.server = CouchServer(couchURL)
        self.db = self.server.connectDatabase(dbName, create = False)
        self.defaultOptions = {'stale': "update_after"}
        
    def getElementByStatus(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'elementsByStatus', self.defaultOptions)
        #return [x['key'] for x in data.get('rows', [])]
        return data['rows']
    
    def getElementByIDs(self, ids):
        if len(ids) == 0:
            return None
    
        docs = self.db.allDocs(options={"include_docs": True}, keys=ids)['rows']
        for j in docs:
            ele = j["doc"]['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']
            print "%s:%s" % (ele['ParentQueueId'], ele['Status'])

if __name__ == "__main__":
    baseUrl = "https://cmsweb.cern.ch/couchdb"
    wqUrl = "%s/workqueue" % baseUrl
    wqSvc = WorkQueueDebug(wqUrl)
    rows = wqSvc.getElementByIDs(wqElementsIDs)
    #wqSvc2 = WorkQueue(wqUrl)
    #wqSvc2.updateElements(*wqElementsIDs, Status = "Available")
                    

    print "done"
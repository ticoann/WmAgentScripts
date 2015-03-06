from WMCore.WMSpec.WMWorkload import WMWorkloadHelper


def fixTrustSiteListFromSpec(baseUrl, request):
    wh = WMWorkloadHelper()
    reqmgrSpecUrl = "%s/reqmgr_workload_cache/%s/spec" % (baseUrl, request)
    
    wh.load(reqmgrSpecUrl)
    tasks = wh.getAllTasks()
    for task in tasks:
        print task.name()
        task.setTrustSitelists(False)
    wh.saveCouch(baseUrl, "reqmgr_workload_cache")
     
    wh.load(reqmgrSpecUrl)
    tasks = wh.getAllTasks()
    for task in tasks:
        print task.name()
        print task.trustSitelists()
    
    return None
    
if __name__ == "__main__":
    baseUrl = "https://cmsweb.cern.ch/couchdb"
    url = "%s/wmstats" % baseUrl
    
    #outDS = fixTrustSiteListFromSpec(baseUrl, "arichard_MCFromGEN_PhEDEx_TEST_Alex_141218_115858_1421")
    
    wfToFix = ["pdmvserv_B2G-Summer12DR53X-00837_00372_v0__150202_131855_223",            
               "pdmvserv_TRK-2019GEMUpg14DR-00005_00069_v0__150201_161509_1516",            
               "pdmvserv_JME-2019GEMUpg14DR-00024_00069_v0__150201_161442_6829",           
               "pdmvserv_HIG-2019GEMUpg14DR-00044_00069_v0__150201_161438_7445",
               "pdmvserv_HIG-2019GEMUpg14DR-00045_00069_v0__150201_161434_9444",            
               "pdmvserv_HIG-2019GEMUpg14DR-00048_00069_v0__150201_161423_4090",            
               "pdmvserv_HIG-2019GEMUpg14DR-00050_00069_v0__150201_161416_147",            
               "pdmvserv_HIG-2019GEMUpg14DR-00051_00069_v0__150201_161411_9330",            
               "pdmvserv_HIG-2019GEMUpg14DR-00053_00069_v0__150201_161403_8727",            
               "pdmvserv_EXO-2019GEMUpg14DR-00019_00069_v0__150201_161400_761"
             ]
    
    wfToFix = ["pdmvserv_MUO-2019GEMUpg14DR-00043_00069_v0__150201_161449_9569",            
               "pdmvserv_JME-2019GEMUpg14DR-00025_00069_v0__150201_161446_2957",            
               "pdmvserv_HIG-2019GEMUpg14DR-00046_00069_v0__150201_161431_3347",           
               "pdmvserv_HIG-2019GEMUpg14DR-00047_00069_v0__150201_161427_2578",
               "pdmvserv_HIG-2019GEMUpg14DR-00052_00069_v0__150201_161407_8336",            
               "pdmvserv_EXO-2019GEMUpg14DR-00016_00069_v0__150201_161347_1485",            
               "pdmvserv_BTV-2019GEMUpg14DR-00010_00069_v0__150201_161339_3302",            
               "pdmvserv_BTV-2019GEMUpg14DR-00009_00069_v0__150201_161333_8418",            
               "pdmvserv_JME-2019GEMUpg14DR-00023_00068_v0__150201_155841_4256"
             ]
    
    for wf in wfToFix:
        fixTrustSiteListFromSpec(baseUrl, wf)

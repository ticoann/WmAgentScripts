from pprint import pprint
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
block = '/QCD_Pt_470to600_TuneCUETP8M1_13TeV_pythia8/RunIISummer15GS-MCRUN2_71_V1-v1/GEN-SIM#2e5a783c-7962-11e5-9a2d-a0369f23d050'
block = '/SingleElectron/Run2012D-v1/RAW#f421edd0-3a9e-11e2-8e2f-842b2b4671d8'
block = '/MET/Run2015B-05Aug2015-v1/DQMIO'
ps = PhEDEx()
sd = SiteDBJSON()

res = ps.getSubscriptionMapping(block)
pprint(res)
res = ps.getReplicaInfoForBlocks(dataset=[block])['phedex']
for block in res['block']:
    nodes = [replica['node'] for replica in block['replica']]
pprint(nodes)

sites = sd.PNNstoPSNs(nodes)

pprint(sites)
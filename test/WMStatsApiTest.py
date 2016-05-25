from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from pprint import pprint

base_url = "https://cmsweb.cern.ch"
reqmgr_url = "%s/reqmgr2" % base_url
wmstats_url = "%s/couchdb/wmstats" % base_url

wmstats = WMStatsReader(wmstats_url)
result = wmstats.agentsByTeam(False)

print result
result = wmstats.agentsByTeam(True)
print result
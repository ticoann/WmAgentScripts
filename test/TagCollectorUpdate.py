from WMCore.Services.TagCollector.TagCollector import TagCollector
from pprint import pprint
#print _get_all_scramarchs_and_versions("https://cmssdt.cern.ch/SDT/cgi-bin/ReleasesXML?anytype=1")
TC = TagCollector()
pprint(dict(TC.releases_by_architecture()))
#print update_software2()
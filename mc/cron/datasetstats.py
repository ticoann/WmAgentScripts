#!/usr/bin/env python
import optparse
import simplejson as json
from dbs.apis.dbsClient import DbsApi
import datetime
import re
import sys

def human(n):
        if n<1000:
                return "%s" % n
        elif n>=1000 and n<1000000:
                order = 1
        elif n>=1000000 and n<1000000000:
                order = 2
        else:
                order = 3

        norm = pow(10,3*order)

        value = float(n)/norm

        letter = {1:'k',2:'M',3:'G'}
        return ("%.1f%s" % (value,letter[order])).replace(".0", "")

def loadcampaignconfig(f):
        try:
                d = open(f).read()
        except:
                print "Cannot load config file %s" % f
                sys.exit(1)
        try:
                s = eval(d)
        except:
                print "Cannot eval config file %s " % f
                sys.exit(1)
        print "\nConfiguration loaded successfully from %s" % f
        #for c in s.keys():
        #       print "%s" % c
        #       for i in s[c]:
        #               print "\t%s\t%s" % (i,s[c][i])
        return s

def main():
        parser = optparse.OptionParser()
        parser.set_defaults(dataset='')
        parser.add_option('--json', help='JSON format',dest='json',action="store_true")
        parser.add_option('--debug', help='debug (verbose) mode',dest='debug',action="store_true")

        (options,args) = parser.parse_args()

	outputformat='text'
	if options.json:
		outputformat='json'

	campaignconfig = loadcampaignconfig('/afs/cern.ch/user/c/cmst2/mc/config/campaign.cfg')
	api = DbsApi("https://cmsweb.cern.ch/dbs/prod/global/DBSReader")
	datasets = {}
	datasets2 = {}
	datasetsdetails = {}
	events = {}
	for status in ['PRODUCTION','VALID','INVALID']:
		datasets[status] = []
		sys.stdout.write("Get datasets in status = %s\n" % status)
		ret = api.listDatasets(dataset_access_type=status,data_tier_name='GEN-SIM',detail=0)
		#ret = ret[1:100]
		le = len(ret)
		sys.stdout.write("Got %s datasets" % le)
		count = 1
		for d in ret:
			dataset = d['dataset']
			sys.stdout.write("%s/%s %s\n" % (count,le,dataset))
			count = count + 1
			if dataset.split('/')[2].split('-')[0] in campaignconfig.keys():
				#ret =  api.listDatasets(dataset=dataset,dataset_access_type=status,detail=1)
				ret =  api.listFileSummaries(dataset=dataset)
				datasetsdetails[dataset] = ret[0]
				c = dataset.split('/')[2].split('-')[0]
				if c not in events.keys():
					events[c] = {} 
					for st in ['PRODUCTION','VALID','INVALID']:
						events[c][st] = 0
				events[c][st] = events[c][st] + datasetsdetails[dataset]['num_event']
	
	stats = {'stats':events}
	f=open('/afs/cern.ch/user/c/cmst2/www/mc/dbs.json','w')
	f.write(json.dumps(stats,indent=4,sort_keys=True))
	f.close()

	sys.exit(0)

if __name__ == "__main__":
        main()

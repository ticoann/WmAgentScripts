#!/usr/bin/env python
import sys,urllib,urllib2,re,time,os
try:
    import json
except ImportError:
    import simplejson as json
import optparse
import httplib
import datetime
import time
import shutil
import os.path
from xml.dom import minidom
import zlib
import re
import math

cachedoverview = '/afs/cern.ch/user/c/cmst2/public/overview.cache'

def getRequestsByTypeStatus(rtype,rstatus):
	global overview
	r = []
	for i in overview:
		if 'type' in i.keys():
			t = i['type']
		else:
			t = None
		if 'status' in i.keys():
			st = i['status']
		else:
			st = None
		if t in rtype and st in rstatus and t != '' and i['request_name'] not in r:
			#print "t = %s st = %s r = %s" % (t,st,i['request_name'])
			r.append(i['request_name'])
	return r
	
def getInputDataset(workflow):
	conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
	r2=conn.getresponse()
	data = r2.read()
	s = json.loads(data)
	conn.close()

	inputdatasetname = s['InputDatasets'][0]
	return inputdatasetname

def loadcampaignconfig(f):
        try:
                d = open(f).read()
        except:
                print "Cannot load config file %s" % f
                sys.exit(1)
        try:
                s = eval(d.strip())
		if '' in s:
			print "XXXXXXXXX"
			s.remove('')
        except:
                print "Cannot eval config file %s " % f
                sys.exit(1)
        print "\nConfiguration loaded successfully from %s" % f
        #for c in s.keys():
        #       print "%s" % c
        #       for i in s[c]:
        #               print "\t%s\t%s" % (i,s[c][i])
        return s

def loadwrblacklist(f):
	if not os.path.exists(f):
		return []
        try:
                d = open(f).read()
        except:
                print "Cannot load blacklist file %s" % f
                sys.exit(1)
	s = d.split('\n')
	if '' in s:
		s.remove('')
	return s
	
def getoverview():
	global cachedoverview
        if not os.path.exists(cachedoverview):
		print "Cannot get overview from %s" % cachedoverview
		sys.exit(1)
	d = open(cachedoverview).read()
        s = eval(d)
        return s


def dbs_get_gensiminfo(campaign):
	#ret.append({'primds':s['primary_ds_name'],'dataset':dataset,'events':s['num_event'],'lastmodts':s['last_modification_date'],'campaign':campaign})
	return ret

def main():
	global overview,forceoverview,pledged,allacdc
	
	campaignconfig = loadcampaignconfig('/afs/cern.ch/user/c/cmst2/mc/config/campaign.cfg')
	blacklist = loadwrblacklist('/afs/cern.ch/user/c/cmst2/www/mc/wr-blacklist.txt')
	validcampaigns = []
	for campaign in campaignconfig.keys():
		if 'tiers' in campaignconfig[campaign].keys():
			if 'GEN-SIM' not in campaignconfig[campaign]['tiers']:
				continue
		validcampaigns.append(campaign)

	output=os.popen("ps aux | grep create-gen-sim-wr.py | grep -v grep").read().split('\n')
	if len(output)>2:
		pass
		#sys.exit(0)

	print "Get PhEDEx info"
	url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/subscriptions?custodial=y&move=y&dataset=/*/*/GEN-SIM&&create_since=%s' % (datetime.date.today()-datetime.timedelta(days=180)).strftime("%s")
	result = json.load(urllib.urlopen(url))
	phinfo = {}
	for i in result['phedex']['dataset']:
		sites = []
		for j in i['subscription']:
			node = j['node'].replace('_MSS','')
			sites.append(node)
		phinfo[i['name']] = sites

	
	print "Get AODSIM list"
	q = '/afs/cern.ch/user/c/cmst2/mc/scripts/dbs3wrapper.sh /afs/cern.ch/user/c/cmst2/mc/scripts/datasetlist.py --tier=AODSIM --status=VALID --json'
	output = os.popen(q).read()
	s = json.loads(output)
	aodsimprimdslist = []
	for i in s:
		aodsimprimdslist.append(i.split('/')[1])

	print "Get GEN-SIM list"
	q = '/afs/cern.ch/user/c/cmst2/mc/scripts/dbs3wrapper.sh /afs/cern.ch/user/c/cmst2/mc/scripts/datasetlist.py --tier=GEN-SIM --status=VALID --json'
	output = os.popen(q).read()
	gensimlist = json.loads(output)
	gensimlist2 = []
	for dataset in gensimlist:
		if dataset.split('/')[2].split('-')[0] in validcampaigns and dataset.split('/')[1] in aodsimprimdslist and dataset in phinfo.keys():
			gensimlist2.append(dataset)

	wr = []
	print "Looping on %s GEN-SIM datasets" % len(gensimlist2)
	c = 1
	for i in gensimlist2:
		print "%s %s" % (c,i)
		c = c + 1 
		if i in blacklist:
			continue
		q = '/afs/cern.ch/user/c/cmst2/mc/scripts/dbs3wrapper.sh /afs/cern.ch/user/c/cmst2/mc/scripts/datasetinfo.py --dataset=%s --ldate --json' % i
		output = os.popen(q).read()
		print output
		s = json.loads(output)
		if s['ldate'] > int((datetime.date.today()-datetime.timedelta(days=20)).strftime("%s")):
			elem = {}
			elem['events'] = s['num_event']
			elem['campaign'] = s['dataset'].split('/')[2].split('-')[0]
			elem['sites'] = phinfo[i]
			wr.append(elem)
		else:
			blacklist.append(i)
			f = open('/afs/cern.ch/user/c/cmst2/www/mc/wr-blacklist.txt','a')
			f.write("%s\n" % i)
			f.close()

	print "Count: %s" % len(wr)
	f = open('/afs/cern.ch/user/c/cmst2/www/mc/gen-sim-wr.json','w')
	f.write(json.dumps(wr,indent=4,sort_keys=True))
	f.close()

if __name__ == "__main__":
        main()


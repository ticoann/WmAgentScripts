import requests
import json
import os

proxy_info = os.environ['X509_USER_PROXY']

try:
 requestCache = requests.get("https://cmsweb.cern.ch/t0_reqmon/data/requestcache",cert=(proxy_info, proxy_info),verify=False).text
except:
 print "Couldn't download RequestCache, goodbye cruel world"
 exit(666)

try:
 requestCacheJsonObj = json.loads(requestCache)
except:
 print "Couldn't decode RequestCache, goodbye, goodbye, goodbye"
 exit(666)

workflowsWithPausedJobs = []
for result in requestCacheJsonObj['result']:
    for  workflowName, workflowObj in result.iteritems():
     agentJobInfo = workflowObj.get('AgentJobInfo',{})
     for agentName, agentObj in agentJobInfo.iteritems():
         #print agentObj.get('status')
         pausedJobs = agentObj.get('status',{}).get('paused',{}).get('job',0)
         if pausedJobs > 0:
             workflowsWithPausedJobs.append(workflowName)

jobPausedArray = []

for workflowWithPausedJob in workflowsWithPausedJobs:
    #print workflowWithPausedJob
    jsontext  = requests.get("https://cmsweb.cern.ch/t0_reqmon/data/jobdetail/%s?sample_size=9223372036854775806" % (workflowWithPausedJob.strip()),cert=(proxy_info, proxy_info),verify=False).text
    jsonobj = json.loads(jsontext)
    for result in jsonobj['result']:
     for workflowName, workflowObj in result.iteritems():
         for taskName, taskObj in workflowObj.iteritems():
             jobPaused = taskObj.get('jobpaused', {})
             for errorCode, jobDataObj in jobPaused.iteritems():
                 for siteName, siteObj in jobDataObj.iteritems():
                    errorCount = siteObj.get('errorCount',0)
                    samples = siteObj.get('samples', [])
                    for sample in samples:
                        jobPausedArray.append(sample)

for job in jobPausedArray:
 [job['wmbsid'], job['exitcode'], job['workflow'], job['site'], job['retrycount'], job['task'], job['jobtype']]
 print job['workflow'], job['wmbsid'], job['retrycount'], job['exitcode']
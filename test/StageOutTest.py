from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.Execute import runCommand
import pdb
testS = StageOutImpl()

testS.executeCommand("srm a b")
# pdb.set_trace()
# runCommand("cp a b")

testS.executeCommand("""#!/bin/bash
env -i X509_USER_PROXY=$X509_USER_PROXY JOBSTARTDIR=$JOBSTARTDIR bash -c '. $JOBSTARTDIR/startup_environment.sh; date; gfal-copy -t 2400 -T 2400 -p -K adler32  file:///srv/localstage/scratch/3973827.1.grid.q/lBlMDmDVgbqn3dFDVpGiSQRqaTsoMnABFKDmaQFKDmABFKDmR5G7Sm/glide_wlTvdU/execute/dir_16476/job/WMTaskSpace/logArch1/logArchive.tar.gz srm://gfe02.grid.hep.ph.ic.ac.uk:8443/srm/managerv2?SFN=/pnfs/hep.ph.ic.ac.uk/data/cms/store/unmerged/logs/prod/2017/6/5/pdmvserv_task_EXO-RunIISummer15GS-10439__v1_T_170602_201712_7572/EXO-RunIISummer15GS-10439_0/EXO-RunIISummer15GS-10439_0MergeRAWSIMoutput/EXO-RunIISummer16DR80Premix-09381_0/EXO-RunIISummer16DR80Premix-09381_1/0000/0/0d7c6fe2-49b5-11e7-be85-002590494fb0-1-0-logArchive.tar.gz'""")
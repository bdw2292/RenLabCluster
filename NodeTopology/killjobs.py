import os
import sys
import subprocess
cpunodepath='nodeinfo.txt'

def ReadCPUNodes(cpunodepath):
    cpunodes=[]
    temp=open(cpunodepath,'r')
    results=temp.readlines()
    temp.close()
    for line in results:
        if '#' not in line:
            linesplit=line.split()
            card=linesplit[0]
            node=card[:-2]
            cpunodes.append(node)
    return cpunodes


def KillSignal(cpunodes,killstring):
    for node in cpunodes:
        cmdstr='ssh %s "%s"'%(node,killstring)
        p = subprocess.Popen(cmdstr, stdout=subprocess.PIPE,shell=True)


cpunodes=ReadCPUNodes(cpunodepath)
killstring='cd /scratch/bdw2292/ ; find . -mtime +3 | xargs rm -r '
KillSignal(cpunodes,killstring)
killstring="pkill -U bdw2292"
KillSignal(cpunodes,killstring)
Killstring="ps aux | grep bdw2292 | grep python | grep amoeba | awk '{ print $2 }' | xargs pwdx | xargs kill -9"
KillSignal(cpunodes,killstring)
Killstring="ps aux | grep bdw2292 | grep python | grep poltype.py | awk '{ print $2 }' | xargs pwdx| xargs kill -9"
KillSignal(cpunodes,killstring)
Killstring="ps aux | grep bdw2292 | grep psi4 | awk '{ print $2 }' | xargs pwdx| xargs kill -9"
KillSignal(cpunodes,killstring)

Killstring="ps aux | grep bdw2292 | grep work_queue_worker | awk '{ print $2 }' | xargs pwdx| xargs kill -9"
KillSignal(cpunodes,killstring)
Killstring="ps -fu bdw2292 | awk '{ print $2 }' | xargs kill -9"
KillSignal(cpunodes,killstring)


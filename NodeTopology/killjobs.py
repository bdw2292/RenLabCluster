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
killstring='pkill -U bdw2292 '
KillSignal(cpunodes,killstring)

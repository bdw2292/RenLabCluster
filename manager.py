import os
import sys
import subprocess
import time
import shutil
import getopt
import traceback
import logging
import random
import signal

global queueloggerfile
global errorloggerfile
global waitingloggerfile
global completedloggerfile
global runningloggerfile

waittime=15 # seconds
startingportnumber=9123
nodelistfilepath=os.path.join('NodeTopology','nodeinfo.txt')
masterhost='nova'
envpath='/home/bdw2292/.allpurpose.bashrc'
jobinfofilepath=None
pidfile='daemon.pid'
canceltaskid=None
canceltasktag=None
thedir= os.path.dirname(os.path.realpath(__file__))+r'/'
projectname=None
password='secret'
queueloggerfile='queuelogger.log'
errorloggerfile='errorlogger.log'
workerdir='/scratch'
waitingloggerfile='waiting.log'
completedloggerfile='completed.log'
runningloggerfile='running.log'
backupmanager=False
usernametoemaillist=os.path.join('NodeTopology','usernamestoemail.txt')
startworkers=False
username=None
runallusers=False
timetokillworkers=15*60 # minutes
opts, xargs = getopt.getopt(sys.argv[1:],'',["bashrcpath=","jobinfofilepath=","canceltaskid=","canceltasktag=",'projectname=','password=','startingportnumber=','workerdir=','backupmanager','startworkers','username=','timetokillworkers=','runallusers='])
for o, a in opts:
    if o in ("--bashrcpath"):
        envpath=a
    elif o in ("--jobinfofilepath"):
        jobinfofilepath=a
    elif o in ("--canceltaskid"):
        canceltaskid=a
    elif o in ("--canceltasktag"):
        canceltasktag=a
    elif o in ("--password"):
        password=a
    elif o in ("--username"):
        username=a
    elif o in ("--projectname"):
        projectname=a
    elif o in ("--startingportnumber"):
        startingportnumber=int(a)
    elif o in ("--workerdir"):
        workerdir=a
    elif o in ("--backupmanager"):
        backupmanager=True
    elif o in ("--runallusers"):
        runallusers=True
    elif o in ("--startworkers"):
        startworkers=True
    elif o in ("--timetokillworkers"):
        timetokillworkers=float(a)*60



def ReadNodeList(nodelistfilepath,usernames):
    
    cardtoallowedcpuusernames={}
    cardtoallowedgpuusernames={}
    nodelist=[]
    nodetocards={}
    nodetousableram={}
    nodetousabledisk={}
    nodetousableproc={}
    nodetolowram={}
    nodetolowdisk={}
    nodetolowproc={}
    nodetoallowedcpuusernames={}
    nodetoallowedgpuusernames={}
    nodetohasgpu={}

    usernametonodetousableram={}
    usernametonodetousabledisk={}
    usernametonodetousableproc={}
    usernametonodetocardcount={}
    for username in usernames:
        usernametonodetousableram[username]={}
        usernametonodetousabledisk[username]={}
        usernametonodetousableproc[username]={}
        usernametonodetocardcount[username]={}


    lowconsumratio=.2
    while not os.path.isfile(nodelistfilepath):
        time.sleep(1)
    temp=open(nodelistfilepath,'r')
    results=temp.readlines()
    temp.close()
    for line in results:
        linesplit=line.split()
        if len(linesplit)<1:
            continue
        newline=line.replace('\n','')
        if '#' not in line:
            linesplit=newline.split()
            card=linesplit[0]
            node=card[:-2]
            cardvalue=int(card[-1])
            if node not in nodelist:
                nodelist.append(node)
            hasgpu=linesplit[1]
            if hasgpu=="GPU":
                nodetohasgpu[node]=True
            else:
                nodetohasgpu[node]=False
            proc=linesplit[3]
            ram=linesplit[4]
            scratch=linesplit[5]
            coreconsumratio=float(linesplit[6])
            ramconsumratio=float(linesplit[7])
            diskconsumratio=float(linesplit[8])
            if len(linesplit)>=9+1:
                cpuusername=linesplit[9]
            else:
                cpuusername='ANYUSER'
            if len(linesplit)>=10+1:
                gpuusername=linesplit[10]
            else:
                gpuusername='ANYUSER'


            if proc!='UNK':
                proc=str(int(int(proc)*coreconsumratio))
                lowproc=str(int(int(proc)*lowconsumratio))
            else:
                lowproc=proc
            
            if ram!='UNK':
                ram=str(int(int(ram)*ramconsumratio))
                lowram=str(int(int(ram)*lowconsumratio))
            else:
                lowram=ram

            if scratch!='UNK':
                scratch=str(int(int(scratch)*diskconsumratio))
                lowscratch=str(int(int(scratch)*lowconsumratio))
            else:
                lowscratch=scratch


            nodetousableram[node]=ram
            nodetousabledisk[node]=scratch
            nodetousableproc[node]=proc
            nodetolowram[node]=lowram
            nodetolowdisk[node]=lowscratch
            nodetolowproc[node]=lowproc

            if node not in nodetocards.keys():
                nodetocards[node]=[]
            nodetocards[node].append(card)
            cardtoallowedcpuusernames[card]=[]
            cardtoallowedgpuusernames[card]=[]
            if cpuusername=='ANYUSER':
                for username in usernames: 
                    if username not in cardtoallowedcpuusernames[card]:
                        cardtoallowedcpuusernames[card].append(username)
            else:
                if cpuusername not in cardtoallowedcpuusernames[card]:
                    cardtoallowedcpuusernames[card].append(cpuusername)
            if gpuusername=='ANYUSER':
                for username in usernames: 
                    if username not in cardtoallowedgpuusernames[card]:
                        cardtoallowedgpuusernames[card].append(username)
            else:
                if gpuusername not in cardtoallowedgpuusernames[card]:
                    cardtoallowedgpuusernames[card].append(gpuusername)
    for node,cards in nodetocards.items():
        usernametocardcount={}
        usableram=nodetousableram[node]
        usablescratch=nodetousabledisk[node]  
        proc=nodetousableproc[node]
        lowram=nodetolowram[node]
        lowdisk=nodetolowdisk[node]
        lowproc=nodetolowproc[node]
        allallowedcpuusernames=[]
        allallowedgpuusernames=[]
        hasgpu=nodetohasgpu[node]
        for card in cards:
            allowedcpuusernames=cardtoallowedcpuusernames[card]
            for cpuusername in allowedcpuusernames:
                if cpuusername not in allallowedcpuusernames:
                    allallowedcpuusernames.append(cpuusername)
            allowedgpuusernames=cardtoallowedgpuusernames[card]
            for gpuusername in allowedgpuusernames:
                if gpuusername not in usernametocardcount.keys():
                    usernametocardcount[gpuusername]=0
                if gpuusername not in allallowedgpuusernames:
                    allallowedgpuusernames.append(gpuusername)
                if hasgpu==True:
                    usernametocardcount[gpuusername]+=1
        nodetoallowedgpuusernames[node]=allallowedgpuusernames
        nodetoallowedcpuusernames[node]=allallowedcpuusernames

        for username in usernames:
            if username in usernametocardcount.keys():
                cardcount= usernametocardcount[username]
            else:
                cardcount=0
            cardcount=str(cardcount)
            usernametonodetocardcount[username][node]=cardcount
            if username in allallowedcpuusernames:
                availram=usableram
                availdisk=usablescratch
                availproc=proc
            else:
                availram=lowram
                availdisk=lowscratch
                availproc=lowproc
            usernametonodetousableram[username][node]=availram
            usernametonodetousabledisk[username][node]=availdisk
            usernametonodetousableproc[username][node]=availproc


    if len(nodelist)==0:
        ReadNodeList(nodelistfilepath,usernames)
    return nodelist,usernametonodetousableproc,usernametonodetousableram,usernametonodetousabledisk,usernametonodetocardcount,nodetoallowedgpuusernames,nodetoallowedcpuusernames


def CallWorker(node,envpath,masterhost,portnumber,proc,ram,disk,projectname,password,cardcount,usernametoqueuenametologgers,usernametoqueuenametolognames,workerdir,username,usernametoqueuenametonodetoworkerpid,queuename):
    fullworkdir=os.path.join(workerdir,username)
    idletimeout=100000000
    cmdstr='work_queue_worker '+str(masterhost)+' '+str(portnumber) 
    cmdstr+=' --workdir '+fullworkdir
    cmdstr+=' -d all -o '+os.path.join(fullworkdir,'worker.debug')
    if proc!='UNK':
        cmdstr+=' '+'--cores '+proc
    if ram!='UNK':
        cmdstr+=' '+'--memory '+ram
    cmdstr+=' '+'--gpus '+str(cardcount)
    cmdstr+=' '+'-t '+str(idletimeout)
    cmdstr+=' '+'-M '+projectname
    #cmdstr+=' '+'--password '+password # for some reason cctools has bug in specifying password
    cmdstr+=' '+'--disk '+str(disk)
    mkdirstring='mkdir '+fullworkdir+' ; '
    cmdstr=mkdirstring+cmdstr
    thedir= os.path.dirname(os.path.realpath(__file__))+r'/'
    cmdstr = 'ssh %s "source %s ;%s"' %(str(node),envpath,cmdstr)
    usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],'Calling: '+cmdstr,usernametoqueuenametolognames[username][queuename],0)
    process = subprocess.Popen(cmdstr, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    pid=process.pid
    usernametoqueuenametonodetoworkerpid[username][queuename][node]=pid
    return usernametoqueuenametologgers,usernametoqueuenametonodetoworkerpid

def CallWorkers(nodelist,envpath,masterhost,usernametoqueuenametoportnumber,usernametonodetousableproc,usernametonodetousableram,usernametonodetousabledisk,usernametoqueuenametoprojectname,usernametoqueuenametopassword,usernametonodetocardcount,usernametoqueuenametologgers,usernametoqueuenametolognames,workerdir,usernametoqueuenametonodetoworkerpid):
    for username,nodetousableproc in usernametonodetousableproc.items():
        nodetousableram=usernametonodetousableram[username]
        nodetousabledisk=usernametonodetousabledisk[username]
        queuenametoprojectname=usernametoqueuenametoprojectname[username]
        queuenametopassword=usernametoqueuenametopassword[username]
        nodetocardcount=usernametonodetocardcount[username]
        queuenametoportnumber=usernametoqueuenametoportnumber[username]
        for node in nodelist:
            for queuename,portnumber in queuenametoportnumber.items():
                if node in nodetousableproc.keys():
                    proc=nodetousableproc[node]
                    ram=nodetousableram[node]
                    disk=nodetousabledisk[node]
                    cardcount=nodetocardcount[node]
                    projectname=queuenametoprojectname[queuename]
                    password=queuenametopassword[queuename]
                    usernametoqueuenametologgers,usernametoqueuenametonodetoworkerpid=CallWorker(node,envpath,masterhost,portnumber,proc,ram,disk,projectname,password,cardcount,usernametoqueuenametologgers,usernametoqueuenametolognames,workerdir,username,usernametoqueuenametonodetoworkerpid,queuename) 
    return usernametoqueuenametologgers,usernametoqueuenametonodetoworkerpid


def ReadJobInfoFromFile(jobinfo,filename):
    if os.path.isfile(filename):
        temp=open(filename,'r')
        results=temp.readlines()
        temp.close()
        for line in results:
            split=line.split()
            if len(split)==0:
                continue
            cmdstr,ram,numproc,inputfilepaths,outputfilepaths,binpath,scratchpath,cache,inputline,disk,username,gpucard,gpujob=ParseJobInfo(line)

            if inputfilepaths==None:
                continue
            if username==None:
                continue
            if cmdstr==None:
                continue
            if ram==None:
                continue
            if disk==None:
                continue
            if numproc==None:
                continue
            convram=ConvertMemoryToMBValue(ram)      
            if gpujob==False:
                if convram<2000:
                    continue    
                if numproc==0:
                    continue
                if 'poltype.py' in cmdstr:
                    if convram<10000:
                        continue
            
            array=['ram','numproc','inputfilepaths','outputfilepaths','binpath','scratchpath','cache','inputline','disk','username','gpucard','gpujob']
            for key in array:
                if key not in jobinfo.keys():
                    jobinfo[key]={}
            job=tuple([cmdstr,tuple(inputfilepaths)])
            jobinfo['ram'][job]=ram
            jobinfo['numproc'][job]=numproc
            jobinfo['inputfilepaths'][job]=inputfilepaths
            jobinfo['outputfilepaths'][job]=outputfilepaths
            jobinfo['binpath'][job]=binpath
            jobinfo['scratchpath'][job]=scratchpath
            jobinfo['cache'][job]=cache
            jobinfo['inputline'][job]=inputline
            jobinfo['disk'][job]=disk
            jobinfo['username'][job]=username
            jobinfo['gpucard'][job]=gpucard
            jobinfo['gpujob'][job]=gpujob


    return jobinfo

def ReadJobInfoFromDic(jobinfo):
    jobtoram={}
    jobtonumproc={}
    jobtoinputfilepaths={}
    jobtobinpath={}
    jobtoscratchpath={}
    jobtooutputfilepaths={}
    jobtocache={}
    jobtoinputline={}
    jobtodisk={}
    jobtousername={}
    jobtogpucard={}
    jobtogpujob={}
    if 'ram' in jobinfo.keys():
        jobtoram=jobinfo['ram']
    if 'numproc' in jobinfo.keys():
        jobtonumproc=jobinfo['numproc']
    if 'inputfilepaths' in jobinfo.keys():
        jobtoinputfilepaths=jobinfo['inputfilepaths']
    if 'binpath' in jobinfo.keys():
        jobtobinpath=jobinfo['binpath']
    if 'scratchpath' in jobinfo.keys():
        jobtoscratchpath=jobinfo['scratchpath']
    if 'outputfilepaths' in jobinfo.keys():
        jobtooutputfilepaths=jobinfo['outputfilepaths']
    if 'cache' in jobinfo.keys():
        jobtocache=jobinfo['cache']
    if 'inputline' in jobinfo.keys():
        jobtoinputline=jobinfo['inputline']
    if 'disk' in jobinfo.keys():
        jobtodisk=jobinfo['disk']
    if 'username' in jobinfo.keys():
        jobtousername=jobinfo['username']
    if 'gpucard' in jobinfo.keys():
        jobtogpucard=jobinfo['gpucard']
    if 'gpujob' in jobinfo.keys():
        jobtogpujob=jobinfo['gpujob']


    return jobtoram,jobtonumproc,jobtoinputfilepaths,jobtobinpath,jobtoscratchpath,jobtooutputfilepaths,jobtocache,jobtoinputline,jobtodisk,jobtousername,jobtogpucard,jobtogpujob

def ConvertMemoryToMBValue(scratch):
    availspace,availunit=SplitScratch(scratch)
    if availunit=='M' or availunit=='MB':
        availspace=float(availspace)
    elif availunit=='T' or availunit=='TB':
        availspace=float(availspace)*1000000
    elif availunit=='G' or availunit=='GB':
        availspace=float(availspace)*1000
    return int(availspace)
     

def SplitScratch(string):
    for eidx in range(len(string)):
        e=string[eidx]
        if not e.isdigit() and e!='.':
            index=eidx
            break
    space=string[:index]
    diskunit=string[index]
    return space,diskunit



def SubmitToQueue(jobinfo,usernametoqueuenametoqueue,usernametoqueuenametotaskidtojob,usernametoqueuenametotaskidtooutputfilepathslist,usernametoqueuenametotaskidtoinputline,usernametoqueuenametologgers,usernametoqueuenametolognames,usernametoqueuenametotaskidtotasktag):
    import work_queue as wq
    jobtoram,jobtonumproc,jobtoinputfilepaths,jobtobinpath,jobtoscratchpath,jobtooutputfilepaths,jobtocache,jobtoinputline,jobtodisk,jobtousername,jobtogpucard,jobtogpujob=ReadJobInfoFromDic(jobinfo)
    for job,ram in jobtoram.items():
        if job!=None:
            numproc=jobtonumproc[job]
            inputfilepaths=jobtoinputfilepaths[job]
            outputfilepaths=jobtooutputfilepaths[job]
            binpath=jobtobinpath[job]
            scratchpath=jobtoscratchpath[job]
            cacheval=jobtocache[job]
            cmdstr=job[0]
            inputline=jobtoinputline[job]
            disk=jobtodisk[job]
            username=jobtousername[job]
            gpucard=jobtogpucard[job]
            gpujob=jobtogpujob[job]
            if gpucard==None:
                queuename=username+'_'+'mainqueue'

            usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],"Submitting tasks...",usernametoqueuenametolognames[username][queuename],0)
            if scratchpath!=None:
                head,tail=os.path.split(scratchpath)
                string1='mkdir '+head+' ; '
                string2='mkdir '+scratchpath+' ; '
                cmdstr=string1+string2+cmdstr
            task = wq.Task(cmdstr)
            if binpath!=None:
                if os.path.isfile(binpath):
                    head,tail=os.path.split(binpath)
                    task.specify_file(binpath, tail, wq.WORK_QUEUE_INPUT, cache=cacheval)
            for inputfile in inputfilepaths:
                if os.path.isfile(inputfile):
                    head,tail=os.path.split(inputfile)
                    task.specify_file(inputfile, tail, wq.WORK_QUEUE_INPUT, cache=cacheval)
            if outputfilepaths!=None:
                for outputfilepath in outputfilepaths:
                    head,outputfile=os.path.split(outputfilepath)
                    task.specify_file(outputfile, outputfile, wq.WORK_QUEUE_OUTPUT, cache=cacheval)
            numproc=int(numproc)
            task.specify_cores(numproc)     
            ram=ConvertMemoryToMBValue(ram)           
            task.specify_memory(ram)    
            disk=ConvertMemoryToMBValue(disk)           
            task.specify_disk(disk)    
            if gpujob==True:
                task.specify_gpus(1)       
                tasktag='GPU' 
            else:
                tasktag='CPU' 
            task.specify_tag(tasktag)
            task.specify_max_retries(2) # if some issue on node, retry on another node

            taskid=str(usernametoqueuenametoqueue[username][queuename].submit(task))
            usernametoqueuenametotaskidtotasktag[username][queuename][taskid]=tasktag
            usernametoqueuenametotaskidtooutputfilepathslist[username][queuename][taskid]=outputfilepaths
            usernametoqueuenametotaskidtoinputline[username][queuename][taskid]=inputline
            usernametoqueuenametotaskidtojob[username][queuename][taskid]=job
            usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],'Task ID of '+taskid+' is assigned to job '+cmdstr,usernametoqueuenametolognames[username][queuename],0)
    return usernametoqueuenametoqueue,usernametoqueuenametotaskidtojob,usernametoqueuenametotaskidtooutputfilepathslist,usernametoqueuenametotaskidtoinputline,usernametoqueuenametotaskidtotasktag


def WriteOutTaskStateLoggingInfo(taskidtoinputline,queue,queuename,username):
    completed=[]
    waiting=[]
    running=[]
    for taskid,inputline in taskidtoinputline.items():
        taskstate=queue.task_state(int(taskid))
        if taskstate==5: # completed
            completed.append(inputline)
        elif taskstate==1: # waiting
            waiting.append(inputline)
        elif taskstate==2: # running
            running.append(inputline)

    os.chdir(username)
    WriteOutFile(completed,queuename+'_'+completedloggerfile,'completed!')
    WriteOutFile(running,queuename+'_'+runningloggerfile,'running!')
    WriteOutFile(waiting,queuename+'_'+waitingloggerfile,'waiting!')
    os.chdir('..')


def WriteOutFile(array,theloggerfile,string):
    temp=open(theloggerfile,'w')
    for line in array:
        temp.write(line+' '+string+'\n')
    temp.close()


def DetectResourceChange(usernametonodetousableresource,prevusernametonodetousableresource):
    change=False
    usernametonodetodifferentusableresource={}
    for username, nodetousableresource in usernametonodetousableresource.items():
        prevnodetousableresource=prevusernametonodetousableresource[username]
        for node,usableresource in nodetousableresource.items():
            if node in prevnodetousableresource.keys():
                prevusableresource=prevnodetousableresource[node]
                if usableresource!=prevusableresource:
                    change=True
                    if username not in usernametonodetodifferentusableresource.keys():
                        usernametonodetodifferentusableresource[username]={}
                    usernametonodetodifferentusableresource[username][node]=usableresource
    

    return change,usernametonodetodifferentusableresource




def FindDifferentUsernameNodes(differentusernametonodelist,usernametonodetodifferentusableresource):
    for username,nodetodifferentusableresource in usernametonodetodifferentusableresource.items():
        for node,usableresource in nodetodifferentusableresource.items():
            if username not in differentusernametonodelist.keys():
                differentusernametonodelist[username]=[]
            if node not in differentusernametonodelist[username]:
                differentusernametonodelist[username].append(node)

    return differentusernametonodelist

def FindDifferentUsernameNodesAllDics(usernametonodetodifferentusableproc,usernametonodetodifferentusableram,usernametonodetodifferentusabledisk,usernametonodetodifferentcardcount):
    differentusernametonodelist={}
    differentusernametonodelist=FindDifferentUsernameNodes(differentusernametonodelist,usernametonodetodifferentusableproc)
    differentusernametonodelist=FindDifferentUsernameNodes(differentusernametonodelist,usernametonodetodifferentusableram)
    differentusernametonodelist=FindDifferentUsernameNodes(differentusernametonodelist,usernametonodetodifferentusabledisk)
    differentusernametonodelist=FindDifferentUsernameNodes(differentusernametonodelist,usernametonodetodifferentcardcount)

    return differentusernametonodelist



def AddUnchangedResources(usernametonodetodifferentresource,usernametonodetoresource,differentusernametonodelist):
    for username,nodelist in differentusernametonodelist.items():
        nodetoresource=usernametonodetoresource[username]
        for node in nodelist:
            resource=nodetoresource[node]
            if username not in usernametonodetodifferentresource.keys():
                usernametonodetodifferentresource[username]={}
            if node not in usernametonodetodifferentresource[username].keys():
                usernametonodetodifferentresource[username][node]=resource 


    return usernametonodetodifferentresource


def AddUnchangedResourcesOnSameNodeAsChangedResource(usernametonodetodifferentusableproc,usernametonodetodifferentusableram,usernametonodetodifferentusabledisk,usernametonodetodifferentcardcount,usernametonodetousableproc,usernametonodetousableram,usernametonodetousabledisk,usernametonodetocardcount):

    differentusernametonodelist=FindDifferentUsernameNodesAllDics(usernametonodetodifferentusableproc,usernametonodetodifferentusableram,usernametonodetodifferentusabledisk,usernametonodetodifferentcardcount)
    usernametonodetodifferentusableproc=AddUnchangedResources(usernametonodetodifferentusableproc,usernametonodetousableproc,differentusernametonodelist)
    usernametonodetodifferentusableram=AddUnchangedResources(usernametonodetodifferentusableram,usernametonodetousableram,differentusernametonodelist)
    usernametonodetodifferentusabledisk=AddUnchangedResources(usernametonodetodifferentusabledisk,usernametonodetousabledisk,differentusernametonodelist)
    usernametonodetodifferentcardcount=AddUnchangedResources(usernametonodetodifferentcardcount,usernametonodetocardcount,differentusernametonodelist)

    return usernametonodetodifferentusableproc,usernametonodetodifferentusableram,usernametonodetodifferentusabledisk,usernametonodetodifferentcardcount,differentusernametonodelist


def SendEmails(usernametonodetodifferentusableproc,usernametonodetodifferentusableram,usernametonodetodifferentusabledisk,usernametonodetodifferentcardcount,prevusernametonodetousableproc,prevusernametonodetousableram,prevusernametonodetousabledisk,prevusernametonodetocardcount,timetokillworkers,prevnodetoallowedgpuusernames,prevnodetoallowedcpuusernames,nodetoallowedgpuusernames,nodetoallowedcpuusernames,usernametoemail,senderemail,senderpassword):
    msgtoemail={}
    for username, nodetodifferentusableproc in usernametonodetodifferentusableproc.items():
        nodetodifferentusableram=usernametonodetodifferentusableram[username]
        nodetodifferentusabledisk=usernametonodetodifferentusabledisk[username]
        nodetodifferentcardcount=usernametonodetodifferentcardcount[username]
        prevnodetousableproc=prevusernametonodetousableproc[username]
        prevnodetousableram=prevusernametonodetousableram[username]
        prevnodetousabledisk=prevusernametonodetousabledisk[username]
        prevnodetocardcount=prevusernametonodetocardcount[username]
        for node, differentusableproc in nodetodifferentusableproc.items():
            differentusabledisk=str(nodetodifferentusabledisk[node])
            differentcardcount=str(nodetodifferentcardcount[node])
            differentusableram=str(nodetodifferentusableram[node])
            prevusableproc=str(prevnodetousableproc[node])
            prevusableram=str(prevnodetousableram[node])
            prevusabledisk=str(prevnodetousabledisk[node])
            prevcardcount=str(prevnodetocardcount[node])
            differentusableproc=str(differentusableproc)
            diskresources=[prevusabledisk,differentusabledisk]
            ramresources=[prevusableram,differentusableram]
            procresources=[prevusableproc,differentusableproc]
            cardresources=[prevcardcount,differentcardcount]
            diskmsg=GenerateMessage(diskresources,'disk',username,node)
            rammsg=GenerateMessage(diskresources,'ram',username,node)
            procmsg=GenerateMessage(procresources,'proc',username,node)
            cardmsg=GenerateMessage(cardresources,'card',username,node)
            allowedgpuusernames=nodetoallowedgpuusernames[node]
            prevallowedgpuusernames=prevnodetoallowedgpuusernames[node]
            gpumsg=GenerateUsernameMessage([prevallowedgpuusernames,allowedgpuusernames],'GPU',node)
            allowedcpuusernames=nodetoallowedcpuusernames[node]
            prevallowedcpuusernames=prevnodetoallowedcpuusernames[node]
            cpumsg=GenerateUsernameMessage([prevallowedcpuusernames,allowedcpuusernames],'CPU',node)
            msg=''
            if gpumsg!='':
                msg+=gpumsg
            if cpumsg!='':
                msg+=cpumsg
            if diskmsg!='':
                msg+=diskmsg        
            if rammsg!='':
                msg+=rammsg        
            if procmsg!='':
                msg+=procmsg        
            if cardmsg!='':
                msg+=cardmsg
            thetime=str(timetokillworkers/60)
            msg+='Will wait '+thetime+' minutes, before killing work_queue_workers and restarting with new resource allocation'+'\n'
            email=usernametoemail[username]
            msgtoemail[msg]=email
    emailtomsg={}
    for msg,email in msgtoemail.items():
        if email not in emailtomsg.keys():
            emailtomsg[email]=[]
        emailtomsg[email].append(msg)
    for email,msgs in emailtomsg.items():
        msgs=list(set(msgs))
        msg='\n'.join(msgs)
        SendReportEmail(msg,senderemail,email,senderpassword)      
 

def SendReportEmail(TEXT,fromaddr,toaddr,password):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = 'Resource Allocation Change Report '
    message = TEXT
    msg.attach(MIMEText(message, 'plain'))
    s = smtplib.SMTP_SSL('smtp.gmail.com')
    s.ehlo()
    s.login(fromaddr,password)
    text = msg.as_string()
    s.sendmail(fromaddr, [toaddr],text)
    s.quit()

  

def GenerateMessage(resources,string,username,node):
    msg=''
    prevresource=resources[0]
    newresource=resources[1]
    if prevresource!=newresource:
        msg+='Previous '+string+' resource allocation for '+username+' '+'on node '+node+' '+'was '+prevresource+' , new allocation is '+newresource+'\n'

    return msg

def GenerateUsernameMessage(usernamelist,string,node):
    msg=''
    prevusernames=usernamelist[0]
    newusernames=usernamelist[1]
    prevusernamestring=','.join(prevusernames)
    newusernamestring=','.join(newusernames)
    if prevusernamestring!=newusernamestring:
        msg+='Previous usernames allocated for '+string+' on node '+node+' are '+prevusernamestring+' , new usernames allocated are '+newusernamestring+'\n'

    return msg

def DetectResourceAllocationChange(usernametonodetousableproc,usernametonodetousableram,usernametonodetousabledisk,usernametonodetocardcount,prevusernametonodetousableproc,prevusernametonodetousableram,prevusernametonodetousabledisk,prevusernametonodetocardcount,timetokillworkers,prevnodetoallowedgpuusernames,prevnodetoallowedcpuusernames,nodetoallowedgpuusernames,nodetoallowedcpuusernames,usernametoemail,senderemail,senderpassword):
    detectresourceallocationchange=False
    changeproc,usernametonodetodifferentusableproc=DetectResourceChange(usernametonodetousableproc,prevusernametonodetousableproc)
    changeram,usernametonodetodifferentusableram=DetectResourceChange(usernametonodetousableram,prevusernametonodetousableram)
    changedisk,usernametonodetodifferentusabledisk=DetectResourceChange(usernametonodetousabledisk,prevusernametonodetousabledisk)
    changecardcount,usernametonodetodifferentcardcount=DetectResourceChange(usernametonodetocardcount,prevusernametonodetocardcount)
    usernametonodetodifferentusableproc,usernametonodetodifferentusableram,usernametonodetodifferentusabledisk,usernametonodetodifferentcardcount,differentusernametonodelist=AddUnchangedResourcesOnSameNodeAsChangedResource(usernametonodetodifferentusableproc,usernametonodetodifferentusableram,usernametonodetodifferentusabledisk,usernametonodetodifferentcardcount,usernametonodetousableproc,usernametonodetousableram,usernametonodetousabledisk,usernametonodetocardcount)
    if changeproc==True or changeram==True or changedisk==True or changecardcount==True:
        detectresourceallocationchange=True  
    if detectresourceallocationchange==True:
        SendEmails(usernametonodetodifferentusableproc,usernametonodetodifferentusableram,usernametonodetodifferentusabledisk,usernametonodetodifferentcardcount,prevusernametonodetousableproc,prevusernametonodetousableram,prevusernametonodetousabledisk,prevusernametonodetocardcount,timetokillworkers,prevnodetoallowedgpuusernames,prevnodetoallowedcpuusernames,nodetoallowedgpuusernames,nodetoallowedcpuusernames,usernametoemail,senderemail,senderpassword)

    return detectresourceallocationchange,usernametonodetodifferentusableproc,usernametonodetodifferentusableram,usernametonodetodifferentusabledisk,usernametonodetodifferentcardcount,differentusernametonodelist


def FindWorkerPIDSToKill(differentusernametonodelist,usernametoqueuenametonodetoworkerpid,usernametoqueuenametologgers,usernametoqueuenametolognames):
    workerpidstokill=[]
    for username, nodelist in differentusernametonodelist.items():
        queuenametonodetoworkerpid=usernametoqueuenametonodetoworkerpid[username]
        for queuename, nodetoworkerpid in queuenametonodetoworkerpid.items():
            for node in nodelist:
                pid=nodetoworkerpid[node]
                workerpidstokill.append(pid)
                del usernametoqueuenametonodetoworkerpid[username][queuename][node]
                string='Killing work_queue_worker for username '+username+'  on node '+node +' for queuename '+queuename
                usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],string,usernametoqueuenametolognames[username][queuename],0)


    return workerpidstokill,usernametoqueuenametonodetoworkerpid,usernametoqueuenametologgers,usernametoqueuenametolognames


def KillWorkers(workerpidstokill):
    for pid in workerpidstokill:
        os.kill(pid, signal.SIGTERM)
    

def WriteToAllQueuesAllUsers(usernametoqueuenametologgers,string,usernametoqueuenametolognames,index):
    for username,queuenametologgers in usernametoqueuenametologgers.items():
        for queuename,loggers in  queuenametologgers.items():
            usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],string,usernametoqueuenametolognames[username][queuename],index)



    return usernametoqueuenametologgers

def Monitor(usernametoqueuenametoqueue,usernametoqueuenametotaskidtojob,usernametoqueuenametotaskidtooutputfilepathslist,waittime,usernametoqueuenametotaskidtoinputline,usernametoqueuenametologgers,usernametoqueuenametolognames,usernametoqueuenametotaskidtotasktag,usernametoqueuenametonodetoworkerpid,nodelist,prevusernametonodetousableproc,prevusernametonodetousableram,prevusernametonodetousabledisk,prevusernametonodetocardcount,nodelistfilepath,envpath,masterhost,usernametoqueuenametoprojectname,usernametoqueuenametopassword,workerdir,detectresourceallocationchange,timetokillworkers,timedetectedchange,prevnodetoallowedgpuusernames,prevnodetoallowedcpuusernames,usernametonodetodifferentusableproc,usernametonodetodifferentusableram,usernametonodetodifferentusabledisk,usernametonodetodifferentcardcount,differentusernametonodelist,usernametoemail,senderemail,senderpassword,usernametoqueuenametoportnumber):
    
    while True:
        time.sleep(5)
        breakout=False
        usernames=list(usernametoqueuenametoqueue.keys())
        nodelist,usernametonodetousableproc,usernametonodetousableram,usernametonodetousabledisk,usernametonodetocardcount,nodetoallowedgpuusernames,nodetoallowedcpuusernames=ReadNodeList(nodelistfilepath,usernames)
        ReadSheetsUpdateFile(usernames,nodelistfilepath)
        jobinfo={}
        jobinfo,foundinputjobs=CheckForInputJobs(jobinfo)
        if foundinputjobs==True:
            usernametoqueuenametoqueue,usernametoqueuenametotaskidtojob,usernametoqueuenametotaskidtooutputfilepathslist,usernametoqueuenametotaskidtoinputline,usernametoqueuenametotaskidtotasktag=SubmitToQueue(jobinfo,usernametoqueuenametoqueue,usernametoqueuenametotaskidtojob,usernametoqueuenametotaskidtooutputfilepathslist,usernametoqueuenametotaskidtoinputline,usernametoqueuenametologgers,usernametoqueuenametolognames,usernametoqueuenametotaskidtotasktag)
        if detectresourceallocationchange==False: 
            try:
                detectresourceallocationchange,usernametonodetodifferentusableproc,usernametonodetodifferentusableram,usernametonodetodifferentusabledisk,usernametonodetodifferentcardcount,differentusernametonodelist=DetectResourceAllocationChange(usernametonodetousableproc,usernametonodetousableram,usernametonodetousabledisk,usernametonodetocardcount,prevusernametonodetousableproc,prevusernametonodetousableram,prevusernametonodetousabledisk,prevusernametonodetocardcount,timetokillworkers,prevnodetoallowedgpuusernames,prevnodetoallowedcpuusernames,nodetoallowedgpuusernames,nodetoallowedcpuusernames,usernametoemail,senderemail,senderpassword)
            except:
                traceback.print_exc(file=sys.stdout)
                text = str(traceback.format_exc())
                print(text,flush=True)


            if detectresourceallocationchange==True:
                timedetectedchange=time.time()

        else:
            currenttime=time.time()
            diff=(currenttime-timedetectedchange)
            if diff>=timetokillworkers: 
                workerpidstokill,usernametoqueuenametonodetoworkerpid,usernametoqueuenametologgers,usernametoqueuenametolognames=FindWorkerPIDSToKill(differentusernametonodelist,usernametoqueuenametonodetoworkerpid,usernametoqueuenametologgers,usernametoqueuenametolognames)
                KillWorkers(workerpidstokill)
                usernametoqueuenametologgers,usernametoqueuenametonodetoworkerpid=CallWorkers(nodelist,envpath,masterhost,usernametoqueuenametoportnumber,usernametonodetodifferentusableproc,usernametonodetodifferentusableram,usernametonodetodifferentusabledisk,usernametoqueuenametoprojectname,usernametoqueuenametopassword,usernametonodetodifferentcardcount,usernametoqueuenametologgers,usernametoqueuenametolognames,workerdir,usernametoqueuenametonodetoworkerpid)
                timedetectedchange=None
                detectresourceallocationchange=False

        random.shuffle(usernames) # this ensures you dont get stuck serviving only one username until their queue is empty
        for username in usernames:
            if breakout==True:
                break
            queuenametoqueue=usernametoqueuenametoqueue[username]
            queuenametotaskidtoinputline=usernametoqueuenametotaskidtoinputline[username]
            queuenametotaskidtojob=usernametoqueuenametotaskidtojob[username]
            queuenametotaskidtooutputfilepathslist=usernametoqueuenametotaskidtooutputfilepathslist[username]
            queuenames=list(queuenametoqueue.keys())
            random.shuffle(queuenames) # ensures that the first queue for user isnt the one that always gets serviced
            for queuename in queuenames:
                if breakout==True:
                    break
                q= queuenametoqueue[queuename]
                taskidtoinputline=queuenametotaskidtoinputline[queuename]
                taskidtooutputfilepathslist=queuenametotaskidtooutputfilepathslist[queuename]
                WriteOutTaskStateLoggingInfo(taskidtoinputline,q,queuename,username) 
                while not q.empty():
                    if breakout==True:
                        break
                    t = q.wait(waittime)

                    q=CheckForTaskCancellations(q,usernametoqueuenametotaskidtojob,usernametoqueuenametotaskidtotasktag)
                    if t:
                        taskid=str(t.id)
                        inputline=taskidtoinputline[taskid]
                        newoutputfilepaths=[] # sort by largest file size and move largest first (like move arc first so dont submit next job before arc and dyn are returned
                        if taskid in taskidtooutputfilepathslist.keys():
                            outputfilepaths=taskidtooutputfilepathslist[taskid]
                            if outputfilepaths!=None:
                                for outputfilepath in outputfilepaths:
                                    head,tail=os.path.split(outputfilepath)
                                    if os.path.isdir(head):
                                        if os.path.isfile(tail):
                                           newoutputfilepaths.append(outputfilepath) 
                        newfiles=[os.path.split(i)[1] for i in newoutputfilepaths]
                        newfilestofilepaths=dict(zip(newfiles,newoutputfilepaths))
                        filesizes=[os.stat(thefile).st_size for thefile in newfiles]
                        newfilestofilesizes=dict(zip(newfiles,filesizes))
                        sorteddic={k: v for k, v in sorted(newfilestofilesizes.items(), key=lambda item: item[1],reverse=True)}  
                        for filename in sorteddic.keys():
                            filepath=newfilestofilepaths[filename]
                            usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],'Moving file '+filename,usernametoqueuenametolognames[username][queuename],0)
                            shutil.move(os.path.join(os.getcwd(),filename),filepath)
                            
                        exectime = t.cmd_execution_time/1000000
                        returnstatus=t.return_status
                        usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],'A job has finished Task %s, return status= %s!\n' % (str(taskid),str(returnstatus)),usernametoqueuenametolognames[username][queuename],0)
                        if returnstatus!=0:
                            usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],'Error: Job did not terminate normally '+inputline,usernametoqueuenametolognames[username][queuename],1)

                        try:
                            usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],'Job name = ' + str(t.tag) + 'command = ' + str(t.command) + '\n',usernametoqueuenametolognames[username][queuename],0)
                            usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],"Host = " + str(t.hostname) + '\n',usernametoqueuenametolognames[username][queuename],0)
                            usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],"Execution time = " + str(exectime),usernametoqueuenametolognames[username][queuename],0)
                            usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],"Task used %s cores, %s MB memory, %s MB disk" % (t.resources_measured.cores,t.resources_measured.memory,t.resources_measured.disk),usernametoqueuenametolognames[username][queuename],0)
                            usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],"Task was allocated %s cores, %s MB memory, %s MB disk" % (t.resources_requested.cores,t.resources_requested.memory,t.resources_requested.disk),usernametoqueuenametolognames[username][queuename],0)
                            if t.limits_exceeded and t.limits_exceeded.cores > -1:
                                usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],"Task exceeded its cores allocation.",usernametoqueuenametolognames[username][queuename],0)
                        except:
                            pass # sometimes task returns as None?? not very often though
                    else:
                        usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],"Workers: %i init, %i idle, %i busy, %i total joined, %i total removed\n" % (q.stats.workers_init, q.stats.workers_idle, q.stats.workers_busy, q.stats.workers_joined, q.stats.workers_removed),usernametoqueuenametolognames[username][queuename],0)
                        usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],"Tasks: %i running, %i waiting, %i dispatched, %i submitted, %i total complete\n"% (q.stats.tasks_running, q.stats.tasks_waiting, q.stats.tasks_dispatched, q.stats.tasks_submitted, q.stats.tasks_done),usernametoqueuenametolognames[username][queuename],0)
                        usernametoqueuenametoqueue[username][queuename]=q
                        
                        breakout=True
       
    


def WaitForInputJobs(usernametoqueuenametologgers,usernametoqueuenametolognames):
    for username, queuenametologgers in usernametoqueuenametologgers.items():
        for queuename,logger in queuenametologgers.items():
            usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],'Waiting for input jobs',usernametoqueuenametolognames[username][queuename],0)
    jobinfo={}
    foundinputjobs=False
    while foundinputjobs==False:
        jobinfo,foundinputjobs=CheckForInputJobs(jobinfo)   
        time.sleep(5)
    return jobinfo,usernametoqueuenametologgers

def CheckForInputJobs(jobinfo):
    files=os.listdir()
    array=[]
    foundinputjobs=False
    for f in files:
        if 'submit.' in f:
            foundinputjobs=True
            jobinfo=ReadJobInfoFromFile(jobinfo,f)
            array.append(f)
    for f in array:
        os.remove(f)
    return jobinfo,foundinputjobs
    
def WritePIDFile(pidfile):
    pid=str(os.getpid())
    temp=open(pidfile, 'w',buffering=1)
    temp.write(pid+'\n')
    temp.flush()
    os.fsync(temp.fileno())
    temp.close()

def ParseJobInfo(line):
    linesplit=line.split('--')[1:]
    linesplit=[e.rstrip() for e in linesplit]
    job=None
    scratch=None
    ram=None
    numproc=None
    inputfilepaths=None
    outputfilepaths=None
    binpath=None
    scratchpath=None
    cache=False
    inputline=line
    disk=None
    gpucard=None
    username=None   
    gpujob=False
    for line in linesplit:
        if "job=" in line:
            job=line.replace('job=','')
        if "scratchpath=" in line:
            scratchpath=line.replace('scratchpath=','')
        if "ram=" in line:
            ram=line.replace('ram=','')
        if "numproc=" in line:
            numproc=line.replace('numproc=','')
        if "inputfilepaths=" in line:
            inputfilepaths=line.replace('inputfilepaths=','')
            inputfilepaths=inputfilepaths.split(',')
        if "outputfilepaths=" in line:
            outputfilepaths=line.replace('outputfilepaths=','')
            outputfilepaths=outputfilepaths.split(',')
        if "absolutepathtobin" in line:
            binpath=line.replace('absolutepathtobin=','')
        if "cache" in line:
            cache=True
        if "disk" in line:
            disk=line.replace('disk=','')
        if "username" in line:
            username=line.replace('username=','')
        if "gpucard" in line:
            gpucard=line.replace('gpucard=','')
        if "gpujob" in line:
            gpujob=True


    return job,ram,numproc,inputfilepaths,outputfilepaths,binpath,scratchpath,cache,inputline,disk,username,gpucard,gpujob

def CheckForTaskCancellations(q,usernametoqueuenametotaskidtojob,usernametoqueuenametotaskidtotasktag):
    thedir= os.path.dirname(os.path.realpath(__file__))+r'/'
    os.chdir(thedir)
    files=os.listdir()
    for f in files:
        if '_cancel.txt' in f:
            temp=open(f,'r')
            results=temp.readlines()
            temp.close()
            result=results[0]
            resultsplit=result.split()
            final=resultsplit[0]
            tryusername=resultsplit[1]
            for username,queuenametotaskidtojob in usernametoqueuenametotaskidtojob.items():
                queuenametotaskidtotasktag=usernametoqueuenametotaskidtotasktag[username]
                if tryusername==username:
                    for queuename,taskidtojob in queuenametotaskidtojob.items():
                        if final in taskidtojob.keys():
                            t = q.cancel_by_taskid(final) 
                    for queuename,taskidtotasktag in queuenametotaskidtotasktag.items():
                        if final in taskidtotasktag.values(): 
                            t = q.cancel_by_tasktag(final) 
            os.remove(f)
    return q

def WriteToLogFile(loggerlist,string,loggernamelist,index):
    loggerlist[index].info(string)
    return loggerlist


def ReadSheets():
    import gspread
    gc = gspread.service_account(filename=os.path.join('NodeTopology','credentials.json'))
    sh=gc.open('Ren lab cluster usage')
    worksheet=sh.sheet1
    noderes=worksheet.col_values(1)
    usernameres=worksheet.col_values(7)
    gpunodetousername=dict(zip(noderes,usernameres))
    usernameres=worksheet.col_values(6)
    cpunodetousername=dict(zip(noderes,usernameres))
    return gpunodetousername,cpunodetousername


def ReadSheetsUpdateFile(usernames,nodetopology):
    try:
        gpunodetousername,cpunodetousername=ReadSheets()
        WriteUsernameToNodeTopologyFile(nodetopology,gpunodetousername,cpunodetousername,usernames) 
    except:
        traceback.print_exc(file=sys.stdout)
        text = str(traceback.format_exc())
        print(text,flush=True)


def WriteUsernameToNodeTopologyFile(nodetopology,gpunodetousername,cpunodetousername,usernames):
    temp=open(nodetopology,'r')
    results=temp.readlines()
    temp.close()
    temp=open(nodetopology,'w')
    for line in results:
        if '#' not in line:
            linesplit=line.split()
            card=linesplit[0]
            linesplit=linesplit[:8+1]
            line=' '.join(linesplit)
            anyuser=False
            if card in gpunodetousername.keys():
                gpuusername=gpunodetousername[card]
                if gpuusername in usernames:
                    pass
                else:
                    anyuser=True
            if anyuser==True:
                gpuusername='ANYUSER'
            anyuser=False
            if card in cpunodetousername.keys():
                cpuusername=cpunodetousername[card]
                if cpuusername in usernames:
                    pass
                else:
                    anyuser=True
            if anyuser==True:
                cpuusername='ANYUSER'
            line=line.replace('\n','')+' '+cpuusername+' '+gpuusername+'\n'
        temp.write(line)
    temp.close()



def SetupLogger(log_file, level=logging.INFO):
    name=log_file.split('.')[0]
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    logger = logging.getLogger(name)
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

def CopyJobInfoFilePath(jobinfofilepath,thedir):
    head,tail=os.path.split(jobinfofilepath)
    split=tail.split('.')
    first=split[0]
    newfirst=first+'_submit'
    split[0]=newfirst
    newname='.'.join(split)
    newpath=os.path.join(thedir,newname)
    shutil.copy(jobinfofilepath,newpath)


def CheckInputs(password,projectname):
    #if password==None:
    #    raise ValueError('Please set password for manager, needed for security purposes. Otherwise cluster data visibile to public.')
    if projectname==None:
        raise ValueError('Please set projectname for manager, needed for multiple instances of managers and work_queue_workers running on cluster via different users.')

def ReadUsernameList(usernamelist):
    usernames=[]
    usernametoemail={}
    temp=open(usernamelist,'r')
    results=temp.readlines()
    temp.close()
    for line in results:
        linesplit=line.split()
        if len(linesplit)!=0:
            username=linesplit[0]
            email=linesplit[1]
            usernames.append(username)
            usernametoemail[username]=email

    return usernames,usernametoemail

  

def StartDaemon(pidfile,nodelistfilepath,startingportnumber,projectname,envpath,masterhost,password,workerdir,waittime,usernametoemaillist,startworkers,username,runallusers):
    import work_queue as wq
    if os.path.isfile(pidfile):
        raise ValueError('Daemon instance is already running')

    WritePIDFile(pidfile)
    usernametoqueuenametotaskidtotasktag={}
    usernametoqueuenametonodetoworkerpid={}
    usernametoqueuenametoqueue={}
    usernametoqueuenametotaskidtojob={}
    usernametoqueuenametolognames={}
    usernametoqueuenametoprojectname={}
    usernametoqueuenametoportnumber={}
    usernametoqueuenametologgers={}
    usernametoqueuenametopassword={}
    usernames,usernametoemail=ReadUsernameList(usernametoemaillist)
    if username!=None:
        usernames=[username]
    if usernames==None and runallusers==False:
        raise ValueError(' please enter username or add option --runallusers ')
    portnumber=startingportnumber
    usernametoqueuenametotaskidtooutputfilepathslist={}
    usernametoqueuenametotaskidtoinputline={}
    for username in usernames:
        if not os.path.isdir(username):
            os.mkdir(username)
        os.chdir(username)
        files=os.listdir()
        for f in files:
            if '.log' in f:
                os.remove(f)

        os.chdir('..')
        usernametoqueuenametotaskidtotasktag[username]={}
        usernametoqueuenametonodetoworkerpid[username]={}
        usernametoqueuenametonodetoworkerpid[username]={}
        usernametoqueuenametoqueue[username]={}
        usernametoqueuenametotaskidtoinputline[username]={}
        usernametoqueuenametotaskidtooutputfilepathslist[username]={}
        usernametoqueuenametotaskidtojob[username]={}
        usernametoqueuenametolognames[username]={}
        usernametoqueuenametoprojectname[username]={}
        usernametoqueuenametoportnumber[username]={}
        usernametoqueuenametologgers[username]={}
        usernametoqueuenametopassword[username]={}
        mainqueuename=username+'_'+'mainqueue'
        queuelogname=os.path.join(username,mainqueuename+'_'+queueloggerfile)
        errorlogname=os.path.join(username,mainqueuename+'_'+errorloggerfile)
        queuelogger=SetupLogger(queuelogname)
        errorlogger=SetupLogger(errorlogname)
        lognamelist=[queuelogname,errorlogname]
        loggerlist=[queuelogger,errorlogger]
        usernametoqueuenametolognames[username][mainqueuename]=lognamelist
        usernametoqueuenametologgers[username][mainqueuename]=loggerlist
        mainqueueprojectname=mainqueuename+'_'+projectname
        usernametoqueuenametoprojectname[username][mainqueuename]=mainqueueprojectname
        usernametoqueuenametoportnumber[username][mainqueuename]=portnumber
        mainqueuepassword=mainqueuename+'_'+password
        usernametoqueuenametopassword[username][mainqueuename]=mainqueuepassword
        usernametoqueuenametotaskidtojob[username][mainqueuename]={}
        usernametoqueuenametotaskidtooutputfilepathslist[username][mainqueuename]={}
        usernametoqueuenametotaskidtoinputline[username][mainqueuename]={}
        usernametoqueuenametonodetoworkerpid[username][mainqueuename]={}
        queue = wq.WorkQueue(portnumber,name=mainqueueprojectname,debug_log = os.path.join(username,mainqueuename+"_output.log"),stats_log = os.path.join(username,mainqueuename+"_stats.log"),transactions_log=os.path.join(username,mainqueuename+"_transactions.log"))
        queue.enable_monitoring(os.path.join(username,mainqueuename+'_resourcesummary'),watchdog=False)
        #queue.specify_password(mainqueuepassword) # bug where speiifying password makes tasks wait in queue
        usernametoqueuenametologgers[username][mainqueuename]=WriteToLogFile(usernametoqueuenametologgers[username][mainqueuename],"listening on port {}".format(queue.port),usernametoqueuenametolognames[username][mainqueuename],0)
        usernametoqueuenametoqueue[username][mainqueuename]=queue
        portnumber+=1
        usernametoqueuenametotaskidtotasktag[username][mainqueuename]={}

    detectresourceallocationchange=False
    timedetectedchange=None
    usernametonodetodifferentusableproc={}
    usernametonodetodifferentusableram={}
    usernametonodetodifferentusabledisk={}
    usernametonodetodifferentcardcount={}
    differentusernametonodelist={}

    senderemail='renlabclusterreport@gmail.com'
    senderpassword='amoebaisbest'
    nodelist,usernametonodetousableproc,usernametonodetousableram,usernametonodetousabledisk,usernametonodetocardcount,nodetoallowedgpuusernames,nodetoallowedcpuusernames=ReadNodeList(nodelistfilepath,usernames)
    jobinfo,usernametoqueuenametologgers=WaitForInputJobs(usernametoqueuenametologgers,usernametoqueuenametolognames)
    ReadSheetsUpdateFile(usernames,nodelistfilepath)
    if startworkers==True:
        usernametoqueuenametologgers,usernametoqueuenametonodetoworkerpid=CallWorkers(nodelist,envpath,masterhost,usernametoqueuenametoportnumber,usernametonodetousableproc,usernametonodetousableram,usernametonodetousabledisk,usernametoqueuenametoprojectname,usernametoqueuenametopassword,usernametonodetocardcount,usernametoqueuenametologgers,usernametoqueuenametolognames,workerdir,usernametoqueuenametonodetoworkerpid)
    
    usernametoqueuenametoqueue,usernametoqueuenametotaskidtojob,usernametoqueuenametotaskidtooutputfilepathslist,usernametoqueuenametotaskidtoinputline,usernametoqueuenametotaskidtotasktag=SubmitToQueue(jobinfo,usernametoqueuenametoqueue,usernametoqueuenametotaskidtojob,usernametoqueuenametotaskidtooutputfilepathslist,usernametoqueuenametotaskidtoinputline,usernametoqueuenametologgers,usernametoqueuenametolognames,usernametoqueuenametotaskidtotasktag)
    Monitor(usernametoqueuenametoqueue,usernametoqueuenametotaskidtojob,usernametoqueuenametotaskidtooutputfilepathslist,waittime,usernametoqueuenametotaskidtoinputline,usernametoqueuenametologgers,usernametoqueuenametolognames,usernametoqueuenametotaskidtotasktag,usernametoqueuenametonodetoworkerpid,nodelist,usernametonodetousableproc,usernametonodetousableram,usernametonodetousabledisk,usernametonodetocardcount,nodelistfilepath,envpath,masterhost,usernametoqueuenametoprojectname,usernametoqueuenametopassword,workerdir,detectresourceallocationchange,timetokillworkers,timedetectedchange,nodetoallowedgpuusernames,nodetoallowedcpuusernames,usernametonodetodifferentusableproc,usernametonodetodifferentusableram,usernametonodetodifferentusabledisk,usernametonodetodifferentcardcount,differentusernametonodelist,usernametoemail,senderemail,senderpassword,usernametoqueuenametoportnumber)
    return usernametoqueuenametologgers,usernametoqueuenametolognames


def StartDaemonHandleErrors(pidfile,nodelistfilepath,startingportnumber,projectname,envpath,masterhost,password,workerdir,waittime,usernametoemaillist,startworkers,username,runallusers):
    CheckInputs(password,projectname)
    try:
        usernametoqueuenametologgers,usernametoqueuenametolognames=StartDaemon(pidfile,nodelistfilepath,startingportnumber,projectname,envpath,masterhost,password,workerdir,waittime,usernametoemaillist,startworkers,username,runallusers)   
    except:
        traceback.print_exc(file=sys.stdout)
        text = str(traceback.format_exc())
        for username,queuenametologgers in usernametoqueuenametologgers.items():
            for queuename,loggers in queuenametologgers.items():
                usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],str(text),usernametoqueuenametolognames[username][queuename],0)
                usernametoqueuenametologgers[username][queuename]=WriteToLogFile(usernametoqueuenametologgers[username][queuename],str(text),usernametoqueuenametolognames[username][queuename],1)

        raise ValueError('Program Crash')

    finally:
        if os.path.isfile(pidfile): # delete pid file
            os.remove(pidfile)
 
if jobinfofilepath==None and backupmanager==False:
    StartDaemonHandleErrors(pidfile,nodelistfilepath,startingportnumber,projectname,envpath,masterhost,password,workerdir,waittime,usernametoemaillist,startworkers,username,runallusers)
elif jobinfofilepath!=None and backupmanager==False:

    if canceltaskid==None and canceltasktag==None:
        CopyJobInfoFilePath(jobinfofilepath,thedir)   
        sys.exit()
    else:
        if username!=None:
            os.chdir(thedir)
            if canceltaskid!=None:
                with open(canceltaskid+'_cancel.txt', 'w') as fp:
                    fp.write(canceltaskid+' '+username+'\n')
            if canceltasktag!=None:
                with open(canceltasktag+'_cancel.txt', 'w') as fp:
                    fp.write(canceltasktag+' '+username+'\n')
        sys.exit()

elif jobinfofilepath==None and backupmanager==True:
    while os.path.isfile(pidfile):
        time.sleep(waitingtime)
    jobinfofilepath=waitingloggerfile
    CopyJobInfoFilePath(jobinfofilepath,thedir)   
    StartDaemonHandleErrors(pidfile,nodelistfilepath,startingportnumber,projectname,envpath,masterhost,password,workerdir,waittime,usernametoemaillist,startworkers,username,runallusers)

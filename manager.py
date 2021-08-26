import work_queue as wq
import os
import sys
import subprocess
import time
import shutil
import getopt


portnumber=9123
nodelistfilepath='nodeinfo.txt'
masterhost='nova'
envpath='/home/bdw2292/.allpurpose.bashrc'
jobinfofilepath=None
pidfile='daemon.pid'
canceltaskid=None
canceltasktag=None
opts, xargs = getopt.getopt(sys.argv[1:],'',["bashrcpath=","jobinfofilepath=","canceltaskid=","canceltasktag="])
for o, a in opts:
    if o in ("--bashrcpath"):
        envpath=a
    elif o in ("--jobinfofilepath"):
        jobinfofilepath=a
    elif o in ("--canceltaskid"):
        canceltaskid=a
    elif o in ("--canceltasktag"):
        canceltasktag=a


def ReadNodeList(nodelistfilepath):
    nodelist=[]
    nodetousableram={}
    nodetohasgpu={}
    nodetousabledisk={}
    nodetousableproc={}
    if os.path.isfile(nodelistfilepath):
        temp=open(nodelistfilepath,'r')
        results=temp.readlines()
        for line in results:
            linesplit=line.split()
            if len(linesplit)<1:
                continue
            newline=line.replace('\n','')
            if '#' not in line:
                linesplit=newline.split()
                node=linesplit[0]
                nodelist.append(node)
                hasgpu=linesplit[1]
                proc=linesplit[2]
                ram=linesplit[3]
                scratch=linesplit[4]
                consumratio=float(linesplit[5])
                if hasgpu=='GPU':
                    nodetohasgpu[node]=True
                else:
                    nodetohasgpu[node]=False
                if proc!='UNK':
                    proc=str(int(int(proc)*consumratio))
                if ram!='UNK':
                    ram=str(int(int(ram)*consumratio))
                if scratch!='UNK':
                    scratch=str(int(int(scratch)*consumratio))
                nodetousableram[node]=ram
                nodetousabledisk[node]=scratch
                nodetousableproc[node]=proc              

        temp.close()
    if len(nodelist)==0:
        raise ValueError('Node list has no nodes to read from')
    return nodelist,nodetohasgpu,nodetousableproc,nodetousableram,nodetousabledisk


def CallWorker(node,envpath,masterhost,portnumber,hasgpu,proc,ram,disk):
    idletimeout=100000000
    cmdstr='work_queue_worker '+str(masterhost)+' '+str(portnumber) + ' -d all -o worker.debug -M '+'RenLabCLuster'
    if proc!='UNK':
        cmdstr+=' '+'--cores '+proc
    if ram!='UNK':
        cmdstr+=' '+'--memory '+ram
    #if disk!='UNK': program complains if report less than available?
    #    cmdstr+=' '+'--disk '+disk
    cmdstr+=' '+'-t '+str(idletimeout)
    thedir= os.path.dirname(os.path.realpath(__file__))+r'/'
    cmdstr = 'cd %s ;%s' %(thedir,cmdstr)
    #cmdstr=SpecifyGPUCard(cardvalue,cmdstr)
    cmdstr = 'ssh %s "source %s ;%s"' %(str(node),envpath,cmdstr)
    print('Calling: '+cmdstr,flush=True)
    process = subprocess.Popen(cmdstr, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)

def CallWorkers(nodelist,envpath,masterhost,portnumber,nodetohasgpu,nodetousableproc,nodetousableram,nodetousabledisk):
    for node in nodelist:
        hasgpu=nodetohasgpu[node]
        proc=nodetousableproc[node]
        ram=nodetousableram[node]
        disk=nodetousabledisk[node]
        CallWorker(node,envpath,masterhost,portnumber,hasgpu,proc,ram,disk)       

def CallJob(cmdstr,path):
    import subprocess
    cmdstr = 'cd %s ;%s' %(path,cmdstr)
    process = subprocess.Popen(cmdstr, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)


def ReadJobInfoFromFile(jobinfo,filename):
    if os.path.isfile(filename):
        temp=open(filename,'r')
        results=temp.readlines()
        temp.close()
        for line in results:
            job,scratchspace,jobpath,ram,numproc=ParseJobInfo(line)
            array=['scratchspace','jobpath','ram','numproc']
            for key in array:
                if key not in jobinfo.keys():
                    jobinfo[key]={}
            jobinfo['scratchspace'][job]=scratchspace
            jobinfo['jobpath'][job]=jobpath
            jobinfo['ram'][job]=ram
            jobinfo['numproc'][job]=numproc

    return jobinfo

def ReadJobInfoFromDic(jobinfo):
    jobtoscratchspace=jobinfo['scratchspace']
    jobtojobpath=jobinfo['jobpath']
    jobtoram=jobinfo['ram']
    jobtonumproc=jobinfo['numproc']
    return jobtoscratchspace,jobtojobpath,jobtoram,jobtonumproc 

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


def SubmitToQueue(jobinfo,queue,taskidtojob,cattomaxresourcedic):
    print("Submitting tasks...",flush=True)
    jobtoscratch,jobtojobpath,jobtoram,jobtonumproc=ReadJobInfoFromDic(jobinfo)
    for job,jobpath in jobtojobpath.items():
        if job!=None:
            scratch=jobtoscratch[job]
            ram=jobtoram[job]
            numproc=jobtonumproc[job]
            temp={} 
            print('Calling: '+str(job),flush=True)
            task = wq.PythonTask(CallJob, job, jobpath)
            if numproc!=None: 
                numproc=int(numproc)
                task.specify_cores(numproc)     
                temp['cores']=numproc 
            if ram!=None:
                ram=ConvertMemoryToMBValue(ram)           
                task.specify_memory(ram)    
                temp['memory']=ram      
            if scratch!=None:
                scratch=ConvertMemoryToMBValue(scratch)         
                task.specify_disk(scratch)    
                temp['disk']=scratch 
            if '_gpu' in job:
                task.specify_gpus(1)          
                task.specify_tag("GPU")
                temp['gpus']=1
            else:
                task.specify_max_retries(2) # let QM do retry for now (or poltype)
                task.specify_tag("CPU")
                temp['gpus']=1
            foundcat=False
            largestcat=0
            for cat,resourcedic in cattomaxresourcedic.items():
                if temp==resoucedic:
                    foundcat=True
                    break
                catnum=cat.replace('Cat','')
                catnum=int(catnum)
                if catnum>largestcat:
                    largestcat=catnum
            if foundcat==True:
                pass
            else:
                largestcat=largestcat+1
                cat="Cat"+str(largestcat)
                queue.specify_category_max_resources(cat, temp)
            task.specify_category(cat)
            taskid=str(queue.submit(task))
            taskidtojob[taskid]=job
            print('Task ID of '+taskid+' is assigned to job '+job,flush=True)
    return queue,taskidtojob,cattomaxresourcedic

def Monitor(q,taskidtojob,cattomaxresourcedic):
    jobinfo={}
    while not q.empty():
        t = q.wait(5)
        q=CheckForTaskCancellations(q,taskidtojob)
        if t:
            print("Task used %s cores, %s MB memory, %s MB disk" % (t.resources_measured.cores,t.resources_measured.memory,t.resources_measured.disk),flush=True)
            print("Task was allocated %s cores, %s MB memory, %s MB disk" % (t.resources_requested.cores,t.resources_requested.memory,t.resources_requested.disk),flush=True)
            if t.limits_exceeded and t.limits_exceeded.cores > -1:
                print("Task exceeded its cores allocation.",flush=True)
            x = t.output
            if isinstance(x,Exception):
                print("Exception: {}".format(x),flush=True)
            else:
                print("Result: {}".format(x),flush=True)
        else:
            jobinfo,foundinputjobs=CheckForInputJobs(jobinfo)
            if foundinputjobs==True:
                q,taskidtojob,cattomaxresourcedic=SubmitToQueue(jobinfo,q,taskidtojob,cattomaxresourcedic)
                Monitor(q,taskidtojob,cattomaxresourcedic)
       
    
    jobinfo=WaitForInputJobs()
    q,taskidtojob,cattomaxresourcedic=SubmitToQueue(jobinfo,q,taskidtojob,cattomaxresourcedic)
    Monitor(q,taskidtojob,cattomaxresourcedic)


def WaitForInputJobs():
    print('Waiting for input jobs',flush=True)
    jobinfo={}
    foundinputjobs=False
    while foundinputjobs==False:
        jobinfo,foundinputjobs=CheckForInputJobs(jobinfo)   
        time.sleep(5)
    return jobinfo

def CheckForInputJobs(jobinfo):
    files=os.listdir()
    array=[]
    foundinputjobs=False
    for f in files:
        if 'submit' in f:
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
    scratchspace=None
    jobpath=None
    ram=None
    numproc=None
    for line in linesplit:
        if "job=" in line:
            job=line.replace('job=','')
        if "scratchspace=" in line:
            scratchspace=line.replace('scratchspace=','')
        if "jobpath=" in line:
            jobpath=line.replace('jobpath=','')
        if "ram=" in line:
            ram=line.replace('ram=','')
        if "numproc=" in line:
            numproc=line.replace('numproc=','')

    return job,scratchspace,jobpath,ram,numproc

def CheckForTaskCancellations(q,taskidtojob):
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
            if final in taskidtojob.keys():
                t = q.cancel_by_taskid(final) 
            else:
                t = q.cancel_by_tasktag(final) 
            os.remove(f)
    return q

thedir= os.path.dirname(os.path.realpath(__file__))+r'/'

if jobinfofilepath==None:
    if os.path.isfile(pidfile):
        raise ValueError('Daemon instance is already running')
    WritePIDFile(pidfile)
    try:
        taskidtojob={}
        cattomaxresourcedic={}
        nodelist,nodetohasgpu,nodetousableproc,nodetousableram,nodetousabledisk=ReadNodeList(nodelistfilepath)
        jobinfo=WaitForInputJobs()
        queue = wq.WorkQueue(portnumber,name='RenLabCluster',debug_log = "output.log",stats_log = "stats.log",transactions_log = "transactions.log")
        queue.enable_monitoring('resourcesummary')
        print("listening on port {}".format(queue.port),flush=True)
        CallWorkers(nodelist,envpath,masterhost,portnumber,nodetohasgpu,nodetousableproc,nodetousableram,nodetousabledisk)
        # Submit several tasks for execution:
        queue,taskidtojob,cattomaxresourcedic=SubmitToQueue(jobinfo,queue,taskidtojob,cattomaxresourcedic)
        Monitor(queue,taskidtojob,cattomaxresourcedic)
    finally:
        if os.path.isfile(pidfile): # delete pid file
            os.remove(pidfile)
    
else:
    if canceltaskid==None and canceltasktag==None:
        head,tail=os.path.split(jobinfofilepath)
        split=tail.split('.')
        first=split[0]
        newfirst=first+'_submit'
        split[0]=newfirst
        newname='.'.join(split)
        newpath=os.path.join(thedir,newname)
        shutil.copy(jobinfofilepath,newpath)
        sys.exit()
    else:
        os.chdir(thedir)
        if canceltaskid!=None:
            with open(canceltaskid+'_cancel.txt', 'w') as fp:
                fp.write(canceltaskid+'\n')
        if canceltasktag!=None:
            with open(canceltasktag+'_cancel.txt', 'w') as fp:
                fp.write(canceltasktag+'\n')
        sys.exit()


import work_queue as wq
import os
import sys
import subprocess
import time
import shutil
import getopt
import traceback

waittime=15
portnumber=9123
nodelistfilepath='nodeinfo.txt'
masterhost='nova'
envpath='/home/bdw2292/.allpurpose.bashrc'
jobinfofilepath=None
pidfile='daemon.pid'
canceltaskid=None
canceltasktag=None
thedir= os.path.dirname(os.path.realpath(__file__))+r'/'
projectname=None
password=None
loggerfile='queuelogger.log'
opts, xargs = getopt.getopt(sys.argv[1:],'',["bashrcpath=","jobinfofilepath=","canceltaskid=","canceltasktag=",'projectname=','password='])
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
    elif o in ("--projectname"):
        projectname=a

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


def CallWorker(node,envpath,masterhost,portnumber,hasgpu,proc,ram,disk,projectname,password):
    idletimeout=100000000
    cmdstr='work_queue_worker '+str(masterhost)+' '+str(portnumber) 
    cmdstr+=' -d all -o worker.debug'
    if proc!='UNK':
        cmdstr+=' '+'--cores '+proc
    if ram!='UNK':
        cmdstr+=' '+'--memory '+ram
    cmdstr+=' '+'-t '+str(idletimeout)
    cmdstr+=' '+'-M '+projectname
    #cmdstr+=' '+'--password '+password CCtools has issues when this is specified
    cmdstr+=' '+'--parent-death'
    thedir= os.path.dirname(os.path.realpath(__file__))+r'/'
    cmdstr = 'ssh %s "source %s ;%s"' %(str(node),envpath,cmdstr)
    WriteToLogFile('Calling: '+cmdstr)
    process = subprocess.Popen(cmdstr, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)

def CallWorkers(nodelist,envpath,masterhost,portnumber,nodetohasgpu,nodetousableproc,nodetousableram,nodetousabledisk,projectname,password):
    for node in nodelist:
        hasgpu=nodetohasgpu[node]
        proc=nodetousableproc[node]
        ram=nodetousableram[node]
        disk=nodetousabledisk[node]
        CallWorker(node,envpath,masterhost,portnumber,hasgpu,proc,ram,disk,projectname,password)       



def ReadJobInfoFromFile(jobinfo,filename):
    if os.path.isfile(filename):
        temp=open(filename,'r')
        results=temp.readlines()
        temp.close()
        for line in results:
            split=line.split()
            if len(split)==0:
                continue
            cmdstr,scratchspace,ram,numproc,inputfilepaths,outputfiles,binpath,scratchdir,scratchpath=ParseJobInfo(line)
            array=['scratchspace','ram','numproc','inputfilepaths','outputfiles','binpath','scratchdir','scratchpath']
            for key in array:
                if key not in jobinfo.keys():
                    jobinfo[key]={}
            job=tuple([cmdstr,tuple(inputfilepaths)])
            jobinfo['scratchspace'][job]=scratchspace
            jobinfo['ram'][job]=ram
            jobinfo['numproc'][job]=numproc
            jobinfo['inputfilepaths'][job]=inputfilepaths
            jobinfo['outputfiles'][job]=outputfiles
            jobinfo['binpath'][job]=binpath
            jobinfo['scratchdir'][job]=scratchdir
            jobinfo['scratchpath'][job]=scratchpath

    return jobinfo

def ReadJobInfoFromDic(jobinfo):
    jobtoscratchspace=jobinfo['scratchspace']
    jobtoram=jobinfo['ram']
    jobtonumproc=jobinfo['numproc']
    jobtoinputfilepaths=jobinfo['inputfilepaths']
    jobtooutputfiles=jobinfo['outputfiles']
    jobtobinpath=jobinfo['binpath']
    jobtoscratchdir=jobinfo['scratchdir']
    jobtoscratchpath=jobinfo['scratchpath']
    return jobtoscratchspace,jobtoram,jobtonumproc,jobtoinputfilepaths,jobtooutputfiles,jobtobinpath,jobtoscratchdir,jobtoscratchpath

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



def SubmitToQueue(jobinfo,queue,taskidtojob,cattomaxresourcedic,taskidtooutputfiles,taskidtoinputfilepath):
    WriteToLogFile("Submitting tasks...")
    jobtoscratch,jobtoram,jobtonumproc,jobtoinputfilepaths,jobtooutputfiles,jobtobinpath,jobtoscratchdir,jobtoscratchpath=ReadJobInfoFromDic(jobinfo)
    for job,ram in jobtoram.items():
        if job!=None:
            scratch=jobtoscratch[job]
            numproc=jobtonumproc[job]
            inputfilepaths=jobtoinputfilepaths[job]
            outputfiles=jobtooutputfiles[job]
            binpath=jobtobinpath[job]
            scratchdir=jobtoscratchdir[job]
            scratchpath=jobtoscratchpath[job]
            cmdstr=job[0]
            if scratchdir!=None and scratchpath!=None:
                fullpath=os.path.join(scratchpath,scratchdir)
                string1='mkdir '+scratchpath+' ; '
                string2='mkdir '+fullpath+' ; '
                cmdstr=string1+string2+cmdstr
            temp={}
            task = wq.Task(cmdstr)
            if os.path.isfile(binpath):
                head,tail=os.path.split(binpath)
                task.specify_file(binpath, tail, wq.WORK_QUEUE_INPUT, cache=False)
            inputfilepath=None
            for inputfile in inputfilepaths:
                if os.path.isfile(inputfile):
                    head,tail=os.path.split(inputfile)
                    inputfilepath=head
                    task.specify_file(inputfile, tail, wq.WORK_QUEUE_INPUT, cache=False)
                else:
                    task.specify_file(inputfile, inputfile, wq.WORK_QUEUE_INPUT, cache=False)
            for outputfile in outputfiles:
                if os.path.isfile(outputfile):
                    head,tail=os.path.split(outputfile)
                    task.specify_file(outputfile, tail, wq.WORK_QUEUE_OUTPUT, cache=False)
                else:
                    task.specify_file(outputfile, outputfile, wq.WORK_QUEUE_OUTPUT, cache=False)
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
                #queue.specify_category_max_resources(cat, temp)
            #task.specify_category(cat)
            taskid=str(queue.submit(task))
            taskidtooutputfiles[taskid]=outputfiles
            if inputfilepath!=None:
                taskidtoinputfilepath[taskid]=inputfilepath
            taskidtojob[taskid]=job
            WriteToLogFile('Task ID of '+taskid+' is assigned to job '+cmdstr)
    return queue,taskidtojob,cattomaxresourcedic,taskidtooutputfiles,taskidtoinputfilepath

def Monitor(q,taskidtojob,cattomaxresourcedic,taskidtooutputfiles,taskidtoinputfilepath,waittime):
    jobinfo={}
    while not q.empty():
        t = q.wait(waittime)
        q=CheckForTaskCancellations(q,taskidtojob)
        if t:
            taskid=str(t.id)
            outputfiles=taskidtooutputfiles[taskid]
            if taskid in taskidtoinputfilepath.keys():
                inputfilepath=taskidtoinputfilepath[taskid]
                if os.path.isdir(inputfilepath):
                    for file in outputfiles:
                        if os.path.isfile(file):
                            os.rename(file,os.path.join(inputfilepath,file))
                
            exectime = t.cmd_execution_time/1000000
            returnstatus=t.return_status
            WriteToLogFile('A job has finished Task %s!\n' % (str(taskid)))
            if returnstatus!=0:
                WriteToLogFile('Error: Job did not terminate normally')
            WriteToLogFile('Job name = ' + str(t.tag) + 'command = ' + str(t.command) + '\n')
            WriteToLogFile("Host = " + str(t.hostname) + '\n')
            WriteToLogFile("Execution time = " + str(exectime))
            WriteToLogFile("Task used %s cores, %s MB memory, %s MB disk" % (t.resources_measured.cores,t.resources_measured.memory,t.resources_measured.disk))
            WriteToLogFile("Task was allocated %s cores, %s MB memory, %s MB disk" % (t.resources_requested.cores,t.resources_requested.memory,t.resources_requested.disk))
            if t.limits_exceeded and t.limits_exceeded.cores > -1:
                WriteToLogFile("Task exceeded its cores allocation.")
        else:
            WriteToLogFile("Workers: %i init, %i idle, %i busy, %i total joined, %i total removed\n" \
                % (q.stats.workers_init, q.stats.workers_idle, q.stats.workers_busy, q.stats.workers_joined, q.stats.workers_removed))
            WriteToLogFile("Tasks: %i running, %i waiting, %i dispatched, %i submitted, %i total complete\n" \
                % (q.stats.tasks_running, q.stats.tasks_waiting, q.stats.tasks_dispatched, q.stats.tasks_submitted, q.stats.tasks_done))

            jobinfo,foundinputjobs=CheckForInputJobs(jobinfo)
            if foundinputjobs==True:
                q,taskidtojob,cattomaxresourcedic,taskidtooutputfiles,taskidtoinputfilepath=SubmitToQueue(jobinfo,q,taskidtojob,cattomaxresourcedic,taskidtooutputfiles,taskidtoinputfilepath)
                Monitor(q,taskidtojob,cattomaxresourcedic,taskidtooutputfiles,taskidtoinputfilepath,waittime)
       
    
    jobinfo=WaitForInputJobs()
    q,taskidtojob,cattomaxresourcedic,taskidtooutputfiles,taskidtoinputfilepath=SubmitToQueue(jobinfo,q,taskidtojob,cattomaxresourcedic,taskidtooutputfiles,taskidtoinputfilepath)
    Monitor(q,taskidtojob,cattomaxresourcedic,taskidtooutputfiles,taskidtoinputfilepath,waittime)


def WaitForInputJobs():
    WriteToLogFile('Waiting for input jobs')
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
    scratchspace=None
    ram=None
    numproc=None
    inputfilepaths=None
    outputfiles=None
    binpath=None
    scratchdir=None
    scratchpath=None
    for line in linesplit:
        if "job=" in line:
            job=line.replace('job=','')
        if "scratchspace=" in line:
            scratchspace=line.replace('scratchspace=','')
        if "scratchdir=" in line:
            scratchdir=line.replace('scratchdir=','')
        if "scratchpath=" in line:
            scratchpath=line.replace('scratchpath=','')
        if "ram=" in line:
            ram=line.replace('ram=','')
        if "numproc=" in line:
            numproc=line.replace('numproc=','')
        if "inputfilepaths=" in line:
            inputfilepaths=line.replace('inputfilepaths=','')
            inputfilepaths=inputfilepaths.split(',')
        if "outputfiles=" in line:
            outputfiles=line.replace('outputfiles=','')
            outputfiles=outputfiles.split(',')
        if "absolutepathtobin" in line:
            binpath=line.replace('absolutepathtobin=','')

    return job,scratchspace,ram,numproc,inputfilepaths,outputfiles,binpath,scratchdir,scratchpath

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

def WriteToLogFile(string):
    now = time.strftime("%c",time.localtime())
    masterloghandle.write(now+' '+string+'\n')
    masterloghandle.flush()
    os.fsync(masterloghandle.fileno())


if jobinfofilepath==None:

    #if password==None:
    #    raise ValueError('Please set password for manager, needed for security purposes. Otherwise cluster data visibile to public.')
    if projectname==None:
        raise ValueError('Please set projectname for manager, needed for multiple instances of managers and work_queue_workers running on cluster via different users.')

    try:
        global masterloghandle
        masterloghandle=open(loggerfile,'w',buffering=1)
        if os.path.isfile(pidfile):
            raise ValueError('Daemon instance is already running')
        WritePIDFile(pidfile)
        taskidtojob={}
        cattomaxresourcedic={}
        taskidtooutputfiles={}
        taskidtoinputfilepath={}
        nodelist,nodetohasgpu,nodetousableproc,nodetousableram,nodetousabledisk=ReadNodeList(nodelistfilepath)
        jobinfo=WaitForInputJobs()
        queue = wq.WorkQueue(portnumber,name=projectname,debug_log = "output.log",stats_log = "stats.log",transactions_log = "transactions.log")
        queue.enable_monitoring('resourcesummary')
        #queue.specify_password(password) CCTools has issues when this is specified
        WriteToLogFile("listening on port {}".format(queue.port))
        CallWorkers(nodelist,envpath,masterhost,portnumber,nodetohasgpu,nodetousableproc,nodetousableram,nodetousabledisk,projectname,password)
        # Submit several tasks for execution:
        queue,taskidtojob,cattomaxresourcedic,taskidtooutputfiles,taskidtoinputfilepath=SubmitToQueue(jobinfo,queue,taskidtojob,cattomaxresourcedic,taskidtooutputfiles,taskidtoinputfilepath)
        Monitor(queue,taskidtojob,cattomaxresourcedic,taskidtooutputfiles,taskidtoinputfilepath,waittime)

    except:
        traceback.print_exc(file=sys.stdout)
        text = str(traceback.format_exc())
        WriteToLogFile(str(text))
        raise ValueError('Program Crash')

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


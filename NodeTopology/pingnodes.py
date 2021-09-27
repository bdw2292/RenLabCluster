import os
import sys
import subprocess
from tqdm import tqdm
import re
import time
import getopt

mincardtype=1000
nodelistfilepath='nodes.txt'
nodetopofilepath='nodeinfo.txt'
coreconsumptionratio='.7'
ramconsumptionratio='.7'
diskconsumptionratio='.7'
bashrcpath='/home/bdw2292/.allpurpose.bashrc'
filename='nodeinfo.txt'
monitorresourceusage=False
usernametoemaillist='usernamestoemail.txt'
senderemail='renlabclusterreport@gmail.com'
senderpassword='amoebaisbest'

opts, xargs = getopt.getopt(sys.argv[1:],'',['diskconsumptionratio=',"bashrcpath=",'coreconsumptionratio=','mincardtype=','ramconsumptionratio=','monitorresourceusage'])
for o, a in opts:
    if o in ("--bashrcpath"):
        bashrcpath=a
    elif o in ("--coreconsumptionratio"):
        coreconsumptionratio=a
    elif o in ("--ramconsumptionratio"):
        ramconsumptionratio=a
    elif o in ("--diskconsumptionratio"):
        diskconsumptionratio=a
    elif o in ("--mincardtype"):
        mincardtype=int(a)
    elif o in ("--monitorresourceusage"):
        monitorresourceusage=True


def AlreadyActiveNodes(nodelist,gpunodes,programexceptionlist):
    activecpunodelist=[]
    activegpunodelist=[]
    for nodeidx in tqdm(range(len(nodelist)),desc='Checking active nodes'):
        node=nodelist[nodeidx]
        activecards=[]
        if node in gpunodes:
            activecards=CheckWhichGPUCardsActive(node)
            for card in activecards:
                activegpunodelist.append(node+'-'+str(card))

        cmdstr=''
        for exception in programexceptionlist:
            cmdstr+='pgrep'+' '+exception+' ; '
        cmdstr=cmdstr[:-2]
        job='ssh %s "%s"'%(node,cmdstr)
        p = subprocess.Popen(job, stdout=subprocess.PIPE,shell=True)
        output, err = p.communicate()
        output=ConvertOutput(output)    

        if len(output)>1:
            keepnode=False
            cmdstr1='ps -p %s'%(output)
            cmdstr2='ps -p %s'%(output)+' -u'
            output1=CheckOutputFromExternalNode(node,cmdstr1)
            output2=CheckOutputFromExternalNode(node,cmdstr2)
            if type(output1)==str:
                print(output1)
            if type(output2)==str:
                print(output2)

            if len(activecards)!=0:
                for card in activecards:
                    activecpunodelist.append(node+'-'+str(card))
            else:
                activecpunodelist.append(node+'-'+str(0))

    return activecpunodelist,activegpunodelist


def CheckWhichGPUCardsActive(node):
    activecards=[]
    cmdstr='nvidia-smi'
    job='ssh %s "%s"'%(node,cmdstr)
    p = subprocess.Popen(job, stdout=subprocess.PIPE,shell=True)
    output, err = p.communicate()
    output=ConvertOutput(output)    

    lines=output.split('\n')
    count=-1
    for line in lines:
        linesplit=line.split()
        if len(linesplit)==15:
            percent=linesplit[-3]
            value=percent.replace('%','')
            if value.isnumeric():
                value=float(value)
                count+=1
                if value==0:
                    pass
                else:
                    card=node+'-'+str(count)
                    activecards.append(count)
            else:
                card=node+'-'+str(count)


    return activecards



def ReadNodeTopology(nodelistfilepath):
    cpunodelist=[]
    gpunodelist=[] 
    cpucardlist=[]
    gpucardlist=[]
    cpunodetousernames={}
    gpunodetousernames={}

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
            if node not in cpunodelist:
                cpunodelist.append(node)
            if card not in cpucardlist:
                cpucardlist.append(card)
            hasgpu=linesplit[1]
            if hasgpu=="GPU":
                if node not in gpunodelist:
                    gpunodelist.append(node) 
                if card not in gpucardlist:
                    gpucardlist.append(card)

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

            if card not in cpunodetousernames.keys():
                cpunodetousernames[card]=[]
            if card not in gpunodetousernames.keys():
                gpunodetousernames[card]=[]
            if gpuusername=='ANYUSER':
                pass
            else:
                gpunodetousernames[card].append(gpuusername)

            if cpuusername=='ANYUSER':
                pass
            else:
                cpunodetousernames[card].append(cpuusername)


    return cpunodelist,gpunodelist,cpucardlist,gpucardlist,cpunodetousernames,gpunodetousernames
 

def ReadNodeList(nodelistfilepath):
    nodelist=[]
    if os.path.isfile(nodelistfilepath):
        temp=open(nodelistfilepath,'r')
        results=temp.readlines()
        for line in results:
            linesplit=line.split()
            if len(linesplit)<1:
                continue
            newline=line.replace('\n','')
            linesplit=newline.split()
            node=linesplit[0]
            node=node.replace('#','')
            nodelist.append(node)

        temp.close()
    return nodelist


def PingNodesAndDetermineNodeInfo(nodelist,bashrcpath):
    gpunodes=[] 
    nodetototalram={}
    nodetototalcpu={}
    nodetototalscratch={}
    nodetocardcount={}
    nodetocardtype={}
    nodefailanalyze=[]
    for nodeidx in tqdm(range(len(nodelist)),desc='Pinging nodes'):
        node=nodelist[nodeidx]
        cudaversion,cardcount,cardtype,failanalyze=CheckGPUStats(node,bashrcpath)
        nproc=CheckTotalCPU(node)

        if nproc==0:
            nproc='UNK'
        ram=CheckRAM(node)
        if ram==False:
            ram='UNK'
        scratch=CheckScratchSpace(node)
        if scratch==False:
            scratch='UNK'
        nodetototalram[node]=ram
        nodetototalcpu[node]=nproc
        nodetototalscratch[node]=scratch

        if cudaversion!=None:
            gpunodes.append(node)
            nodetocardcount[node]=cardcount
            nodetocardtype[node]=cardtype
            if failanalyze==True:
                nodefailanalyze.append(node)
        else:
            nodetocardcount[node]=0
            nodetocardtype[node]='UNK'

    return gpunodes,nodetototalram,nodetototalcpu,nodetototalscratch,nodetocardcount,nodetocardtype,nodefailanalyze

def CheckGPUStats(node,bashrcpath):
    failanalyze=False
    cmdstr='nvidia-smi'
    job='ssh %s "%s"'%(node,cmdstr)
    try:
        output = subprocess.check_output(job, stderr=subprocess.STDOUT,shell=True, timeout=2.5)
        output=ConvertOutput(output)
        nodedead=False
    except:
        nodedead=True
    cudaversion=None
    cardcount=0
    cardtype=None
    if nodedead==False:
        lines=output.split('\n')
        for line in lines:
            linesplit=line.split()
            if 'CUDA' in line:
                cudaversion=linesplit[8]
            if len(linesplit)==15:
                cardcount+=1

    cmdstr='nvidia-smi -q'
    job='ssh %s "%s"'%(node,cmdstr)
    try:
        output = subprocess.check_output(job,stderr=subprocess.STDOUT, shell=True,timeout=2.5)
        output=ConvertOutput(output)
    except:
        nodedead=True
    if nodedead==False:
        lines=output.split('\n')
        for line in lines:
            if "Product Name" in line:
                linesplit=line.split()
                founddigit=False
                for e in linesplit:
                    if e.isdigit():
                        founddigit=True
                        cardtype=e
                if founddigit==False:
                    cardtype=linesplit[-1]
        filepath=os.path.abspath(os.path.split(__file__)[0])
        cmdstr='analyze_gpu'+' '+os.path.join(os.path.join(filepath,'VersionFiles/'),'water.xyz')+' '+'-k'+' '+os.path.join(os.path.join(filepath,'VersionFiles/'),'water.key')+' '+'e'
        job = 'ssh %s "source %s ;%s"' %(str(node),bashrcpath,cmdstr)
        try:
            output = subprocess.check_output(job,stderr=subprocess.STDOUT, shell=True,timeout=10)
            output=ConvertOutput(output)
        except:
            failanalyze=True


    return cudaversion,cardcount,cardtype,failanalyze


def ConvertOutput(output):
    if output!=None:
        output=output.rstrip()
        if type(output)!=str:
            output=output.decode("utf-8")
    return output


def CheckTotalCPU(node):
    totalproc=False
    cmdstr='nproc'
    output=CheckOutputFromExternalNode(node,cmdstr)
    if output!=False:
        lines=output.split('\n')
        firstline=lines[0]
        firstlinesplit=firstline.split()
        totalproc=int(firstlinesplit[0])
    return totalproc

def CheckOutputFromExternalNode(node,cmdstr):
    output=True
    job='ssh %s "%s"'%(node,cmdstr)
    try: # if it has output that means this process is running
        output=subprocess.check_output(job,stderr=subprocess.STDOUT,shell=True,timeout=10)
        output=ConvertOutput(output)
    except: #if it fails, that means no process with the program is running or node is dead/unreachable
         output=False
    return output 

def CheckRAM(node):
    ram=False
    total=False
    cmdstr='free -g'
    output=CheckOutputFromExternalNode(node,cmdstr)
    if output!=False:
        lines=output.split('\n')
        for line in lines:
            linesplit=line.split()
            if 'Mem' in line:
                ram=float(linesplit[3])
                total=float(linesplit[1])
                break
        ram=str(total)+'GB'
        ram=ConvertMemoryToMBValue(ram)
    return ram

def ConvertMemoryToMBValue(scratch):
    availspace,availunit=SplitScratch(scratch)
    if availunit=='M' or availunit=='MB':
        availspace=float(availspace)
    elif availunit=='T' or availunit=='TB':
        availspace=float(availspace)*1000000
    elif availunit=='G' or availunit=='GB':
        availspace=float(availspace)*1000
    return int(availspace)
 

def CheckScratchSpace(node):
    cmdstr='df -h'
    scratchavail=False
    scratchtotal=False
    output=CheckOutputFromExternalNode(node,cmdstr)
    if output!=False:
        lines=output.split('\n')[1:-1]

        d={}
        for line in lines:
            linesplit=line.split()
            if len(linesplit)==5 or len(linesplit)==6:
                avail = re.split('\s+', line)[1]
                mount = re.split('\s+', line)[5]
                d[mount] = avail
        if '/scratch' in d.keys(): 
            scratchavail=d['/scratch']
        else:
            scratchavail='0G'
        if scratchavail==None:
            scratchavail='0G'
        else:
            try:
                scratchavail=ConvertMemoryToMBValue(scratchavail)
            except:
                scratchavail=0

        


    return int(scratchavail)

def SplitScratch(string):
    for eidx in range(len(string)):
        e=string[eidx]
        if not e.isdigit() and e!='.':
            index=eidx
            break
    space=string[:index]
    diskunit=string[index]
    return space,diskunit


def WriteOutNodeInfo(filename,gpunodes,nodetototalram,nodetototalcpu,nodetototalscratch,nodelist,coreconsumptionratio,ramconsumptionratio,diskconsumptionratio,nodetocardcount,nodetocardtype,mincardtype,nodefailanalyze):
    temp=open(filename,'w')
    columns='#node'+' '+'HASGPU'+' '+'CARDTYPE'+' '+'Processors'+' '+'RAM(MB)'+' '+'Scratch(MB)'+' '+'CPUConsumptionRatio'+' '+'RAMConsumptionRatio'+' '+'DiskConsumptionRatio'+' '+'CPUUsername'+' '+'GPUUsername'+'\n'
    temp.write(columns)
    for node in nodelist:
        hasgpu=False
        if node in gpunodes:
            hasgpu=True
        if hasgpu==True:
            gpustring='GPU'
        else:
            gpustring='NOGPU'
        ram=str(nodetototalram[node])
        nproc=str(nodetototalcpu[node])
        scratch=str(nodetototalscratch[node])
        cardcount=str(nodetocardcount[node])
        cardtype=nodetocardtype[node]
        if cardtype!='UNK' and cardtype!=None:
            if cardtype.isdigit()==False:
                node='#'+node # temp until --gpus 0 fixed
            else:
                value=float(cardtype)
                if value<mincardtype:
                    cardcount='0'
                    node='#'+node # temp until --gpus 0 fixed
        if node in nodefailanalyze:
            node='#'+node # do this for now until we can set --gpus 0
        if cardtype==None:
            cardtype='UNK'
        if hasgpu==True:
            thecoreconsumptionratio=str(.5)
            theramconsumptionratio=str(.5)
            thediskconsumptionratio=str(.6)
        else:
            thecoreconsumptionratio=coreconsumptionratio
            theramconsumptionratio=ramconsumptionratio
            thediskconsumptionratio=diskconsumptionratio

        if ram!='UNK':
            if float(ram)<15000:
                thecoreconsumptionratio=str(.2)
                theramconsumptionratio=str(.2)
    

        count=int(cardcount)
        if count>0: 
            for i in range(count):
                cardstring='-'+str(i)
                string=node+cardstring+' '+gpustring+' '+cardtype+' '+nproc+' '+ram+' '+scratch+' '+thecoreconsumptionratio+' '+theramconsumptionratio+' '+thediskconsumptionratio+'\n'
                temp.write(string)
        else:
            cardstring='-'+str(0)
            string=node+cardstring+' '+gpustring+' '+cardtype+' '+nproc+' '+ram+' '+scratch+' '+thecoreconsumptionratio+' '+theramconsumptionratio+' '+thediskconsumptionratio+'\n'
            temp.write(string)

    temp.close()


def NonActiveNodes(cpunodelist,gpunodelist,activecpunodelist,activegpunodelist):
    nonactivecpunodelist=[]
    nonactivegpunodelist=[]
    for node in cpunodelist:
        if node not in activecpunodelist:
            nonactivecpunodelist.append(node)

    for node in gpunodelist:
        if node not in activegpunodelist:
            nonactivegpunodelist.append(node)

    return nonactivecpunodelist,nonactivegpunodelist

def UpdateTotalTime(nodesnonactivetototaltime,nodesnonactive,nodesnonactivetofirsttime,string,nodetousernames,usernametomsgs):
    totaltimetol=2 # days
    for node in nodesnonactive:
        currenttime=time.time()
        if node not in nodesnonactivetofirsttime.keys():    
            nodesnonactivetofirsttime[node]=currenttime
            firsttime=currenttime
        else:
            firsttime=nodesnonactivetofirsttime[node]

        totaltime=currenttime-firsttime
        nodesnonactivetototaltime[node]=totaltime
    nodesneedtoberemoved=[]
    for node in nodesnonactivetofirsttime.keys():
        if node not in nodesnonactive: # then was not active but now is need to remove (reset) firsttime
            nodesneedtoberemoved.append(node)
    for node in nodesneedtoberemoved:
        del nodesnonactivetofirsttime[node]      
    for node,totaltime in nodesnonactivetototaltime.items():
        totaltime=totaltime/86400
        if totaltime>=totaltimetol:
            usernames=nodetousernames[node]
            if len(usernames)>0:
                username=usernames[0]
                msg=string+' Node '+node+' was claimed by username '+username+' but has been unactive for a period of '+str(totaltimetol)+' days'
                if username not in usernametomsgs.keys():
                    usernametomsgs[username]=[]
                if msg not in usernametomsgs[username]:
                    usernametomsgs[username].append(msg)
    
    return nodesnonactivetototaltime,nodesnonactivetofirsttime,usernametomsgs

def MonitorResourceUsage(nodetopofilepath,cpuprogramlist,usernametoemaillist,senderemail,senderpassword):
    cpunodesnonactivetototaltime={}
    cpucardsnonactivetofirsttime={}
    gpunodesnonactivetototaltime={}
    gpucardsnonactivetofirsttime={}
    while True:
        usernames,usernametoemail=ReadUsernameList(usernametoemaillist)
        usernametomsgs={}
        cpunodelist,gpunodelist,cpucardlist,gpucardlist,cpunodetousernames,gpunodetousernames=ReadNodeTopology(nodetopofilepath)
        activecpunodelist,activegpunodelist=AlreadyActiveNodes(cpunodelist,gpunodelist,cpuprogramlist)
        nonactivecpunodelist,nonactivegpunodelist=NonActiveNodes(cpucardlist,gpucardlist,activecpunodelist,activegpunodelist)
        gpunodesnonactivetototaltime,gpunodesnonactivetofirsttime,usernametomsgs=UpdateTotalTime(gpunodesnonactivetototaltime,nonactivegpunodelist,gpucardsnonactivetofirsttime,'GPU',gpunodetousernames,usernametomsgs)
        cpunodesnonactivetototaltime,cpunodesnonactivetofirsttime,usernametomsgs=UpdateTotalTime(cpunodesnonactivetototaltime,nonactivecpunodelist,cpucardsnonactivetofirsttime,'CPU',cpunodetousernames,usernametomsgs)
        send=SendEmails(usernametomsgs,usernametoemail,senderemail,senderpassword)
        if send==True:
            cpunodesnonactivetototaltime={}
            cpucardsnonactivetofirsttime={}
            gpunodesnonactivetototaltime={}
            gpucardsnonactivetofirsttime={}

        time.sleep(1)

def SendEmails(usernametomsgs,usernametoemail,senderemail,senderpassword):
    send=False
    for username,msgs in usernametomsgs.items():
        msg='\n'.join(msgs)
        msg+='\n'
        msg+='Please remove reservation or use the resource allocation '+'\n'
        email=usernametoemail[username]
        SendReportEmail(msg,senderemail,email,senderpassword)      
        send=True
    return send
        


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



def SendReportEmail(TEXT,fromaddr,toaddr,password):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = 'Resource Monitor Usage Report '
    message = TEXT
    msg.attach(MIMEText(message, 'plain'))
    s = smtplib.SMTP_SSL('smtp.gmail.com')
    s.ehlo()
    s.login(fromaddr,password)
    text = msg.as_string()
    s.sendmail(fromaddr, [toaddr],text)
    s.quit()

 


cpuprogramlist=['bar','dynamic','psi4','g09','g16',"cp2k.ssmp","mpirun_qchem","dynamic.x",'minimize.x','minimize','poltype.py','amoebaannihilator.py']

nodelist=ReadNodeList(nodelistfilepath)
if monitorresourceusage==False:
    gpunodes,nodetototalram,nodetototalcpu,nodetototalscratch,nodetocardcount,nodetocardtype,nodefailanalyze=PingNodesAndDetermineNodeInfo(nodelist,bashrcpath)
    WriteOutNodeInfo(filename,gpunodes,nodetototalram,nodetototalcpu,nodetototalscratch,nodelist,coreconsumptionratio,ramconsumptionratio,diskconsumptionratio,nodetocardcount,nodetocardtype,mincardtype,nodefailanalyze)

else:
    MonitorResourceUsage(nodetopofilepath,cpuprogramlist,usernametoemaillist,senderemail,senderpassword)

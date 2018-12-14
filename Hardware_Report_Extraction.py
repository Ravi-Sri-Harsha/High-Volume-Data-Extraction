########################################################################################################
# script: Hardware_Report.py
# author: Sri Harsha Ravi
# Info  : Taken from the XML push adapter and amended to cater for custom CSV output with normalisation
#         Script based on existing UD Data         
########################################################################################################
from __future__ import with_statement

import string
import re
import codecs
import logger
import time

import datetime
from java.io import File, BufferedWriter, FileWriter, FileInputStream
from java.util import Calendar
from com.hp.ucmdb.discovery.library.common import CollectorsParameters


#######################

import netutils
import shellutils

from com.hp.ucmdb.discovery.library.clients import ClientsConsts
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

from com.hp.ucmdb.api import UcmdbServiceFactory
from com.hp.ucmdb.discovery.library.common import CollectorsParameters
from com.hp.ucmdb.discovery.library.credentials.dictionary import ProtocolDictionaryManager


from java.util import HashMap
from java.util import HashSet
from java.util import TreeSet


##############################################
########      VARIABLES             ##########
##############################################

C__INTEGRATION_USER = "UCMDBQuery"

months = {1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May', 6:'Jun', 7:'Jul', 8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec'}

TIMESTAMP   = "%02d_%s_%04d" % (Calendar.getInstance().get(Calendar.DAY_OF_MONTH),
                                  months[Calendar.getInstance().get(Calendar.MONTH)+1],
                                  Calendar.getInstance().get(Calendar.YEAR)
                                  )

TIMESTAMPSHORT   = "%02d%s%02d" % (Calendar.getInstance().get(Calendar.DAY_OF_MONTH),
                                months[Calendar.getInstance().get(Calendar.MONTH)+1],
                                Calendar.getInstance().get(Calendar.YEAR)-2000
                                )

TIMESTAMPREV   = "%04d-%02d-%02d" % (Calendar.getInstance().get(Calendar.YEAR),
                                    Calendar.getInstance().get(Calendar.MONTH) + 1,
                                    Calendar.getInstance().get(Calendar.DAY_OF_MONTH)
                                    )

#TIMESTAMP   = "%d%d%d%d%d%d%d" % (Calendar.getInstance().get(Calendar.YEAR)-2000,
#                                  Calendar.getInstance().get(Calendar.MONTH),
#                                  Calendar.getInstance().get(Calendar.DAY_OF_MONTH),
#                                  Calendar.getInstance().get(Calendar.HOUR_OF_DAY),
#                                  Calendar.getInstance().get(Calendar.MINUTE),
#                                  Calendar.getInstance().get(Calendar.SECOND),
#                                  Calendar.getInstance().get(Calendar.MILLISECOND)
#                                  )


ADAPTER_NAME          = "Hardware Report"
FILE_SEPARATOR        = "\\"

C__FILTERED_CUTSOMER_CLASSIFICATION = ("Green" , "Blue")
C__FILTERED_SYSTEM_STATUS           = ("in production")
##############################################################################################
#                                        GLOBALS
##############################################################################################
g__NodeWithSWandCPUQty = 0
g__SortedData = TreeSet()


adapterConfigBaseDir  = "%s%s%s%s" % (CollectorsParameters.BASE_PROBE_MGR_DIR, CollectorsParameters.getDiscoveryConfigFolder(), FILE_SEPARATOR, ADAPTER_NAME)

def quote(stringin):
    return '"' + str(stringin) + '"'

class CPUData :
    def __init__(self):
        self.cputype = ''
        self.cpuspeed = ''
        self.cpuvendor = ''
        self.physicalcpus = ''
        self.corecount = ''
        self.logicalcpucount = ''
        self.sockets = ''
        self.hyperthreading = ''

class UDAData :
    def __init__(self):
        self.scandate = ''
        self.builddate = ''
        self.primaryIP = ''
        self.containername = ''
    
class ITAMline :
    # A 'constructor'
    def __init__(self):
        #  self.deviceid = ''
        self.servername = ''
        self.country = ''
        self.domain = ''
        self.primaryIP = ''
        self.servermodel = ''
        self.servercompany = ''
        self.serialnumber = ''
        self.logicalcpucount = ''
        self.physicalcpus = ''
        self.corecount = ''
        self.sockets = ''
        self.cputype = ''
        self.cpuspeed = ''
        self.cpuvendor = ''
        self.memory = ''
        self.OS = ''
        self.OSdescription = ''
        self.virtualtype = ''
        self.scandate = ''
        self.workflow = ''
        self.NetworkZone = ''
        self.superstack = ''
        self.builddate = ''
        self.hyperthreading = ''
        self.primarydnsname = ''
        self.osinstallationtype = ''
        self.osrelease = ''
        self.description = ''
        
    #############################################
    ####       Execution Functions           ####
    #############################################


    #
    # the following return the data for the reports in the format each report is required in
    # parameter passed in is the report to be produced and the output is then sent as appropriate
    #

    def exportfileName(self, repname):
        if repname == "HARDWAREALL":
            return "HARDWARE_Extract.csv"

    def exportHeader(self, repname):
        if repname == "HARDWAREALL":
            return '"Server","Country","Domain","IP","Model","Manufacturer","Serial Number","CPU Count","Physical CPUs","Core Count","Sockets","CPU Type","CPU Speed","CPU Vendor","Memory","OS","OS Details","Virtual Type","Scan Date","Workflow","Network Zone","SuperStack","Build Date","HyperThreading","Primary DNS Name","OS Installation Type","OS Release","Description"'

        return ''

    def exportData(self, repname):
        if repname == "HARDWAREALL":
            return ','.join([ quote(self.servername), quote(self.country), quote(self.domain), quote(self.primaryIP), quote(self.servermodel), quote(self.servercompany), quote(self.serialnumber), quote(self.logicalcpucount), quote(self.physicalcpus), quote(self.corecount), quote(self.sockets), quote(self.cputype), quote(self.cpuspeed), quote(self.cpuvendor), quote(self.memory), quote(self.OS), quote(self.OSdescription), quote(self.virtualtype), quote(self.scandate), quote(self.workflow), quote(self.NetworkZone), quote(self.superstack), quote(self.builddate), quote(self.hyperthreading), quote(self.primarydnsname), quote(self.osinstallationtype), quote(self.osrelease), quote(self.description)])


# check to see if a key exists in a dictionary and return the value if it does
# if not it returns ''
def checkdict(array, key):
    if key in array:
        return array[key]
    else:
        return ''


def checkdictnum(array, key):
    if key in array:
        return array[key]
    else:
        return 0


def removenone(s):
    if s == 'None':
        return ''
    return s

def validateDirectory(Framework):
    exportDirectory = Framework.getTriggerCIData("Export Directory")
    if exportDirectory != None and exportDirectory != "":
        dir = File(exportDirectory)
        if dir.exists() and dir.isDirectory():
            return 1
        return 0


def isEmpty(xml, type = ""):
    objectsEmpty = 0
    linksEmpty = 0

    m = re.findall("<objects />", xml)
    if m:
        logger.debug("\t[%s] No objects found" % type)
        objectsEmpty = 1

    m = re.findall("<links />", xml)
    if m:
        logger.debug("\t[%s] No links found" % type)
        linksEmpty = 1

    if objectsEmpty and linksEmpty:
        return 1
    return 0


def writeFile(expDirPath, fileName, result):
    endOfData = ""
    #if isLastChunk.lower() == 'true':
        #endOfData = "-EOD"
    fileName = "%s/%s" % (expDirPath, fileName)
    with codecs.open(fileName, "w", "UTF-8") as file:
        file.write(result)
#    writer = BufferedWriter(FileWriter("%s/%s-%s-%s%s.xml" % (expDirPath, queryName, type, TIMESTAMP, endOfData)))
#    writer.write(result)
#    writer.close()

def openWrite(expDirPath, fileName):
    fileName = "%s/%s" % (expDirPath, fileName)
    file = codecs.open(fileName, "w", "UTF-8")
    return file

def writeLine(file, line):
    write = file.writelines(line)

def checkint(s):
    try:
        int(s)
        return s
    except:
        return 0

def emptyzero(val):
    if val == 0:
        return ''
    else:
        return val        
        
##############################################################################################
#     Function        : removeExtra
#     Description     : This function will remove all the commas and return the value
##############################################################################################
def removeExtra(io__Data):
    if io__Data:
        io__Data = string.replace(io__Data.decode('utf-8', 'ignore'),',','')

        #io__Data = string.replace(format(io__Data),',','')
        io__Data = io__Data.strip()

        io__Data = io__Data.replace("\r", " ")
        #io__Data = io__Data.replace("\n", " ")

    return removenone(io__Data)

#  Normalise the data for the CPU Type attribute based on the script from J Newman 
def normaliseCPUType(cpustr):
    #logger.debug("cpu pre %s" % cpustr)
    cpustr = re.sub(r"\(.+\)", "", cpustr)
    cpustr = re.sub(r"\s+", " ", cpustr)
    cpustr = re.sub(r"\- ", "", cpustr)
    cpustr = re.sub(r"\s+$", "", cpustr)
    #logger.debug("cpu post %s" % cpustr)
    return cpustr

def getUDAScantime(CIobject):
    udaobj = UDAData()
    for l__TopologyRelation in CIobject.getOutgoingRelations():
        relcitype = removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("root_class")))              
        logger.debug(relcitype)
        if relcitype == 'uda':
            scantime = removenone(removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("root_lastaccesstime"))))
            builddate = removenone(removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("create_time"))))
            primaryIP = removenone(removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("application_ip"))))
            containername = removenone(removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("root_container_name"))))
            udaobj.scandate = scantime
            udaobj.builddate = builddate
            udaobj.primaryIP = primaryIP
            udaobj.containername = containername
    return udaobj      

def getCPUsummary(CIobject):
    physicalcpus = 0
    sockets = 0    
    logicalcpucount = 0
    corecount = 0
    cpuspeed = 0
    cpuvendor = ''
    cputype = ''
    cpuobj = CPUData()
    try:
        for l__TopologyRelation in CIobject.getOutgoingRelations():
            relcitype = removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("root_class")))              
            #logger.debug(relcitype)
            if relcitype == 'cpu':
                physicalcpus += 1
                sockets += 1
                logicalcpucount += int(checkint(l__TopologyRelation.getEnd2CI().getPropertyValue("logical_cpu_count")))
                corecount += int(checkint(l__TopologyRelation.getEnd2CI().getPropertyValue("core_number")))
                cpuspeed = removenone(removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("cpu_clock_speed"))))
                cpuvendor = removenone(removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("cpu_vendor"))))
                cputype = removenone(normaliseCPUType(removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("name"))))).decode(encoding='UTF-8',errors='strict')
                hyperthreading = removenone(str(l__TopologyRelation.getEnd2CI().getPropertyValue("hyper_thread")))
                cpuobj.cputype = cputype 
                cpuobj.cpuvendor = cpuvendor
                cpuobj.cpuspeed = emptyzero(cpuspeed)
                cpuobj.physicalcpus = emptyzero(physicalcpus)
                cpuobj.corecount = emptyzero(corecount)
                cpuobj.logicalcpucount = emptyzero(logicalcpucount)
                cpuobj.sockets = emptyzero(sockets)
                cpuobj.hyperthreading = hyperthreading
    except:
        logger.debug('in except')            

    #return '"%s","%s","%s","%s"' % (emptyzero(physicalcpus), emptyzero(corecount), emptyzero(cpuspeed), cputype)
    return cpuobj

def getCICollectionsummary(CIobject):
    superstack = ''
    for l__TopologyRelation in CIobject.getIncomingRelations():
        relcitype = removeExtra(str(l__TopologyRelation.getEnd1CI().getPropertyValue("root_class")))
        if relcitype == 'ci_collection':
            logger.debug("ci collection")
            superstack = removeExtra(str(l__TopologyRelation.getEnd1CI().getPropertyValue("name")))
            logger.debug(superstack)
    return superstack

def getConfigurationdocumentdata(CIobject):
    accesstime = ''
    for l__TopologyRelation in CIobject.getOutgoingRelations():
        relcitype = removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("root_class")))
        if relcitype == 'configuration_document':
            logger.debug("configuration document")
            accesstime = removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("root_lastaccesstime"))) 
            logger.debug(accesstime)
    return accesstime
     
def makeReport(i__UCMDBService, i__dir, i__FileName, queryTQL):
    logger.info('************* START makeReport *************')
    l__QueryService    = i__UCMDBService.getTopologyQueryService()

    #The unique name of the query node of type "node"
    #l__NodeName    = "Node"
    #Creating a query node from type "host_node" and asking for all returned nodes the display_label

    #Execute the unsaved query
    #__Topology            = l__QueryService.executeQuery(l__QueryDefinition)
    l__Topology            = l__QueryService.executeNamedQuery(queryTQL)
    #Get the node results
    l__NodeCollection     = l__Topology.getCIsByName("Node")
    logger.debug(queryTQL + ' : ' + str(l__NodeCollection))
    #Go over the nodes and print its related IPs
    l__NodeQty  = 0
    #l__new = sorted(l__NodeCollection,key=attrgetter('label'))
    #l__new = sorted(l__NodeCollection,key=lambda l__NodeCollection:l__NodeCollection[2])

    #sorted(l__NodeCollection, key=itemgetter(2))
    #logger.debug("\\\\\\\\\\\\\\\\"+l__NodeCollection.toString())

    if l__NodeCollection:
        newCI = ITAMline()
        toprow = newCI.exportHeader("HARDWAREALL")
        logger.debug("toprow : " + toprow)
        output = ''


        filename = newCI.exportfileName("HARDWAREALL")
        file = openWrite(i__dir, filename)

        if toprow != '':
            output = toprow + '\r\n'
            writeLine(file, output)
    chunks = {}
    chunkcount = 0
    chunksize = 1000
    for l__CITypeNode in l__NodeCollection:
        if l__CITypeNode:
            logger.debug(l__CITypeNode)
            servername = removeExtra(str(l__CITypeNode.getPropertyValue("display_label")))
            serverid = removeExtra(str(l__CITypeNode.getPropertyValue("global_id")))    
            #logger.debug(servername)
            intnum = 999
            if ((chunkcount % chunksize) == 0):
                chunks[(int(chunkcount / chunksize))] = []
            chunks[(int(chunkcount / chunksize))].append(serverid)
            chunkcount = chunkcount + 1
    if(chunkcount > 0):
        for id in chunks:
            idlist = chunks[id]
            logger.debug('CI %s : %s' % (id, idlist))
            max_retry = 1000
            try:
                while(max_retry > 0):
                    l__QueryExec = l__QueryService.createExecutableQuery("DDMi_Hardware_Report")
                    l__QueryExec.nodeRestrictions("Node").restrictToIdsFromStrings(idlist)
                    l__Topologyhw = l__QueryService.executeQuery(l__QueryExec)
                    l__NodeCollection2 = l__Topologyhw.getCIsByName("Node")
                    #logger.debug('in while loop - %s' % l__NodeCollection2)
                    max_retry = 0
                for hwnode in l__NodeCollection2:
                    #logger.debug('in for loop - %s' % hwnode)
                    servername = removeExtra(str(hwnode.getPropertyValue("display_label")))
                    serverid = removeExtra(str(hwnode.getPropertyValue("global_id")))
                    country = removeExtra(str(hwnode.getPropertyValue("region")))
                    domain = removeExtra(str(hwnode.getPropertyValue("domain_name")))
                    #logger.debug('servername - %s domain - %s test-test' %(servername, domain))
                    servermodel = removeExtra(str(hwnode.getPropertyValue("discovered_model")))
                    servercompany = removeExtra(str(hwnode.getPropertyValue("discovered_vendor")))
                    serialnumber = removeExtra(str(hwnode.getPropertyValue("serial_number")))
                    memory = removeExtra(str(hwnode.getPropertyValue("memory_size")))
                    OS = removeExtra(str(hwnode.getPropertyValue("discovered_os_name")))
                    OSdescription = removeExtra(str(hwnode.getPropertyValue("os_family")))
                    virtualtype = removeExtra(str(hwnode.getPropertyValue("host_isvirtual")))
                    NetworkZone = removeExtra(str(hwnode.getPropertyValue("data_note")))
                    serverprimaryip = removeExtra(str(hwnode.getPropertyValue("primary_ip_address")))
                    serverdnsname = removeExtra(str(hwnode.getPropertyValue("primary_dns_name")))
                    servervendor = removeExtra(str(hwnode.getPropertyValue("vendor")))
                    nodemodel = removeExtra(str(hwnode.getPropertyValue("node_model")))
                    createtime = removenone(removeExtra(str(hwnode.getPropertyValue("create_time"))))
                    primarydnsname = removeExtra(str(hwnode.getPropertyValue("primary_dns_name")))
                    osinstallationtype = removeExtra(str(hwnode.getPropertyValue("host_osinstalltype")))
                    osrelease = removeExtra(str(hwnode.getPropertyValue("host_osrelease")))
                    description = removeExtra(str(hwnode.getPropertyValue("description")))
                    scandata = getUDAScantime(hwnode)
                    cpudata = getCPUsummary(hwnode)
                    cicollectiondata = getCICollectionsummary(hwnode)
                    configurationdocumentdata = getConfigurationdocumentdata(hwnode)
                #logger.debug(servername)
                #logger.debug(servername)
                #max_retry = 100
                    try:
                        newCI = ITAMline()
                        newCI.servername = servername
                        newCI.country = country
                        newCI.domain = domain
                        if scandata.primaryIP != '':
                            newCI.primaryIP = scandata.primaryIP
                        else:
                            newCI.primaryIP = serverprimaryip
                        if servermodel != '':
                            newCI.servermodel = servermodel
                        else:
                            newCI.servermodel = nodemodel
                        if servercompany != '':
                            newCI.servercompany = servercompany
                        else:
                            newCI.servercompany = servervendor
                        
                        newCI.serialnumber = serialnumber
                        newCI.OS = OS
                        newCI.physicalcpus = cpudata.physicalcpus
                        newCI.sockets = cpudata.sockets
                        newCI.corecount = cpudata.corecount
                        newCI.cputype = cpudata.cputype
                        newCI.cpuvendor = cpudata.cpuvendor
                        newCI.logicalcpucount = cpudata.logicalcpucount
                        newCI.cpuspeed = cpudata.cpuspeed
                        newCI.memory = memory
                        newCI.OSdescription = OSdescription
                        newCI.virtualtype = virtualtype
                        if scandata.scandate != '':
                            newCI.scandate = scandata.scandate
                        else:
                            newCI.scandate = configurationdocumentdata
                            
                        newCI.workflow = scandata.containername
                        newCI.NetworkZone = NetworkZone
                        newCI.superstack = cicollectiondata
                        if scandata.builddate != '':
                            newCI.builddate = scandata.builddate
                        else:
                            newCI.builddate = createtime     
                        newCI.hyperthreading = cpudata.hyperthreading
                        newCI.osinstallationtype = osinstallationtype
                        newCI.osrelease = osrelease
                        newCI.description = description
                        newCI.primarydnsname = primarydnsname
                        outstr = newCI.exportData("HARDWAREALL")
                        if outstr != '':
                            #logger.debug("datathere")
                            writeLine(file, outstr + '\r\n')
                    except:
                        logger.debug('In exception')        

            except Exception as e:
                logger.debug('UCMDB query for %s failed with %s, pausing for 5 seconds and retrying (count = %d)' % (servername, e, max_retry))
                time.sleep(5)
                max_retry = max_retry - 1
                if (max_retry == 0):
                    logger.debug('Abandoning query attempts')
                        #outputlist.append(newCI)
                        #logger.debug(servername + ' : ' + newCI.applicationname)'''

            #filename = filename[:-3] + 'success'
            #fileok = openWrite(i__dir, filename)
            #writeLine(fileok, 'Report generated successfully')

    logger.info('************* END makeReport *************')


    #return outputlist




##############################################################################################
#     Function        : createCMDBConnection
#     Description     : This function creates a connection with UCMDB and return the context
##############################################################################################
def createCMDBConnection(i__LocalShell):

    logger.info('************* START createCMDBConnection *************')
    l__HostName = CollectorsParameters.getValue(CollectorsParameters.KEY_SERVER_NAME)
    l__HTTPPort = int(CollectorsParameters.getValue(CollectorsParameters.KEY_SERVER_PORT_HTTP))
    l__HTTPSPort = int(CollectorsParameters.getValue(CollectorsParameters.KEY_SERVER_PORT_HTTPS))

    l__ProtocolParameters = ProtocolDictionaryManager.getProtocolParameters('genericprotocol', netutils.resolveIP(i__LocalShell, l__HostName))

    l__UserName = ''

    for l__Protocol in l__ProtocolParameters:
        if l__Protocol.getProtocolAttribute('protocol_username') == C__INTEGRATION_USER:
            l__UserName = l__Protocol.getProtocolAttribute('protocol_username')
            l__UserPassword = l__Protocol.getProtocolAttribute('protocol_password')
            break

    if not l__UserName:
        logger.error('Error Username Protocol not initialized')
        return None

    #logger.debug('Accessing uCMDB = ',(l__HostName, l__HTTPPort, l__UserName))
    # try http first
    try:
        logger.debug('Attempting HTTP connection')
        l__Provider = UcmdbServiceFactory.getServiceProvider('http', l__HostName, l__HTTPPort)
    except:
        logger.debug('HTTP connection failed, trying HTTPS')
        UcmdbServiceFactory.initSSL()
        l__Provider = UcmdbServiceFactory.getServiceProvider('https', l__HostName, l__HTTPSPort)

    l__Credentials = l__Provider.createCredentials(l__UserName, l__UserPassword)
    l__ClientContext = l__Provider.createClientContext("UD")
    o__UcmdbService = l__Provider.connect(l__Credentials, l__ClientContext)
    logger.info('************* END createCMDBConnection *************')
    return o__UcmdbService



##############################################
########      MAIN                  ##########
##############################################
def DiscoveryMain(Framework):

    errMsg = "Export Directory is not valid. Ensure export directory exists on the probe system."
    testConnection = Framework.getTriggerCIData("testConnection")
    if testConnection == 'true':
        # check if valid export directory exists
        isValid = validateDirectory(Framework)
        if not isValid:
            raise Exception, errMsg
            return
        else:
            logger.debug("Test connection was successful")
            return

    i__Framework = Framework
    logger.info('************* START MAIN *************')
    l__LocalShell   = shellutils.ShellUtils(i__Framework.createClient(ClientsConsts.LOCAL_SHELL_PROTOCOL_NAME))
    l__OSHVResult   = ObjectStateHolderVector()
    l__UCMDBService = createCMDBConnection(l__LocalShell)

    logger.debug(str(l__UCMDBService))

    if not validateDirectory(Framework):
        logger.error(errMsg)
        raise Exception, errMsg
        return

    expDirPath = Framework.getTriggerCIData("Export Directory")
    queryTQL = Framework.getTriggerCIData("TQL Input Name")
    isLastChunk = Framework.getTriggerCIData("isLastChunk")

    logger.debug("Generating Hardware report output")
    # clean up XML
    empty = 0 ##isEmpty(addResult, "addResult")
    if not empty:
        ##addResult = replace(addResult, iplookup)
        if l__UCMDBService:
            logger.debug("Calling makereport")
            #addResult = makeReport(l__UCMDBService, expDirPath, 'testout.txt', queryTQL)
            makeReport(l__UCMDBService, expDirPath, 'testout.txt', queryTQL)


        logger.debug("Back from makereport")
        reports = [] #'ITAMALL']
        addResult = 0
        if addResult:

            for rep in reports:
                toprow = addResult[0].exportHeader(rep)
                logger.debug("report : " + rep)
                logger.debug("toprow : " + toprow)
                output = ''

                try:
                    filename = addResult[0].exportfileName(rep)
                    file = openWrite(expDirPath, filename)

                    if toprow != '':
                        output = toprow + '\r\n'
                        writeLine(file, output)
                    for ddmi in addResult:
                        outstr = ddmi.exportData(rep)
                        #logger.debug('out: ' + ddmi.exportData(rep))
                        if outstr != '':
                            #output = output + outstr + '\r\n'
                            writeLine(file, outstr + '\r\n')




                except:
                    logger.debug("Error writing to file for report " + rep + " filename - " + expDirPath + "/" + addResult[0].exportfileName(rep))
                #writeFile(expDirPath, addResult[0].exportfileName(rep), output)



    #empty = isEmpty(updateResult, "updateResult")
    #if not empty:
    #    updateResult = replace(updateResult)
    #    writeFile(expDirPath, queryName, "updateResult", updateResult, isLastChunk)
    #
    #empty = isEmpty(deleteResult, "deleteResult")
    #if not empty:
    #    deleteResult = replace(deleteResult)
    #    writeFile(expDirPath, queryName, "deleteResult", deleteResult, isLastChunk)

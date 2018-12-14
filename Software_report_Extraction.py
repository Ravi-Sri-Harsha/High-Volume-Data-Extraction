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


ADAPTER_NAME          = "HSBC New ITAM UD Report"
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
    
    
class ITAMline :
    # A 'constructor'
    def __init__(self):
        #  self.deviceid = ''
        self.softwarevendor = ''
        self.softwarename = ''
        self.releasename = ''
        self.productversion = ''
        self.devicelabel = ''
        self.domain = ''
        self.lastscannedtime = ''
        self.model = ''
        self.servercompany = ''
        self.serialnumber = ''
        self.OS = ''
        self.installedpath = ''
        self.logicalcpus = ''
        self.physicalcpus = ''
        self.corecount = ''
        self.socketcount = ''
        self.cputype = ''
        self.cpuspeed = ''
        self.cpuvendor = ''
        
    #############################################
    ####       Execution Functions           ####
    #############################################


    #
    # the following return the data for the reports in the format each report is required in
    # parameter passed in is the report to be produced and the output is then sent as appropriate
    #

    def exportfileName(self, repname):
        if repname == "UDALL":
            return "UD_ITAM_Extract_%s.csv" % (TIMESTAMP)

    def exportHeader(self, repname):
        if repname == "UDALL":
            return '"Company/Vendor","Application","Release","Version","Servername","Domain","Last Scanned","Server Model","Manufacturer","Serial Number","Logical CPUs","Physical CPUs","Core Count","Socket Count","CPU Type","CPU Speed","CPU Vendor","OS","Installed Directories"'

        return ''

    def exportData(self, repname):
        if repname == "UDALL":
            return ','.join([ quote(self.softwarevendor), quote(self.softwarename), quote(self.releasename), quote(self.productversion), quote(self.devicelabel), quote(self.domain), quote(self.lastscannedtime), quote(self.model), quote(self.servercompany), quote(self.serialnumber), quote(self.logicalcpus), quote(self.physicalcpus), quote(self.corecount), quote(self.socketcount), quote(self.cputype), quote(self.cpuspeed), quote(self.cpuvendor), quote(self.OS), quote(self.installedpath) ])


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
#     Description     : This function will removes all the commas and return the value
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
    scantime =''
    for l__TopologyRelation in CIobject.getOutgoingRelations():
        relcitype = removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("root_class")))              
        logger.debug(relcitype)
        if relcitype == 'uda':
            scantime = removenone(removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("root_lastaccesstime"))))
            logger.debug("reaching uda")
    return scantime       

def getCPUsummary(CIobject):
    physicalcpus = 0
    sockets = 0    
    cpucount = 0
    corecount = 0
    cpuspeed = 0
    cpuvendor = ''
    cputype = ''
    cpuobj = CPUData()
    for l__TopologyRelation in CIobject.getOutgoingRelations():
        relcitype = removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("root_class")))              
        logger.debug(relcitype)
        if relcitype == 'cpu':
            physicalcpus += 1
            sockets += 1
            cpucount += int(checkint(l__TopologyRelation.getEnd2CI().getPropertyValue("logical_cpu_count")))
            corecount = int(checkint(l__TopologyRelation.getEnd2CI().getPropertyValue("core_number")))
            cpuspeed = removenone(removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("cpu_clock_speed"))))
            cpuvendor = removenone(removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("cpu_vendor"))))
            cputype = removenone(normaliseCPUType(removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("name")))))
            cpuobj.cputype = cputype 
            cpuobj.cpuvendor = cpuvendor
            cpuobj.cpuspeed = emptyzero(cpuspeed)
            cpuobj.physicalcpus = emptyzero(physicalcpus)
            cpuobj.corecount = emptyzero(corecount)
            cpuobj.logicalcpucount = emptyzero(cpucount)
            cpuobj.sockets = emptyzero(sockets)    
    #return '"%s","%s","%s","%s"' % (emptyzero(physicalcpus), emptyzero(corecount), emptyzero(cpuspeed), cputype)
    return cpuobj

def makeReport(i__UCMDBService, i__dir, i__FileName, queryTQL):
    logger.info('************* START makeReport *************')
    l__QueryService    = i__UCMDBService.getTopologyQueryService()

    #The unique name of the query node of type "node"
    #l__NodeName    = "Node"
    #Creating a query node from type "host_node" and asking for all returned nodes the display_label

    #Execute the unsaved query
##    l__Topology            = l__QueryService.executeQuery(l__QueryDefinition)
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
        toprow = newCI.exportHeader("UDALL")
        logger.debug("toprow : " + toprow)
        output = ''


        filename = newCI.exportfileName("UDALL")
        file = openWrite(i__dir, filename)

        if toprow != '':
            output = toprow + '\r\n'
            writeLine(file, output)

    chunks = {}
    chunkcount = 0
    chunksize = 100
    #citlist = ["host_node","unix","nt"]
    for l__CITypeNode in l__NodeCollection:
        if l__CITypeNode:
            logger.debug(l__CITypeNode)
            servername = str(l__CITypeNode.getPropertyValue("name"))
            serverid = str(l__CITypeNode.getPropertyValue("global_id"))
            serverdomain = str(l__CITypeNode.getPropertyValue("domain_name"))
            #serverscannedtime = str(l__CITypeNode.getPropertyValue("root_lastaccesstime"))
            servermodel = str(l__CITypeNode.getPropertyValue("node_model"))
            servervendor = str(l__CITypeNode.getPropertyValue("vendor"))
            serverserialnumber = str(l__CITypeNode.getPropertyValue("serial_number"))
            serverOS = str(l__CITypeNode.getPropertyValue("discovered_os_name"))
            logger.debug(servername)
            intnum = 999

            if ((chunkcount % chunksize) == 0):
                chunks[(int(chunkcount / chunksize))] = []

            chunks[(int(chunkcount / chunksize))].append(serverid)
            chunkcount = chunkcount + 1


    if (chunkcount > 0):
        for id in chunks:
            # retry the query up to 100 times
            idlist = chunks[id]
            logger.debug('ITAM %s : %s' % (id, idlist))
            max_retry = 100
            while (max_retry > 0):
                try:
                    # Base query to get software for a node which will be dynamically run each time
                    logger.debug("execyqyery")
                    l__QueryExec = l__QueryService.createExecutableQuery("UD Node Software")
                    l__QueryExec.nodeRestrictions("Node").restrictToIdsFromStrings(idlist)

                    l__Topologysw = l__QueryService.executeQuery(l__QueryExec)
                    l__NodeCollection2 = l__Topologysw.getCIsByName("Node")
                    logger.debug(l__NodeCollection2)
                    max_retry = 0
                    for swnode in l__NodeCollection2:
                        logger.debug(swnode)
                        logger.debug("getall nnode related cis")
                        servername = removeExtra(swnode.getPropertyValue("name"))
                        serverid = removeExtra(swnode.getPropertyValue("global_id"))
                        serverdomain = removeExtra(swnode.getPropertyValue("domain_name"))
                        serverscannedtime = getUDAScantime(swnode)
                        servermodel = removeExtra(swnode.getPropertyValue("node_model"))
                        servervendor = removeExtra(swnode.getPropertyValue("vendor"))
                        serverserialnumber = removeExtra(swnode.getPropertyValue("serial_number"))
                        serverOS = removeExtra(swnode.getPropertyValue("discovered_os_name"))
                        logger.debug(servername)
                        cpudata = getCPUsummary(swnode)
                        
                        for l__TopologyRelation in swnode.getOutgoingRelations():
        
                            relcitype = removeExtra(str(l__TopologyRelation.getEnd2CI().getPropertyValue("root_class")))
                                
                            if relcitype == 'installed_software':
                                logger.debug("in mainloop")
                                newCI = ITAMline()
                                newCI.devicelabel = servername
                                newCI.deviceid = serverid
                                newCI.softwarevendor = removeExtra(l__TopologyRelation.getEnd2CI().getPropertyValue("discovered_vendor"))
                                newCI.softwarename = removeExtra(l__TopologyRelation.getEnd2CI().getPropertyValue("name"))
                                newCI.releasename = removeExtra(l__TopologyRelation.getEnd2CI().getPropertyValue("release"))
                                newCI.productversion = removeExtra(l__TopologyRelation.getEnd2CI().getPropertyValue("version"))
                                newCI.domain = serverdomain
                                newCI.lastscannedtime = serverscannedtime
                                newCI.model = servermodel
                                newCI.servercompany = servervendor
                                newCI.serialnumber = serverserialnumber
                                newCI.installedpath = removeExtra(l__TopologyRelation.getEnd2CI().getPropertyValue("file_system_path"))
                                newCI.OS = serverOS
                                newCI.physicalcpus = cpudata.physicalcpus
                                newCI.socketcount = cpudata.sockets
                                newCI.corecount = cpudata.corecount
                                newCI.cputype = cpudata.cputype
                                newCI.cpuvendor = cpudata.cpuvendor
                                newCI.logicalcpus = cpudata.logicalcpucount
                                newCI.cpuspeed = cpudata.cpuspeed
        
                                outstr = newCI.exportData("UDALL")
                                if outstr != '':
                                    logger.debug("datathere")
                                    writeLine(file, outstr + '\r\n')
                except Exception as e:
                    logger.debug('UCMDB query for %s failed with %s, pausing for 5 seconds and retrying (count = %d)' % (servername, e, max_retry))
                    time.sleep(5)
                    max_retry = max_retry - 1
                    if (max_retry == 0):
                        logger.debug('Abandoning query attempts')

                        #outputlist.append(newCI)
                        #logger.debug(servername + ' : ' + newCI.applicationname)

        filename = filename[:-3] + 'success'
        fileok = openWrite(i__dir, filename)
        writeLine(fileok, 'Report generated successfully')

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
    l__ClientContext = l__Provider.createClientContext("DDM")
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

    logger.debug("Generating ITAM report output")
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

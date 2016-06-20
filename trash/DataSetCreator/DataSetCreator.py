#!/usr/bin/env python
#
# Copyright Simon Skilevic
# Master Thesis for Chair of Databases and Information Systems
# Uni Freiburg
#
import sys, getopt, subprocess, time, os, errno

# --------------------------------------------------------------------------------
# Spark Settings (Change accordingly to your needs)
driverMemory = "2g"
# Prefix by "yarn" keyword for yarn cluster 
sparkMaster = "spark://dbisma01.informatik.privat:7077"
# Path to the datsetcreator JAR file
jarFilePath = "./S2RDF_DataSetCreator/target/scala-2.10/datasetcreator_2.10-1.0.jar"
# --------------------------------------------------------------------------------

# DatasetCreator args
# Database directory in hdfs
dbDir = ""
# Input RDF dataset filename (file has to be inside of the Database Directory)
inpuRDFFile = ""
# Default Upper Bound Scale
scaleUB = 0.25

# remove some file
def removeFile(filename):
    try:
        os.remove(filename)
    except OSError:
        pass

# write some line to the log file
def writeToLog(line):
    with open("./DataBaseCreator.log", "a") as logFile:
        logFile.write(line+"\n")

# Delay ins seconds for waiting until cluster is recovered after execution
# Probably gonna be no more necessary after spark update (actual Spark version 1.3)
def delay():
    delTime = 360
    writeToLog("Cluster recovering (time_out = " + str(delTime) + "sec)... ")
    time.sleep(delTime)
    writeToLog("Done!")

# Submits spark driver app to the cluster 
def submitSparkCommand(relationType):
    global jarFilePath
    global driverMemory
    global sparkMaster
    global dbDir, inpuRDFFile, scaleUB
    
    # generate submit command
    command = ("spark-submit --driver-memory "
    + driverMemory
    + " --class runDriver --master "
    + sparkMaster + " "
    + jarFilePath + " "
    + dbDir + " " + inpuRDFFile + " " + relationType + " " + "{:.1f}".format(scaleUB)
    + " > ./DataBaseCreator.err")
    
    writeToLog("Execute " + command + " ...")
    start = int(round(time.time()))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    out, err = process.communicate()
    end = int(round(time.time()))
    eTime =  "{:.0f}".format(end-start)
    writeToLog("Time--> "+ eTime +" sec")
    writeToLog("Done!")
    return end-start

# Generate ExtVP datasets in following order:
# VT->SO->OS->SS (VT has to be executed always first, since is needed for
# generation of other datasets)
def generateDatsets():
    removeFile("./DataBaseCreator.log")
    wholeRunTime = 0
    writeToLog("Generate Vertical Partitioning")
    wholeRunTime = submitSparkCommand("VP")
    delay()
    writeToLog("Generate Exteded Vertical Partitioning subset SO")
    wholeRunTime += submitSparkCommand("SO")
    delay()
    writeToLog("Generate Exteded Vertical Partitioning subset OS")
    wholeRunTime += submitSparkCommand("OS")
    delay()
    writeToLog("Generate Exteded Vertical Partitioning subset SS")
    wholeRunTime += submitSparkCommand("SS")
    writeToLog("Whole Run Time --> " + "{:.0f}".format(wholeRunTime) + " sec")

# Extracts file and path to the file into separate variables
def separateFileNameAndDir(str):
    begin = str.rfind("/")+1
    return str[:begin], str[begin:]

def main(argv):
    sourceFile = ''
    global scaleUB

    # pars arguments (actually just one argument: path to the input RDF file
    # in HDFS)
    try:
      opts, args = getopt.getopt(argv,"hi:s:",["rdfFile=", "scaleUB"])
    except getopt.GetoptError:
      print 'DataBaseCreator.py -i <inputRDFFile> [-s <ScaleUB>]'
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print 'DataBaseCreator.py -i <inputRDFFile> [-s <ScaleUB>]'
         sys.exit()
      elif opt in ("-i", "--rdfFile"):
         sourceFile = arg
      elif opt in ("-s", "--scaleUB"):
         scaleUB = float(arg)
    if (len(sourceFile) == 0):
        print 'DataBaseCreator.py -i <inputRDFFile> [-s <ScaleUB>]'
        sys.exit()
    print 'Input RDF file ->"', sourceFile
    global dbDir, inpuRDFFile
    dbDir, inpuRDFFile = separateFileNameAndDir(sourceFile)
    
    generateDatsets()

if __name__ == "__main__":
   main(sys.argv[1:])

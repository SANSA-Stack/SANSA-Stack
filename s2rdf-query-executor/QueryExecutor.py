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
# Path to the QueryExecutor JAR file
jarFilePath = "./S2RDF_QueryExecutor/target/scala-2.10/queryexecutor_2.10-1.0.jar"
# --------------------------------------------------------------------------------

# QueryExecutor args
# Database directory in hdfs
dbDir = ""
# Input file containing list of queries
queryFile = ""

# write some line to the log file
def writeToLog(line):
    with open("./DataBaseCreator.log", "a") as logFile:
        logFile.write(line+"\n")

# Submits QueryExecutor driver app to the cluster 
def startQueryExecutor():
    global jarFilePath, driverMemory, sparkMaster    
    global dbDir, queryFile
    
    # generate submit command
    command = ("spark-submit --driver-memory "
    + driverMemory
    + " --class runDriver --master "
    + sparkMaster + " "
    + jarFilePath + " "
    + dbDir + " " + queryFile
    + " > ./QueryExecutor.log")
    
    writeToLog("Execute " + command + " ...")
    start = int(round(time.time()))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    out, err = process.communicate()
    end = int(round(time.time()))
    eTime =  "{:.0f}".format(end-start)
    writeToLog("Time--> "+ eTime +" sec")
    writeToLog("Done!")
    return end-start

def main(argv):
    global dbDir, queryFile

    # pars arguments (actually just one argument: path to the input RDF file
    # in HDFS)
    try:
      opts, args = getopt.getopt(argv,"hd:q:",["dbDir=", "queryFile="])
    except getopt.GetoptError:
      print 'QueryExecutor.py -d <databaseDirectory> -q <querieListFile>'
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print 'QueryExecutor.py -d <databaseDirectory> -q <querieListFile>'
         sys.exit()
      elif opt in ("-d", "--dbDir"):
         dbDir = arg
      elif opt in ("-q", "--queryFile"):
         queryFile = arg
    if (len(queryFile) == 0 or len(dbDir) == 0 ):
        print 'QueryExecutor.py -d <databaseDirectory> -q <querieListFile>'
        sys.exit()
    print 'Query list file->', queryFile
    print 'Database Directory->', dbDir
    startQueryExecutor()

if __name__ == "__main__":
   main(sys.argv[1:])

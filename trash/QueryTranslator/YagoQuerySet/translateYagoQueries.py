#!/usr/bin/env python
#
# Copyright Simon Skilevic
# Master Thesis for Chair of Databases and Information Systems
# Uni Freiburg
#
import sys, getopt, subprocess
# --------------------------------------------------------------------------------
# Description

# This script generates for every SPARQL query in input directory two SQL queries
# (for two test cases, e.g. VP vs. ExtVP) using S2RDF_QueryTranslator. Futher more it 
# adds every generated SQL query to a compositeQueryFile.txt
# For example: The script generates for input SPARQL query IL5-1-U-2__VP_SO-OS-SS-VP.in
# and dataset WatDiv1M
# two SQL queries: IL5-1-U-2--SO-OS-SS-VP__WatDiv1M.sql (using VP+ExtVP tablesets SO,OS,SS)
# IL5-1-U-2--VP__WatDiv1M.sql (using only VP tableset) Thereby the postfix after '--'
# defines tablesets for usage in the testcase.
#
# Further the script adds both queries to compositeQueryFile:
# ...
# >>>>>>IL5-1-U-2--SO-OS-SS-VP__WatDiv1M
# [sqlQuery from IL5-1-U-2--SO-OS-SS-VP__WatDiv1M.sql]
# >>>>>>IL5-1-U-2--VP__WatDiv1M
# [sqlQuery from IL5-1-U-2--VP__WatDiv1M.sql]
# --------------------------------------------------------------------------------
# Settings

# ExtVP tables statistics directory and datasetName
# WatDiv1M directory corresponds to WatDivScale=10
# WatDiv1000M directory corresponds to WatDivScale=10000
dataSetName = "yago"
statisticsDir = "statistics/" + dataSetName + "/"
# S2RDF QueryTranslator jar
S2RDFQueryTranslator = "../S2RDF_QueryTranslator/queryTranslator.jar"
# upper bound for scale
scaleUB = "0.1"
# --------------------------------------------------------------------------------

# get the list of files in mypath
def loadListOfQueries(mypath):
    from os import listdir
    from os.path import isfile, join
    onlyfiles = [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]
    return onlyfiles

# extract testname and allowed tablesets
def parseFileName(str):
    end = str.rfind(".")
    begin = str.rfind("/")
    if (begin < 0):
        begin = 0;
    begin=begin+1;
    baseName = str[begin:-(len(str)-end)]
    temp = baseName.split("__");
    testName =  temp[0];
    tableSets =  temp[1];
    return testName, tableSets;

# generate tablesets arguments for S2RDF Query Translator
def parseArgs(str):
    result = "";
    if "SO" in str: result+=" -so";
    if "OS" in str: result+=" -os";
    if "SS" in str: result+=" -ss";
    return result;

def readFileToString(fileName):    
    with open(fileName) as myFile: return myFile.read()
    
def addStringToFile(fileName, str, queryName):
    with open(fileName, "a") as myFile: myFile.write(">>>>>>"+queryName+"\n"+str)
    
# execute S2RDF Query Translator
def executeCommand(fileName, sqlDir, tableName, baseName, args):
    global statisticsDir, S2RDFQueryTranslator, scaleUB
    outPutFileName = (sqlDir+"/"+baseName+"__"+tableName).replace("//", "/")
    command = ("java -jar " + S2RDFQueryTranslator
    + " -i " + fileName + " -o " + outPutFileName
    + " " + args
    +" -sd " + statisticsDir +" -sUB " + scaleUB)
    print("\n" + command)
    status =  subprocess.call(command, shell=True);
    query = readFileToString(outPutFileName+".sql")
    
    # add to composite query file (all queries of the input directory)
    addStringToFile(sqlDir+"compositeQueryFile.txt", query, baseName+"__"+tableName)
    command = command.replace("//", "/")
    return status

def translateQuery(fileName, sqlDir):    
    global statisticsDir, dataSetName
    if ((not "~" in fileName) and (not "rdf3x" in fileName)):
        print ("Parse " + fileName)
        testName, tableSets = parseFileName(fileName)
        for case in tableSets.split("_"):
	    if not (scaleUB!="1" and case=="VP"):
            	args = parseArgs(case)
            	status = executeCommand(fileName, sqlDir, dataSetName, testName+"--"+case, args)
        subprocess.call("rm -f "+sqlDir+"/*.log", shell=True)

def main(argv):
    sparqlDir = ''
    sqlDir = ''
    try:
      opts, args = getopt.getopt(argv,"hs:t:",["sdir=","tdir="])
    except getopt.GetoptError:
      print 'translateWatDivQueries.py -s <sparqlDir> -t <sqlDir>'
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print 'translateWatDivQueries.py -s <sparqlDir> -t <sqlDir>'
         sys.exit()
      elif opt in ("-s", "--sparqlDir"):
         sparqlDir = arg
      elif opt in ("-t", "--sqlDir"):
         sqlDir = arg
    if (len(sparqlDir) == 0):
        print 'translateWatDivQueries.py -s <sparqlDir> -t <sqlDir>'
        sys.exit()
    if (len(sqlDir) == 0):
        sqlDir = "./";
    print 'Input Dir is "', sparqlDir
    print 'Output Dir is "', sqlDir
    sparqlList = loadListOfQueries(sparqlDir);
    subprocess.call("rm -f "+sqlDir+"/*.*", shell=True)
    
    for fileName in sparqlList:
        translateQuery(sparqlDir+"/"+fileName, sqlDir)

if __name__ == "__main__":
   main(sys.argv[1:])


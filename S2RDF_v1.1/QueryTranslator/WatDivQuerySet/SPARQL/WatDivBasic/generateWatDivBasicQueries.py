#!/usr/bin/env python
#
# Copyright Simon Skilevic
# Master Thesis for Chair of Databases and Information Systems
# Uni Freiburg
#
import sys, getopt, subprocess
# -------------------------------------------------------------------------------
# Description

# This script generates WatDiv Basic SPARQL queries.
# It becomes as input a directory containing templates with one placeholder each
# and corresponding parameters, as well as WatDiv dataset scale.
# The script generates than for every suitable
# combination query-template<->parameter one query
# Necessary directory structure ->

# Templates:
# [inputDir]/templates/[templateFile]

# Parameters:
# [inputDir]/parameters/Scale[WatDivScale]/[parameterFile]
# Parameter-file and template-file must have equals names

# Output format:
# [inputDir]/[WatDivScale]/[templateName]__[testCase1]_[testCase2].in

# -------------------------------------------------------------------------------
# Settings

templatesDir = "/templates/"
parametersDir = "/parameters/Scale"
# -------------------------------------------------------------------------------
# watDivScaleFactor
scaleFactor = ""

def extractTemplates(mypath):
    from os import listdir
    from os.path import isfile, join
    onlyfiles = [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]
    return onlyfiles

def extractBaseName(str):
    end = str.rfind(".")
    begin = str.rfind("/")
    if (begin < 0):
        begin = 0;
    return str[begin:-(len(str)-end)]

def readFileToString(fileName):    
    with open(fileName) as myFile:
        return myFile.read();

def writeStringToFile(fileName, str):
    with open(fileName, "w") as myFile:
        myFile.write(str)    

# for every parameter in parameter-file create a query
# by replacing of placeholder %v1% with this parameter
# if template-file contains no placeholder than save it
# once as result sparql-query
def handleQueryTemplate(fileName, targetDir):    
    if not "~" in fileName:
        baseName = extractBaseName(fileName);	
        query = readFileToString(fileName).replace("\r", "");
        
        if "\"%v1%\"" in query:
            substitutions = readFileToString(parametersDir+baseName+".txt").replace("\r", "").split("\n");
            fileId = 1;
            for case in substitutions:
                newQuery = query.replace("\"%v1%\"", case);
                writeStringToFile(targetDir+"/"+baseName+"-"+str(fileId)+"__VP_SO-OS-SS-VP.in", newQuery)
                fileId = fileId + 1
        else:
            writeStringToFile(targetDir+"/"+baseName+"__VP_SO-OS-SS-VP.in", query)

def main(argv):
    sourceDir = ''
    targetDir = ''
    scale = ''
    try:
      opts, args = getopt.getopt(argv,"hi:o:s:",["idir=","odir=","scale="])
    except getopt.GetoptError:
      print 'generateWatDivBasicQueries.py -i <sourceDir> -o <targetDir> -s <WatDivScale>'
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print 'test.py -i <sourceDir> -o <targetDir>'
         sys.exit()
      elif opt in ("-i", "--idir"):
         sourceDir = arg
      elif opt in ("-o", "--odir"):
         targetDir = arg
      elif opt in ("-s", "--scale"):
         scale = arg
    if (len(sourceDir) == 0):
        print 'generateWatDivBasicQueries.py -i <sourceDir> -o <targetDir> -s <WatDivScale>'
        sys.exit()
    if (len(targetDir) == 0):
        targetDir = "./";
    if (len(scale) == 0):
        print 'generateWatDivBasicQueries.py -i <sourceDir> -o <targetDir> -s <WatDivScale>'
        sys.exit()
    print 'Input Dir is "', sourceDir
    print 'Output Dir is "', targetDir
    print 'WatDivScale is "', scale
    global templatesDir, scaleFactor, parametersDir
    templatesDir = sourceDir + templatesDir;
    scaleFactor = scale
    parametersDir = sourceDir + parametersDir + scaleFactor;
    
    # read pathes of all query templates 
    sparqlList = extractTemplates(templatesDir);
    
    # recreate directories for result queries
    subprocess.call("rm -f -r  "+targetDir+"/"+scaleFactor, shell=True)
    subprocess.call("mkdir  "+targetDir+"/"+scaleFactor, shell=True)
    
    # for every template generate queries
    for fileName in sparqlList:
        handleQueryTemplate(templatesDir+"/"+fileName, targetDir+"/"+scaleFactor)

if __name__ == "__main__":
   main(sys.argv[1:])


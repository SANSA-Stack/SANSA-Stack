#!/usr/bin/env python
#
# Copyright Simon Skilevic
# Master Thesis for Chair of Databases and Information Systems
# Uni Freiburg
#
import sys, getopt, subprocess
# -------------------------------------------------------------------------------
# Description

# This script generates Mix/Sel/Inc SPARQL queries.
# It becomes as input a directory containing templates with one placeholder each
# and corresponding files with parameters, as well as WatDiv dataset scale.
# The script generates than for every suitable
# combination query-template<->parameter one query
# Necessary directory structure ->

# Templates:
# [inputDir]/templates/[templateFile]

# Parameters:
# [inputDir]/parameters/Scale[WatDivScale]/[parameterFile]
# The corresponding parameter-file is defined by posfix of the template-file name
# Example: parameter-file ML1.txt corresponds to template-files ML5-1.txt,
# ML6-1.txt, ML7-1.txt ...

# Output format:
# [inputDir]/[WatDivScale]/[templateName]__[testCase1]_[testCase2].in

# -------------------------------------------------------------------------------
# Settings

templatesDir = "/templates/"
parametersDir = "/parameters/Scale"
# -------------------------------------------------------------------------------
# watDivScaleFactor
scaleFactor = ""

# prefixes
startString = ('PREFIX gn: <http://www.geonames.org/ontology#>\n'
            +'PREFIX gr: <http://purl.org/goodrelations/>\n'
            +'PREFIX mo: <http://purl.org/ontology/mo/>\n'
            +'PREFIX og: <http://ogp.me/ns#>\n'
            +'PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n'
            +'PREFIX sorg: <http://schema.org/>\n'
            +'PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>\n'
            +'PREFIX rev: <http://purl.org/stuff/rev#>\n'
            +'PREFIX foaf: <http://xmlns.com/foaf/>\n'
            +'PREFIX dc: <http://purl.org/dc/terms/>\n');

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
    else:
        begin = begin + 1
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
    global scaleFactor, startString, parametersDir
    if not "~" in fileName:
        baseName = extractBaseName(fileName);	
        query = readFileToString(fileName).replace("\r", "");
        if "%v0%" in query:            
            if "wsdbm:User" in query[:query.find("\n")]:
                #create queries with parameter file 1 (wsdbm:User)
                substitutions = readFileToString(parametersDir+"/"+baseName[:2]+"-1.txt").replace("\r", "").split("\n");
                query = startString + query
                fileId = 1;
                for case in substitutions:
                    if len(case)>0:
                        newQuery = query.replace("%v0%", case)
                        writeStringToFile(targetDir+"/"+baseName+"-U-"+str(fileId)+"__VP_SO-OS-SS-VP.in", newQuery)
                        fileId = fileId + 1
            else:
                #create queries with parameter file 2 (wsdbm:Reailer)
                substitutions = readFileToString(parametersDir+"/"+baseName[:2]+"-2.txt").replace("\r", "").split("\n");
                query = startString + query
                fileId = 1;
                for case in substitutions:
                    if len(case)>0:
                        newQuery = query.replace("%v0%", case);
                        writeStringToFile(targetDir+"/"+baseName+"-R-"+str(fileId)+"__VP_SO-OS-SS-VP.in", newQuery)
                        fileId = fileId + 1
        else:
            query = startString + query
            writeStringToFile(targetDir+"/"+baseName+"__VP_SO-OS-SS-VP.in", query)

def main(argv):
    sourceDir = ''
    targetDir = ''
    scale = ''
    try:
      opts, args = getopt.getopt(argv,"hi:o:s:",["idir=","odir=","scale="])
    except getopt.GetoptError:
      print 'generateWatDivPathQueries.py -i <sourceDir> -o <targetDir> -s <WatDivScale>'
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
        print 'generateWatDivPathQueries.py -i <sourceDir> -o <targetDir> -s <WatDivScale>'
        sys.exit()
    if (len(targetDir) == 0):
        targetDir = "./";
    if (len(scale) == 0):
        print 'generateWatDivPathQueries.py -i <sourceDir> -o <targetDir> -s <WatDivScale>'
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


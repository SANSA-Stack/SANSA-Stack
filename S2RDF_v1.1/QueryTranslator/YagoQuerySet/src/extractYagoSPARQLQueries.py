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
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    return onlyfiles


def extractBaseName(str):
    end = str.rfind(".")
    begin = str.rfind("/")
    if (begin < 0):
        begin = 0;
    return str[begin:-(len(str) - end)]


def readFileToString(fileName):
    with open(fileName) as myFile:
        return myFile.read()


def writeStringToFile(fileName, str):
    with open(fileName, "w") as myFile:
        myFile.write(str)

    # for every parameter in parameter-file create a query


# by replacing of placeholder %v1% with this parameter
# if template-file contains no placeholder than save it
# once as result sparql-query
def handleQueryTemplate(fileName, targetDir):
    if not "~" in fileName:
        queryList = readFileToString(fileName).replace("\r", "");
        queryBatches = queryList.split("}")

        for query in queryBatches:
            if len(query) > 0:
                print("XXXX-->" + query)
                buf = query.split("###")
                baseName = buf[0][buf[0].find("Q"):buf[0].find(":")]
                print("XXbaseNameXX-->" + baseName+"<")
                if buf[1][0]=='\n':
                    query = buf[1][1:]
                else:
                    query = buf[1]
		for i in range(1, 6):
                	writeStringToFile(targetDir + "/" + baseName + "-" + str(i) + "__VP_SO-OS-SS-VP.in", query + "}")


def main(argv):
    sourceFile = ''
    targetDir = ''
    scale = ''
    try:
        opts, args = getopt.getopt(argv, "hi:o:s:", ["idir=", "odir=", "scale="])
    except getopt.GetoptError:
        print('generateWatDivBasicQueries.py -i <sourceDir> -o <targetDir>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('test.py -i <sourceDir> -o <targetDir>')
            sys.exit()
        elif opt in ("-i", "--iFile"):
            sourceFile = arg
        elif opt in ("-o", "--odir"):
            targetDir = arg
        else:
            print('test.py -i <sourceDir> -o <targetDir>')
    if len(sourceFile) == 0:
        print('generateWatDivBasicQueries.py -i <sourceDir> -o <targetDir>')
        sys.exit()
    if len(targetDir) == 0:
        targetDir = "./"
    print('Input File is "', sourceFile)
    print('Output Dir is "', targetDir)
    global parametersDir
    parametersDir = sourceFile + parametersDir + scaleFactor

    # recreate directories for result queries
    subprocess.call("rm -f -r  " + targetDir + "/" + scaleFactor, shell=True)
    subprocess.call("mkdir  " + targetDir + "/" + scaleFactor, shell=True)

    # for every template generate queries
    handleQueryTemplate(sourceFile, targetDir + "/")


if __name__ == "__main__":
    main(sys.argv[1:])

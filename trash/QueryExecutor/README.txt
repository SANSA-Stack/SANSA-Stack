DESCRIPTION:
QueryExecutor executes composite query file containing list of SQL queries with table usage instructions. These file are created by QueryTranslator using translateWatDivQueries.py script. The queries are listen in the file using following format:
>>>>>>[query name]--[test case name]
[SQL query]
++++++Tables Statistic
[table usage instructions]

EXAMPLE QUERY FILE:
>>>>>>IL6-2-R-9--VP__WatDiv1M
SELECT tab0.v1 AS v1 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2
 FROM    (SELECT obj AS v1
     FROM gr__offers$$1$$
     WHERE sub = 'wsdbm:Retailer23'
    ) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2
     FROM gr__includes$$2$$
    ) tab1
 ON(tab0.v1=tab1.v1)


++++++Tables Statistic
gr__offers$$1$$    0    VP    gr__offers/
    VP    <gr__offers>    14179
------
gr__includes$$2$$    0    VP    gr__includes/
    VP    <gr__includes>    9000
------

S2RDF QueryExecutor is an sbt-scala project. You need to install sbt to compile the project. You can also compile the project on your own way. All what you need is runnable jar file, which is then committed to Spark-Cluster using QueryExecutor.py script. This script contains also several settings to deploy Spark-claster.

For further description see comments at the top of Python-scripts.

INSTALLATION:
cd S2RDF_QueryExecutor
sbt package

EXECUTION:
python QueryExecutor.py -d <databaseDirectory> -q <querieListFile>

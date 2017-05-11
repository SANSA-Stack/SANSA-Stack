# SANSA-Notebooks
Interactive Spark Notebooks for running SANSA examples.
In this repository you will find a [docker-compose.yml](./docker-compose.yml) for running Hadoop/Spark cluster locally.
The cluster also includes [Hue](http://gethue.com/) for navigation and copying file to HDFS.
The notebooks are created and run using [Apache Zeppelin](https://zeppelin.apache.org/).

# Getting started
Get the SANSA Examples jar file (requires ```wget```):
```
make
```
Start the cluster:
```
make up
```
When start-up is done you will be able to access the following interfaces:
* http://localhost:8080/ (Spark Master)
* http://localhost:8088/ (Hue HDFS Filebrowser)
* http://localhost/ (Zeppelin)
Go on and open [Zeppelin](http://localhost), choose any available notebook and try to execute it.

![Apache Zeppelin OWL](./docs/images/Zeppelin_OWL_Screenshot.png "Apache Zeppelin Running OWL Examples")

# Executing Examples From Command Line
It is also possible to execute the applications from the command line. Get SANSA-Examples jar and start the cluster if you already have not done it:
```
make
make up
```
Then you can execute any of the following commands to run the examples from the command line:
```
make cli-triples-reader
make cli-triple-ops
make cli-triples-writer
make cli-pagerank
make cli-inferencing
make cli-sparklify
make cli-owl-reader-manchester
make cli-owl-reader-functional
make cli-owl-dataset-reader-manchester
make cli-owl-dataset-reader-functional
make cli-clustering
make cli-rule-mining
```

# Notes
* The instructions from this repo were tested on Ubuntu 16.04 with Docker engine 17.03.
Maintained by [Ivan Ermilov](https://github.com/earthquakesan/)

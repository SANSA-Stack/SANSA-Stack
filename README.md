# SANSA-Notebooks
Interactive Spark Notebooks for running [SANSA-Examples](https://github.com/SANSA-Stack/SANSA-Examples).
In this repository you will find a [docker-compose.yml](./docker-compose.yml) for running Hadoop/Spark cluster locally.
The cluster also includes [Hue](http://gethue.com/) for navigation and copying file to HDFS.
The notebooks are created and run using [Apache Zeppelin](https://zeppelin.apache.org/).

# Requirements
* Docker Engine >= 1.13.0
* docker-compose >= 1.10.0
* Around 10 GB of disk space for Docker images

After installation of docker add yourself to docker group (%username% is your username) and relogin:
```
sudo usermod -aG docker %username%
```
This allows to run docker commands without sudo prefix (necessary for running make targets).

# Getting started
Get the SANSA Examples jar file (requires ```wget```):
```
make
```
Start the cluster (this will lead to downloading BDE docker images, will take a while):
```
make up
```
When start-up is done you will be able to access the following interfaces:
* http://localhost:8080/ (Spark Master)
* http://localhost:8088/ (Hue HDFS Filebrowser)
* http://localhost/ (Zeppelin)
To load the data to your cluster simply do:
```
make load-data
```
Go on and open [Zeppelin](http://localhost), choose any available notebook and try to execute it.

![Apache Zeppelin OWL](./docs/images/Zeppelin_OWL_Screenshot.png "Apache Zeppelin Running OWL Examples")

To restart Zeppelin without restarting the whole stack:
```
make restart
```

# Executing Examples From Command Line
It is also possible to execute the applications from the command line. Get SANSA-Examples jar and start the cluster if you already have not done it:
```
make
make up
make load-data
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
* Maintained by [Ivan Ermilov](https://github.com/earthquakesan/)

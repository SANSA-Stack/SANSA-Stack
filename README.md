# SANSA-Stack
<!-- [![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-parent/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-parent) -->
[![Build Status](https://github.com/SANSA-Stack/SANSA-Stack/workflows/CI/badge.svg)](https://github.com/SANSA-Stack/SANSA-Stack/actions?query=workflow%3ACI)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Twitter](https://img.shields.io/twitter/follow/SANSA_Stack.svg?style=social)](https://twitter.com/SANSA_Stack)

This project comprises the whole Semantic Analytics Stack (SANSA). At a glance, it features the following functionality:

* Ingesting RDF and OWL data in various formats into RDDs
* Operators for working with RDDs and data frames of RDF data at various levels (triples, bindings, graphs, etc)
* *Transformation* of RDDs to data frames and *partitioning* of RDDs into R2RML-mapped data frames
* Distributed SPARQL querying over R2RML-mapped data frame partitions using RDB2RDF engines (Sparqlify & Ontop)
* Enrichment of RDDs with inferences
* Application of machine learning algorithms

For a detailed description of SANSA, please visit http://sansa-stack.net. 

## Layers
The SANSA project is structured in the following five layers developed in their respective sub-folders:

* [RDF](sansa-rdf)
* [OWL](sansa-owl)
* [Query](sansa-query)
* [Inference](sansa-inference)
* [ML](sansa-ml)

## Release Cycle
A SANSA stack release is done every six months and consists of the latest stable versions of each layer at this point. This repository is used for organising those joint releases.

## Usage

### Spark

#### Requirements

We currently require a Spark 3.x.x with Scala 2.12 setup. A Spark 2.x version can be built from source based on the [spark2](https://github.com/SANSA-Stack/SANSA-Stack/tree/spark2) branch.

#### Release Version
Some of our dependencies are not in Maven central (yet), so you need to add following Maven repository to your project POM file `repositories` section:
```xml
<repository>
   <id>maven.aksw.internal</id>
   <name>AKSW Release Repository</name>
   <url>http://maven.aksw.org/archiva/repository/internal</url>
   <releases>
      <enabled>true</enabled>
   </releases>
   <snapshots>
      <enabled>false</enabled>
   </snapshots>
</repository>
```

If you want to import the full SANSA Stack, please add the following Maven dependency to your project POM file:
```xml
<!-- SANSA Stack -->
<dependency>
   <groupId>net.sansa-stack</groupId>
   <artifactId>sansa-stack-spark_2.12</artifactId>
   <version>$LATEST_RELEASE_VERSION$</version>
</dependency>
```
If you only want to use particular layers, just replace `$LAYER_NAME$` with the corresponding name of the layer
```xml
<!-- SANSA $LAYER_NAME$ layer -->
<dependency>
   <groupId>net.sansa-stack</groupId>
   <artifactId>sansa-$LAYER_NAME$-spark_2.12</artifactId>
   <version>$LATEST_RELEASE_VERSION$</version>
</dependency>
```

#### SNAPSHOT Version
While the release versions are available on Maven Central, latest SNAPSHOT versions have to be installed from source code:
```bash
git clone https://github.com/SANSA-Stack/SANSA-Stack.git
cd SANSA-Stack
```
Then to build and install the full SANSA Spark stack you can do
```bash
./dev/mvn_install_stack_spark.sh 
```
or for a single layer `$LAYER_NAME$` you can do
```bash
mvn -am -DskipTests -pl :sansa-$LAYER_NAME$-spark_2.12 clean install 
```

Alternatively, you can use the following Maven repository and add it to your project POM file `repositories` section:
```xml
<repository>
   <id>maven.aksw.snapshots</id>
   <name>AKSW Snapshot Repository</name>
   <url>http://maven.aksw.org/archiva/repository/snapshots</url>
   <releases>
      <enabled>false</enabled>
   </releases>
   <snapshots>
      <enabled>true</enabled>
   </snapshots>
</repository>
```
Then do the same as for the release version and add the dependency:
```xml
<!-- SANSA Stack -->
<dependency>
   <groupId>net.sansa-stack</groupId>
   <artifactId>sansa-stack-spark_2.12</artifactId>
   <version>$LATEST_SNAPSHOT_VERSION$</version>
</dependency>
```

## How to Contribute
We always welcome new contributors to the project! Please see [our contribution guide](http://sansa-stack.net/contributing-to-sansa/) for more details on how to get started contributing to SANSA.

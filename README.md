# SANSA RDF
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-rdf-parent_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-rdf-parent_2.11)
[![Build Status](https://ci.aksw.org/jenkins/job/SANSA%20RDF/job/develop/badge/icon)](https://ci.aksw.org/jenkins/job/SANSA%20RDF/job/develop/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Twitter](https://img.shields.io/twitter/follow/SANSA_Stack.svg?style=social)](https://twitter.com/SANSA_Stack)

## Description
SANSA RDF is a library to read RDF files into [Spark](https://spark.apache.org) or [Flink](https://flink.apache.org). It allows files to reside in HDFS as well as in a local file system and distributes them across Spark RDDs/Datasets or Flink DataSets.


SANSA uses the RDF data model for representing graphs consisting of triples with subject, predicate and object. RDF datasets may contains multiple RDF graphs and record information about each graph, allowing any of the upper layers of sansa (Querying and ML) to make queries that involve information from more than one graph. Instead of directly dealing with RDF datasets, the target RDF datasets need to be converted into an RDD/DataSets of triples. We name such an RDD/DataSets a main dataset. The main dataset is based on an RDD/DataSets data structure, which is a basic building block of the Spark/Flink framework. RDDs/DataSets are in-memory collections of records that can be operated on in parallel on large clusters.

## Usage

The following Scala code shows how to read an RDF file (be it a local file or a file residing in HDFS) into a Spark RDD:
```scala

import net.sansa_stack.rdf.spark.io.NtripleReader

val input = sc.textFile("hdfs://...")

val triplesRDD = NTripleReader.load(sparkSession, new File(input))

triplesRDD.take(5).foreach(println(_))
```
An overview is given in the [FAQ section of the SANSA project page](http://sansa-stack.net/faq/#rdf-processing). Further documentation about the builder objects can also be found on the [ScalaDoc page](http://sansa-stack.net/scaladocs/).

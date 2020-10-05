# SANSA RDF
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-rdf-parent_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-rdf-parent_2.11)
[![Build Status](https://ci.aksw.org/jenkins/job/SANSA%20RDF/job/develop/badge/icon)](https://ci.aksw.org/jenkins/job/SANSA%20RDF/job/develop/)
[![Coverage Status](https://coveralls.io/repos/github/SANSA-Stack/SANSA-RDF/badge.svg?branch=develop)](https://coveralls.io/github/SANSA-Stack/SANSA-RDF?branch=develop)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Twitter](https://img.shields.io/twitter/follow/SANSA_Stack.svg?style=social)](https://twitter.com/SANSA_Stack)

## Description
SANSA RDF is a library to read RDF files into [Spark](https://spark.apache.org) or [Flink](https://flink.apache.org). It allows files to reside in HDFS as well as in a local file system and distributes them across Spark RDDs/Datasets or Flink DataSets.


SANSA uses the RDF data model for representing graphs consisting of triples with subject, predicate and object. RDF datasets may contains multiple RDF graphs and record information about each graph, allowing any of the upper layers of sansa (Querying and ML) to make queries that involve information from more than one graph. Instead of directly dealing with RDF datasets, the target RDF datasets need to be converted into an RDD/DataSets of triples. We name such an RDD/DataSets a main dataset. The main dataset is based on an RDD/DataSets data structure, which is a basic building block of the Spark/Flink framework. RDDs/DataSets are in-memory collections of records that can be operated on in parallel on large clusters.

## Usage

We suggest to import the `net.sansa_stack.rdf.spark.io` package which adds the function `rdf()` to a Spark session. You can either explicitely specify the type of RDF serialization or let the API guess the format based on the file extension. 

For example, the following Scala code shows how to read an RDF file in N-Triples syntax (be it a local file or a file residing in HDFS) into a Spark RDD:
```scala
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang

val spark: SparkSession = ...

val lang = Lang.NTRIPLES
val triples = spark.rdf(lang)(path)

triples.take(5).foreach(println(_))
```

## Input
We basically support reading most (if not all) of the common RDF formats due to the Apache Jena being our core parser backend. Note, some of the formats can be easily read from distributed data, i.e. multiple file splits can be processed in parallel which ideally results in a much higher loading performance. This holds especially for line based formats like N-Triples and N-Quads, but we also do provide an (experimental) Trig parser which works on file splits distributed among the cluster nodes.

### Triple Formats

| RDF Serialisation  | Line Based  | Batch Based  | File Extension(s) |
|--------------------|-------------|--------------|-------------------|
| N-Triples           | Yes         | Yes          | nt                |
| Turtle             | No          | No           | ttl               |
| RDF/XML            | No          | No           | rdf, xml          |
| RDF/JSON           | No          | No           | rj                |

### Quad Formats

| RDF Serialisation  | Line Based  | Batch Based  | File Extension(s) |
|--------------------|-------------|--------------|------------|
| N-Quads             | Yes         | Yes          | nq        |
| TriG               | No          | No           | trig        |
| TriX               | No          | No           | trix        |

### Triple/Quad Formats

| RDF Serialisation  | Line Based  | Batch Based  | File Extension(s) |
|--------------------|-------------|--------------|------------|
| JSON-LD            | No          | No           | jsonld        |
| RDF Thrift         | No          | No           | rt, trdf        |

### Loading options
Loading from a path can be a file or directory in results into an RDD[Triple].
The `path` argument can also consist of multiple paths
and even wildcards, e.g.
`"/my/dir1,/my/paths/part-00[0-5],/another/dir,/a/specific/file"`

#### Handling of errors for N-Triples and N-Quads Format

By default, loading from N-Triples or N-Quads format stops once a parse error occurs, i.e. a `org.apache.jena.net.sansa_stack.rdf.spark.riot.RiotException` will be thrown by the underlying parser.

The following options exist:
- `STOP` the whole data loading process will be stopped and a `org.apache.jena.net.sansa_stack.rdf.spark.riot.RiotException` will be thrown
- `SKIP` the line will be skipped but the data loading process will continue, an error message will be logged

#### Handling of warnings

If the additional checking of RDF terms is enabled, warnings during parsing can occur. For example,
a wrong lexical form of a literal w.r.t. to its datatype will lead to a warning.

The following can be done with those warnings:
- `IGNORE` the warning will just be logged to the configured logger
- `STOP` similar to the error handling mode, the whole data loading process will be stopped and a
`org.apache.jena.net.sansa_stack.rdf.spark.riot.RiotException` will be thrown
- `SKIP` similar to the error handling mode, the line will be skipped but the data loading process will continue. 
In additon, an error message will be logged


#### Checking of RDF terms
Set whether to perform checking of NTriples - defaults to no checking.

Checking adds warnings over and above basic syntax errors.
This can also be used to turn warnings into exceptions if the option `stopOnWarnings` is set to STOP or SKIP.

- IRIs - whether IRIs confirm to all the rules of the IRI scheme
- Literals: whether the lexical form conforms to the rules for the datatype.
- Triples: check slots have a valid kind of RDF term (parsers usually make this a syntax error anyway).


See also the optional `errorLog` argument to control the output. The default is to log.

---
An overview is given in the [FAQ section of the SANSA project page](http://sansa-stack.net/faq/#rdf-processing). 
Further documentation about the builder objects can also be found on the [ScalaDoc page](http://sansa-stack.net/scaladocs/).

## How to Contribute
We always welcome new contributors to the project! Please see [our contribution guide](http://sansa-stack.net/contributing-to-sansa/) for more details on how to get started contributing to SANSA.


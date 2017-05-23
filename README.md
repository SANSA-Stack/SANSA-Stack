# SANSA Inference Layer
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-inference-parent_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-inference-parent_2.11)
[![Build Status](https://ci.aksw.org/jenkins/job/SANSA%20Inference%20Layer/job/develop/badge/icon)](https://ci.aksw.org/jenkins/job/SANSA%20Inference%20Layer/job/develop/)

## Structure
### sansa-inference-common
* common datastructures
* rule dependency analysis 

### sansa-inference-spark
Contains the core Inference API based on Apache Spark.

### sansa-inference-flink
Contains the core Inference API based on Apache Flink.

### sansa-inference-tests
Contains common test classes and data.


## Setup
### Prerequisites
* Maven 3.x
* Java 8
* Scala 2.11 (support for Scala 2.10 is planned)
* Apache Spark 2.x
* Apache Flink 1.x

### From source

To install the SANSA Inference API, you need to download it via Git and install it via Maven.
```shell
git clone https://github.com/SANSA-Stack/SANSA-Inference.git
cd SANSA-Inference
mvn clean install
```
Afterwards, you have to add the dependency to your pom.xml

For Apache Spark
```xml
<dependency>
  <groupId>net.sansa-stack</groupId>
  <artifactId>sansa-inference-spark_2.11</artifactId>
  <version>VERSION</version>
</dependency>
```
and for Apache Flink
```xml
<dependency>
  <groupId>net.sansa-stack</groupId>
  <artifactId>sansa-inference-flink_2.11</artifactId>
  <version>VERSION</version>
</dependency>
```
with `VERSION` beeing the released version you want to use.

### Using Maven pre-build artifacts

 1. Add AKSW Maven repository to your pom.xml (will be added to Maven Central soon)
```xml
<repository>
  <id>maven.aksw.snapshots</id>
  <name>University Leipzig, AKSW Maven2 Repository</name>
  <url>http://maven.aksw.org/archiva/repository/snapshots</url>
  <releases>
  <releases>
	<enabled>false</enabled>
  </releases>
  <snapshots>
	<enabled>true</enabled>
  </snapshots>
</repository>

<repository>
  <id>maven.aksw.internal</id>
  <name>University Leipzig, AKSW Maven2 Internal Repository</name>
  <url>http://maven.aksw.org/archiva/repository/internal</url>
  <releases>
	<enabled>true</enabled>
  </releases>
  <snapshots>
	<enabled>false</enabled>
  </snapshots>
</repository>
```
2\. Add dependency to your pom.xml

For Apache Spark
```xml
<dependency>
  <groupId>net.sansa-stack</groupId>
  <artifactId>sansa-inference-spark_2.11</artifactId>
  <version>VERSION</version>
</dependency>
```
and for Apache Flink
```xml
<dependency>
  <groupId>net.sansa-stack</groupId>
  <artifactId>sansa-inference-flink_2.11</artifactId>
  <version>VERSION</version>
</dependency>
```
with `VERSION` beeing the released version you want to use.
### Using SBT
SANSA Inference API has not been published on Maven Central yet, thus, you have to add an additional repository as follows
```scala
resolvers ++= Seq(
  "AKSW Maven Releases" at "http://maven.aksw.org/archiva/repository/internal",
  "AKSW Maven Snapshots" at "http://maven.aksw.org/archiva/repository/snapshots"
)
```
Then you have to add a dependency on either the Apache Spark or the Apache Flink module.

For Apache Spark add
```scala
"net.sansa-stack" % "sansa-inference-spark_2.11" % VERSION
```

and for Apache Flink add
```scala
"net.sansa-stack" % "sansa-inference-flink_2.11" % VERSION
```

where `VERSION` is the released version you want to use.

## Usage
Besides using the Inference API in your application code, we also provide a command line interface with various options that allow for a convenient way to use the core reasoning algorithms:
```
RDFGraphMaterializer 0.1.0
Usage: RDFGraphMaterializer [options]

  -i, --input <path1>,<path2>,...
                           path to file or directory that contains the input files (in N-Triples format)
  -o, --out <directory>    the output directory
  --properties <property1>,<property2>,...
                           list of properties for which the transitive closure will be computed (used only for profile 'transitive')
  -p, --profile {rdfs | rdfs-simple | owl-horst | transitive}
                           the reasoning profile
  --single-file            write the output to a single file in the output directory
  --sorted                 sorted output of the triples (per file)
  --parallelism <value>    the degree of parallelism, i.e. the number of Spark partitions used in the Spark operations
  --help                   prints this usage text
```
This can easily be used when submitting the Job to Spark (resp. Flink), e.g. for Spark

```bash
/PATH/TO/SPARK/sbin/spark-submit [spark-options] /PATH/TO/INFERENCE-SPARK-DISTRIBUTION/FILE.jar [inference-api-arguments]
```

and for Flink

```bash
/PATH/TO/FLINK/bin/flink run [flink-options] /PATH/TO/INFERENCE-FLINK-DISTRIBUTION/FILE.jar [inference-api-arguments]
```

In addition, we also provide Shell scripts that wrap the Spark (resp. Flink) deployment and can be used by first
setting the environment variable `SPARK_HOME` (resp. `FLINK_HOME`) and then calling
```bash
/PATH/TO/INFERENCE-DISTRIBUTION/bin/cli [inference-api-arguments]
```
(Note, that setting Spark (resp. Flink) options isn't supported here and has to be done via the corresponding config files)

### Example

```bash
RDFGraphMaterializer -i /PATH/TO/FILE/test.nt -o /PATH/TO/TEST_OUTPUT_DIRECTORY/ -p rdfs
```
will compute the RDFS materialization on the data contained in `test.nt` and write the inferred RDF graph to the given directory `TEST_OUTPUT_DIRECTORY`.

## Supported Reasoning Profiles

Currently, the following reasoning profiles are supported:

##### RDFS

###### RDFS Simple

A fragment of RDFS that covers the most relevant vocabulary, prove that it
preserves the original RDFS semantics, and avoids vocabulary and axiomatic
information that only serves to reason about the structure of the language
itself and not about the data it describes.
It is composed of the reserved vocabulary
`rdfs:subClassOf`, `rdfs:subPropertyOf`, `rdf:type`, `rdfs:domain` and `rdfs:range`.

More details can be found in

Sergio Muñoz, Jorge Pérez, Claudio Gutierrez:
    *Simple and Efficient Minimal RDFS.* J. Web Sem. 7(3): 220-234 (2009)
##### OWL Horst
OWL Horst is a fragment of OWL and was proposed by Herman ter Horst [1] defining an "intentional" version of OWL sometimes also referred to as pD\*. It can be materialized using a set of rules that is an extension of the set of RDFS rules. OWL Horst is supposed to be one of the most common OWL flavours for scalable OWL reasoning while bridging the gap between the unfeasible OWL Full and the low expressiveness of RDFS.

[1] Herman J. ter Horst:
*Completeness, decidability and complexity of entailment for RDF Schema and a semantic extension involving the OWL vocabulary.* J. Web Sem. 3(2-3): 79-115 (2005)

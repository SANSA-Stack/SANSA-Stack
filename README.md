# SANSA Inference Layer
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
  <artifactId>sansa-inference-spark</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```
For Apache Flink
```xml
<dependency>
  <groupId>net.sansa-stack</groupId>
  <artifactId>sansa-inference-flink</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```
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
2. Add dependency to your pom.xml

For Apache Spark
```xml
<dependency>
  <groupId>net.sansa-stack</groupId>
  <artifactId>sansa-inference-spark</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```
For Apache Flink
```xml
<dependency>
  <groupId>net.sansa-stack</groupId>
  <artifactId>sansa-inference-flink</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```
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
"net.sansa-stack" % "sansa-inference-spark" % VERSION
```

and for Apache Flink add
```scala
"net.sansa-stack" % "sansa-inference-flink" % VERSION
```
where, `VERSION` is the released version you want to use of the Finatra framework

## Usage
```
RDFGraphMaterializer 0.1.0
Usage: RDFGraphMaterializer [options]
 
 
  -i <file> | --input <file>
        the input file in N-Triple format
  -o <directory> | --out <directory>
        the output directory
  --single-file
        write the output to a single file in the output directory
  --sorted
        sorted output of the triples (per file)
  -p {rdfs | owl-horst} | --profile {rdfs | owl-horst}
        the reasoning profile
  --help
        prints this usage text
```
### Example

`RDFGraphMaterializer -i /PATH/TO/FILE/test.nt -o /PATH/TO/TEST_OUTPUT_DIRECTORY/ -p rdfs` will compute the RDFS materialization on the data contained in `test.nt` and write the inferred RDF graph to the given directory `TEST_OUTPUT_DIRECTORY`.

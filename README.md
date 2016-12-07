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

### From source

To install the SANSA Inference API, you need to download it via Git and install it via Maven.

git clone https://github.com/AKSW/SANSA-Inference.git
cd SNASA-Inference
mvn clean install
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

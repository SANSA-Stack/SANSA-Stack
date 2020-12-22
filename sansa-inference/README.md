

# SANSA Inference Layer
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Twitter](https://img.shields.io/twitter/follow/SANSA_Stack.svg?style=social)](https://twitter.com/SANSA_Stack)

**Table of Contents**

- [SANSA Inference Layer](#)
	- [Structure](#structure)
		- [sansa-inference-common](#sansa-inference-common)
		- [sansa-inference-spark](#sansa-inference-spark)
		- [sansa-inference-flink](#sansa-inference-flink)
		- [sansa-inference-tests](#sansa-inference-tests)
	- [Setup](#setup)
	- [Usage](#usage)
		- [From Code](#from-code)
		- [From CLI](#from-cli)
		- [Example](#example)
	- [Supported Reasoning Profiles](#supported-reasoning-profiles)
		- [RDFS](#rdfs)
		- [RDFS Simple](#rdfs-simple)
		- [OWL Horst](#owl-horst)
  	- [How to Contribute](#how-to-contribute)


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

See how to use latest release or snapshot version of the Inference Layer [here](https://github.com/SANSA-Stack/SANSA-Stack).

Long story short, just add the Maven dependency for either release or snapshot version (note, for the snapshot version either build from source or add the AKSW Maven repository)
```xml
<dependency>
   <groupId>net.sansa-stack</groupId>
   <artifactId>sansa-inference-spark_2.12</artifactId>
   <version>$VERSION$</version>
</dependency>
```

## Usage

### From code


```scala
...
// some of the used imports here
import org.apache.jena.riot.Lang
import org.apache.jena.graph.Triple

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import net.sansa_stack.inference.spark.forwardchaining.triples.ForwardRuleReasonerRDFS
import net.sansa_stack.rdf.spark.io._import org.apache.spark.
...

val spark: SparkSession = // SparkSession here

val input: String = // path to input

// load triples from disk
val triples: RDD[Triple] = spark.rdf(Lang.NTRIPLES)(input)

// create RDFS reasoner
val reasoner = new ForwardRuleReasonerRDFS(spark)
    
// compute inferred triples
// Note: they do also contain the asserted triples, if you need just the inferred triples you have to compute the diff)
val inferredTriples: RDD[Triple] = reasoner.apply(triples)

// write triples to disk in N-Triples format
inferredTriples.saveAsNTriplesFile(output.toString)
```


### From CLI
Besides using the Inference API in your application code, we provide a single application with various options that allow for a convenient way to use the core reasoning algorithms:
```
RDFGraphMaterializer 0.7.1
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
The RDFS reasoner can be configured to work at two different compliance levels: 

###### RDFS (Default)
This implements all of the [RDFS closure rules](https://www.w3.org/TR/rdf11-mt/#patterns-of-rdfs-entailment-informative) with the exception of bNode entailments and datatypes (**rdfD 1**). RDFS axiomatic triples are also omitted. This is an expensive mode because all statements in the data graph need to be checked for possible use of container membership properties. It also generates type assertions for all resources and properties mentioned in the data (**rdf1**, **rdfs4a**, **rdfs4b**).

###### RDFS Simple

A fragment of RDFS that covers the most relevant vocabulary, prove that it
preserves the original RDFS semantics, and avoids vocabulary and axiomatic
information that only serves to reason about the structure of the language
itself and not about the data it describes.
It is composed of the reserved vocabulary
`rdfs:subClassOf`, `rdfs:subPropertyOf`, `rdf:type`, `rdfs:domain` and `rdfs:range`.
This implements just the transitive closure of `rdfs:subClassOf` and `rdfs:subPropertyOf` relations, the `rdfs:domain` and `rdfs:range` entailments and the implications of `rdfs:subPropertyOf` and `rdfs:subClassOf` in combination with instance data. It omits all of the axiomatic triples. This is probably the most useful mode but it is a less complete implementation of the standard. 

More details can be found in

Sergio Muñoz, Jorge Pérez, Claudio Gutierrez:
    *Simple and Efficient Minimal RDFS.* J. Web Sem. 7(3): 220-234 (2009)
##### OWL Horst
OWL Horst is a fragment of OWL and was proposed by Herman ter Horst [1] defining an "intentional" version of OWL sometimes also referred to as pD\*. It can be materialized using a set of rules that is an extension of the set of RDFS rules. OWL Horst is supposed to be one of the most common OWL flavours for scalable OWL reasoning while bridging the gap between the unfeasible OWL Full and the low expressiveness of RDFS.

[1] Herman J. ter Horst:
*Completeness, decidability and complexity of entailment for RDF Schema and a semantic extension involving the OWL vocabulary.* J. Web Sem. 3(2-3): 79-115 (2005)

## How to Contribute
We always welcome new contributors to the project! Please see [our contribution guide](http://sansa-stack.net/contributing-to-sansa/) for more details on how to get started contributing to SANSA.


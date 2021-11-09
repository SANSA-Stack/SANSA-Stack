---
parent: RDF
title: Loading RDF
nav_order: 1
---

# Loading RDF


## Scala

We suggest to import the `net.sansa_stack.rdf.spark.io` package which adds the function `rdf()` to a Spark session. You can either explicitely specify the type of RDF serialization or let the API guess the format based on the file extension.

For example, the following Scala code shows how to read an RDF file in N-Triples syntax (be it a local file or a file residing in HDFS) into a Spark RDD:


### Load as RDD

<details open>
  <summary markdown="span">Scala</summary>

```scala
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang

val spark: SparkSession = ...

val lang = Lang.NTRIPLES
val triples = spark.rdf(lang)(path)

triples.take(5).foreach(println(_))
```

</details>

### Load as DataFrame

<details open>
  <summary markdown="span">Scala</summary>

```scala
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang

val spark: SparkSession = ...

val lang = Lang.NTRIPLES
val triples = spark.read.rdf(lang)(path)

triples.take(5).foreach(println(_))
```

</details>


## Java

The main class for loading RDDs with Java is  `net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactoryImpl`:

<details open>
  <summary markdown="span">Java</summary>

```java
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;

SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

RdfSourceFactory rdfSourceFactory = RdfSourceFactoryImpl.from(sparkSession);

RdfSource rdfSource = rdfSourceFactory.get("path");

RDD<Triple> rddOfTrple = rdfSource.asTriples();
RDD<Quad> rddOfQuad = rdfSource.asQuads();
RDD<Model> rddOfModel = rdfSource.asModels();
RDD<Dataset> rddOfDataset = rdfSource.asDatasets();

```

</details>

Note, that a `JavaSparkContext` is not necessary for basic RDF loading.
One can easily obtain one using:

```java
JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
```




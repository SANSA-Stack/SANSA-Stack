# OWLStats
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-owl-parent_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-owl-parent_2.11)
[![Build Status](https://ci.aksw.org/jenkins/job/SANSA%20OWL%20Layer/job/develop/badge/icon)](https://ci.aksw.org/jenkins/job/SANSA%20OWL%20Layer/job/develop/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Twitter](https://img.shields.io/twitter/follow/SANSA_Stack.svg?style=social)](https://twitter.com/SANSA_Stack)

One of the main challenges to the broader use of the Semantic Web is the complexity of getting a clear view of the available data sets. It is essential to know the internal structure, distribution, and coherence of the data in order to reuse, interlink, integrate, infer, or query a dataset published on the Web.
Since there are several machine learning algorithms, which were developed on top of the ontology axioms with the aim of predicting, classifying, and making sense of the data, the need to gain a clear view on OWL datasets became more prevalent. In this paper, we present OWLStats - a software component for gathering statistical information of large scale OWL datasets.
We present the primary distributed in-memory approach for computing 50 different statistical criteria for OWL datasets utilizing Apache Spark, which scales horizontally to a cluster of machines. OWLStats has been integrated into the SANSA framework. The preliminary results show that OWLStats provides a linear scale-up to a cluster of machines. 

## Main Application Class

The main application class is `net.sansa_stack.owl.spark.stats.OWLStats`. The application requires as application arguments:

- Path to the input file containing the OWL data (e.g. /data/input).
- The syntax of the input file (FUNCTIONAL, MANCHESTER, OWLXML) 

### Usage

The following Scala code shows how to read an OWL file in Functional Syntax (be it a local file or a file residing in HDFS) and then calculate the statstics for the input file:

```scala
import net.sansa_stack.owl.spark.owl._
import net.sansa_stack.owl.spark.stats

[...]

  val spark = SparkSession.builder
        .appName(s"OWL Stats")
        .master("local[*]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "net.sansa_stack.owl.spark.dataset.UnmodifiableCollectionKryoRegistrator")
        .getOrCreate()
      
  val syntax = Syntax.FUNCTIONAL
  val input = "path/to/file.owl"

  val axioms = spark.owl(syntax)(input)
  val stats = new OWLStats(spark).run(axioms)

[...]
```


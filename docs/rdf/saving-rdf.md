---
parent: RDF
title: Saving RDF
nav_order: 2
---

# Saving RDF


## Java

The class `net.sansa_stack.spark.io.rdf.output.RddRdfSaver` provides a fluent API to the combined functionality of spark and Jena.

```java
RDD<Dataset> rdd = ...;

RddRdfSaver.createForDataset(rdd)
    .setGlobalPrefixMapping(prefixMapping)
    .setOutputFormat(cmd.outFormat)
    .setMapQuadsToTriplesForTripleLangs(true)
    .setAllowOverwriteFiles(true)
    .setPartitionFolder(outFolder)
    .setTargetFile(outFile)
    .setAllowOverwriteFiles(true)
    .setDeletePartitionFolderAfterMerge(true)
    .run();

```

* The saver provides methods for the common RDF RDD types: `createForTriple`, `createForQuad`, `createForModel`, `createForQuad`.
* If `outFolder` and `outFile` are both `null`, the output is written to `STDOUT`.


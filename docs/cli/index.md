---
title: Commmand Line Tools
has_children: true
nav_order: 7
---

## SANSA Command Line Tools

The SANSA project features the `sansa` command line tool which provides several commands for working with RDF data.

### General Spark Options

Spark options can generally be provided with the `JAVA_OPTS` environment variable as shown in the example below:

```bash
JAVA_OPTS="-Dspark.executor.instances=4 -Dspark.executor.cores=8 -Dspark.hadoop.mapreduce.input.fileinputformat.split.maxsize=16000000 -Dspark.master=local[32]" sansa tarql query.tarql data.csv
```




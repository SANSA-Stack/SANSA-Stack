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



## Detailed Logging

Use the `-X` option to enable detailed logging. This will cause stack traces of exceptions to be shown as illustrated below.

```
sansa map ...

[INFO] Initialized BlockManager: BlockManagerId(driver, my.server, 37353, None)
[ERROR] NullPointerException: 
[INFO] Invoking stop() from shutdown hook
```

```
sansa map -X ...
[INFO] Initialized BlockManager: BlockManagerId(driver, my.server, 42565, None)
java.lang.RuntimeException: java.lang.NullPointerException
        at org.aksw.commons.util.exception.ExceptionUtilsAksw.rethrowUnless(ExceptionUtilsAksw.java:40)
        at org.aksw.commons.util.exception.ExceptionUtilsAksw.rethrowIfNotBrokenPipe(ExceptionUtilsAksw.java:81)
        at net.sansa_stack.spark.cli.main.MainCliSansaSpark.lambda$mainCore$0(MainCliSansaSpark.java:29)
        ...
[INFO] Invoking stop() from shutdown hook
```


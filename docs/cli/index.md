---
title: Commmand Line Tools
has_children: true
nav_order: 7
---

## SANSA Command Line Tools

The SANSA project features the `sansa` command line tool which provides several commands for working with RDF data.


### Running from the JAR Bundle

Unfortunately, a simple `java -jar sansa.jar` may not work because additional `--add-opens` declarations are needed.

* `Java 11`: Sansa CLI seems to works without `--add-opens` declarations althought warnings are shown. Adding the declarations below is recommended.
* `Java 17`: Sansa CLI won't work. The declarations must be added.

```bash
# Extra options for Java 11 and 17
# Source: https://stackoverflow.com/questions/73465937/apache-spark-3-3-0-breaks-on-java-17-with-cannot-access-class-sun-nio-ch-direct
SANSA_EXTRA_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens=java.base/java.io=ALL-UNNAMED \
    --add-opens=java.base/java.net=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/java.util=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
    --add-opens=java.base/sun.security.action=ALL-UNNAMED \
    --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
    --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

java $SANSA_EXTRA_OPTS -jar sansa.jar
```

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


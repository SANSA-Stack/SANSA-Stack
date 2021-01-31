
## Common commands (to be run from the root of the repo)

```bash
# Rebuild the cli module
mvn -pl sansa-spark-cli clean install

# Rebuild the deb package
mvn -Pdeb -pl sansa-debian-spark-cli clean install

# Locate the deb package in the target folder and install it using "sudo dpkg -i your.deb"
./reinstall-debs.sh
```



## Trig Query
Ad hoc querying over a list of .trig and .trig.bz2 files.


* Simple invocation
```bash
sansa trig --rq query.rq data1.trig.bz2 ... dataN.trig
```

* Options
```bash
Usage: sansa trig [-X] [--distinct] [-m=<sparkMaster>] [-o=<outFormat>]
                  [--rq=<queryFile>] <trigFiles>...
Run a special SPARQL query on a trig file
      <trigFiles>...     Trig File
      --distinct, --make-distinct
                         Start with making all quads across all input files
                           distinct; groups all named graphs by name. Default:
                           false
  -m, --spark-master=<sparkMaster>
                         Spark master. Default: local[*]
  -o, --out-format=<outFormat>
                         Output format. Default: srj
      --rq=<queryFile>   File with a SPARQL query (RDF Query)
  -X                     Debug mode; enables full stack traces

```

* `--distinct` adds a preprocessing step that merges all named graphs *after* the union rdd of all input files has been created. This is typically a very slow operation and it is recommended to preprocess any data into sorted .trig.bz2 files and use those as input to this tool.
If you are a data publisher then consider publishing sorted .trig.bz2 files as this allows for instant named-graph based analytics by consumers.

Note, that modern data catalogue systems such as the DBpedia databus provide metadata for datasets such that data processing pipelines can automatically adapt to given input data and optimize processing accordingly.

```turtle
<someDistribution>
    <http://dataid.dbpedia.org/ns/core#sorted> true ;
    dcat:downloadURL <http://.../data.trig> .
```




* Tuning parameters
If the graphs in the trig file are large then the max record length needs to be adjusted.

For this we added a hadoop level option called

`mapreduce.input.trigrecordreader.record.maxlength` 

Add the prefix 'spark.hadoop.' to configure this via spark:

Also, if intermediate results are large you may want to increase java's heap space.

```bash
JAVA_OPTS="-Xmx16g -Dspark.hadoop.mapreduce.input.trigrecordreader.record.maxlength=200000000 -Dspark.hadoop.mapreduce.input.trigrecordreader.probe.count=1 -Dspark.de
fault.parallelism=10" sansa trig -o tsv --rq query.sansa.rq data.trig.bz2
```



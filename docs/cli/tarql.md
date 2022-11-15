---
parent: Commmand Line Tools
title: Sansa Tarql
has_children: false
nav_order: 1
---

# Sansa Tarql

[TARQL (https://tarql.github.io/)](https://tarql.github.io/) is a tool for mapping tabular data to RDF via SPARQL queries.
This works by first converting each CSV row to a SPARQL binding and then evaluating the SPARQL query  w.r.t. to that binding.
Sansa features an Apache Spark based re-implementation that allows for processing a CSV file in parallel.

The implementation is also based on ApacheJena and our own "JenaX" extension project.

## Important

Parallel processing requires specifying `--out-file` and/or `--out-folder`! If neither argument is given then output is streamed to STDOUT using a single thread (using `RDD.toLocalIterator`).

## TARQL Compatibility

The design of Sansa tarql is aimed to ease the transition to or from the original tarql. Therefore, many options of the original are supported.


### Notable extensions:

* The [JenaX extension functions](https://github.com/Scaseco/jenax/tree/develop/jenax-arq-parent/jenax-arq-plugins-parent/jenax-arq-plugins-bundle) are available

### Notable limitations:

* The extension functions `tarql:expandPrefix(?prefix)` and `tarql:expandPrefixedName(?qname) expands` are not yet supported.

## CLI Options

```bash
Usage: sansa tarql [-hHrstuVX] [--accumulate] [--header-row] [--iriasgiven]
                   [--ntriples] [--optimize-prefixes] [--out-overwrite]
                   [--pretty] [-d=<delimiter>] [-e=<encoding>] [-o=<outFormat>]
                   [--oup=<deferOutputForUsedPrefixes>] [--out-file=<outFile>]
                   [--out-folder=<outFolder>] [-p=<quoteEscapeChar>]
                   [-q=<quoteChar>] [--sort-partitions=<sortPartitions>]
                   [--unique-partitions=<uniquePartitions>]
                   [--header-naming=<columnNamingSchemes>]...
                   [--op=<outPrefixes>]... <inputFiles>...
Map one or more CSV files to RDF via a single SPARQL query
      <inputFiles>...        tarql query file following by one or more csv file
      --accumulate           Accumulate every row's intermediate mapping output
                               in a dataset which subsequent mappings can query
  -d, --delimiter=<delimiter>
                             Delimiter
  -e, --encoding=<encoding>  Encoding (e.g. UTF-8, ISO-8859-1)
  -h, --help                 Show this help message and exit.
  -H, --no-header-row        no header row; use variable names ?a, ?b, ...
      --header-naming=<columnNamingSchemes>
                             Which column names to use. Allowed values: 'row',
                               'excel'. Numerics such as '0', '1' number with
                               that offset. If there are no header rows then
                               'row' is treated as 'excel'. Column names are
                               unqiue, first name takes precedence.
      --header-row           Input file's first row is a header with variable
                               names (default)
      --iriasgiven           Use an alternative IRI() implementation that is
                               non-validating but fast
      --ntriples             Tarql compatibility flag; turns any quad/triple
                               based output to nquads/ntriples
  -o, --out-format=<outFormat>
                             Output format. Default: null
      --op, --out-prefixes=<outPrefixes>
                             Prefix sources for output. Subject to used prefix
                               analysis. Default: rdf-prefixes/prefix.cc.
                               2019-12-17.ttl
      --optimize-prefixes    Only output used prefixes (requires additional
                               scan of the data)
      --oup, --out-used-prefixes=<deferOutputForUsedPrefixes>
                             Only for streaming to STDOUT. Number of records by
                               which to defer RDF output for collecting used
                               prefixes. Negative value emits all known
                               prefixes. Default: 100
      --out-file=<outFile>   Output file; Merge of files created in out-folder
      --out-folder=<outFolder>
                             Output folder
      --out-overwrite        Overwrite existing output files and/or folders
  -p, --escapechar=<quoteEscapeChar>
                             Quote escape character
      --pretty               Enables --sort, --unique and --optimize-prefixes
  -q, --quotechar=<quoteChar>
                             Quote character
  -r, --reverse              Sort ascending (does nothing if --sort is not
                               specified)
  -s, --sort                 Sort data (component order is gspo)
      --sort-partitions=<sortPartitions>
                             Number of partitions to use for the sort operation
  -t, --tabs                 Separators are tabs; default: false
  -u, --unique               Make quads unique
      --unique-partitions=<uniquePartitions>
                             Number of partitions to use for the unique
                               operation
  -V, --version              Print version information and exit.
  -X                         Debug mode; enables full stack traces

```


## Examples

The `sansa` command is available either by installing the debian/rpm packages or by running the jar bundle directly using `java -jar sansa-version.jar`
Depending on the Java version you may need to add `--add-opens` declarations as documented [here](index.md).

* Basic Invocation
```bash
sansa tarql mapping.rq input.csv --out-file /tmp/output.ttl
```

* Faster processing of the IRI() function by disabling validation. Only use this if you can ensure the generated IRIs will be valid.
```bash
sansa tarql mapping.rq input.csv --iriasgiven --out-file /tmp/output.ttl
```

* Options for spark/hadoop can be supplied via the `JAVA_OPTS` environment variable. Also, `--out-overwrite` deletes any existing hadoop files.
```bash
JAVA_OPTS="-Dspark.master=local[4]" sansa tarql mapping.rq input.csv --out-overwrite --out-folder /tmp/out-folder
```



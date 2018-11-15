## Querying heterogeneous data sources (data lake)

This feature allows to query data stored in heterogeneous data sources using SPARQL. The following data sources are supported: CSV, Parquet, MongoDB, Cassandra, Elasticsearch, Couchbase, JDBC (MySQL, SQL Server, etc.).

**Note:** Refer to [SANSA-DataLake](https://github.com/SANSA-Stack/SANSA-DataLake) to learn about the concepts used in this guideline.

### Usage

```scala
import net.sansa_stack.query.spark.query._

val configFile = "/config"
val mappingsFile = "/mappings.ttl"
val query = "SPARQL query"

val resultsDF = spark.sparqlDL(query, mappingsFile, configFile)
```

#### Input
`configFile` and `mappingsFile` are the paths to the two required _config_ ([example](https://raw.githubusercontent.com/SANSA-Stack/SANSA-Query/feature/datalake/sansa-query-spark/src/main/resources/config)) and _mappings_ ([example](https://raw.githubusercontent.com/SANSA-Stack/SANSA-Query/feature/datalake/sansa-query-spark/src/main/resources/mappings.ttl)) files, `query` ([example](https://raw.githubusercontent.com/SANSA-Stack/SANSA-Query/feature/datalake/sansa-query-spark/src/main/resources/queries/Q1.sparql)) is a string variable holding a correct SPARQL query.

**Note:** Refer to [SANSA-DataLake](https://github.com/SANSA-Stack/SANSA-DataLake) to learn about the suggested ways of creating the config and mappings file.

#### Output
`resultsDF` is a Spark DataFrame with each column corresponding to the projected variables in the SPARQL query.

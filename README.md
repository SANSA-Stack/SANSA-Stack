# DataLake
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-datalake-parent_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-datalake-parent_2.11)
[![Build Status](https://ci.aksw.org/jenkins/job/SANSA-ML/job/develop/badge/icon)](https://ci.aksw.org/jenkins/job/SANSA-DataLake//job/master/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Twitter](https://img.shields.io/twitter/follow/SANSA_Stack.svg?style=social)](https://twitter.com/SANSA_Stack)

A library to query heterogeneous data sources uniformly using SPARQL.

## Description
### Data Lake
The term Data Lake denotes a schema-less repository of data residing in its original format and form. As such, there is not a single point of entry to the Data Lake, as data in its diversity has various schemata, query interfaces and languages.

### _Semantic_ Data Lake
Semantic Data Lake is an effort to enable querying this wealth of heterogeneous data using Semantic Web principles: mapping language and  SPARQL query language. This supplies the Data Lake with a schema and enables a one entry point, SPARQL query, to the various heterogeneous data. In order to reach a data source, the latter needs to be connected to.

That said, to query the data lake using the _Semantic Data Lake_ approach, users need to provide three inputs: (1) Mappings file, (2) Config file, and (3) a SPARQL query, described in the next three sections.

### 1. Mapping Language and Data Lake Schema
A virtual schema is added to the Data Lake by _mapping_ data elements, e.g., tables and attributes to ontology concepts, e.g., classes and predicates. We benefit from [RML](http://rml.io/) mappings to express those schema mapping links.

An example of such mappings is given below. It maps a collection named _Product_ (`rml:source "Product"`) in a MongoDB database to an ontology class _Product_ (`rr:class bsbm:Product`), meaning that every documebt in Product document is of type `bsbm:Product`. The mappings also link MongoDB collection fields `label`, `publisher` and `producer` to ontology predicates `rdfs:label`, `dc:publisher` and `bsbm:producer`, respectively. The `_id` field found in `rr:subjectMap rr:template "http://example.com/{_id}"` triple points to the primary key of MongoDB collection.

```
<#OfferMapping>
	rml:logicalSource [
		rml:source "//Offer";
		nosql:store nosql:Mongodb
	];
	rr:subjectMap [
		rr:template "http://example.com/{_id}";
		rr:class schema:Offer
	];

	rr:predicateObjectMap [
		rr:predicate bsbm:validTo;
		rr:objectMap [rml:reference "validTo"]
	];

	rr:predicateObjectMap [
		rr:predicate dc:publisher;
		rr:objectMap [rml:reference "publisher"]
	];

	rr:predicateObjectMap [
		rr:predicate bsbm:producer;
		rr:objectMap [rml:reference "producer"]
	];
```

Note the presence of the triple `nosql:store nosql:MongoDB`, it contains an addition to RML mappings from the [NoSQL ontology](http://purl.org/db/nosql#) to allow stating what type of source it is being mapped.

_The mappings file can either be created manually or using the following graphical utility: [Squerall-GUI](https://github.com/EIS-Bonn/Squerall-GUI)_.

### 2. Data Connection Configurations
In order for data to connect to a data source, users need to provide a set of config parameters, in JSON format. This differs from data source to another, for example for a MongoDB collection, the config parameters could be: database host URL, database name, collection name, and replica set name.

```JSON
{
  "type": "mongodb",
  "options": {
    "url": "127.0.0.1",
    "database": "bsbm",
    "collection": "offer",
    "options": "replicaSet=mongo-rs"
  },
  "source": "//Offer",
  "entity": "Offer"
}
```

It is necessary to link the configured source (`"source": "//Offer"`)  to the mapped source (`rml:logicalSource rml:source "//Offer"`, see Mapping section above)

_The config file can either be created manually or using the following graphical utility: [Squerall-GUI](https://github.com/EIS-Bonn/Squerall-GUI)_.

### 3. SPARQL Query Interface
SPARQL queries are expressed using the Ontology terms the data was previously mapped to. SPARQL query should conform to the currently supported SPARQL fragment:

```SPARQL
Query       := Prefix* SELECT Distinguish WHERE{ Clauses } Modifiers?
Prefix      := PREFIX "string:" IRI
Distinguish := DISTINCT? (“*”|(Var|Aggregate)+)
Aggregate   := (AggOpe(Var) ASVar)
AggOpe      := SUM|MIN|MAX|AVG|COUNT
Clauses     := TP* Filter?
Filter      := FILTER (Var FiltOpe Litteral)
             | FILTER regex(Var, "%string%")
FiltOpe     :==|!=|<|<=|>|>=
TP          := VarIRIVar .|Varrdf:type IRI.
Var         := "?string"
Modifiers   := (LIMITk)? (ORDER BY(ASC|DESC)? Var)? (GROUP BYVar+)?
```

## Usage
The usage of the Semantic Data Lake is documented under the respective SANSA-Query [datalake component](https://github.com/SANSA-Stack/SANSA-Query/tree/feature/datalake/sansa-query-spark/src/main/scala/net/sansa_stack/query/spark/datalake).

## How to Contribute
We always welcome new contributors to the project! Please see [our contribution guide](http://sansa-stack.net/contributing-to-sansa/) for more details on how to get started contributing to SANSA.

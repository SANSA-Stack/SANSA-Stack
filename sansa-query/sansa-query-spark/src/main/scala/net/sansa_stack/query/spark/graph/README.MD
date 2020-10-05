# Sparql To GraphX

### Introduction<a name="introduction"></a>
This package provide a interface to convert Sparql to GraphX, which aim to use Sparql queries to generate [a property graph](https://spark.apache.org/docs/latest/graphx-programming-guide.html#the-property-graph) which is a class of package [org.apache.spark.graphx](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.package). In this case, we use Apache Jena query engine [ARQ](https://jena.apache.org/documentation/query/) to parser [SPARQL RDF Query language](https://www.w3.org/TR/sparql11-query/), convert basic graph pattern to graph in which the type of vertices and edges are [Node](https://jena.apache.org/documentation/javadoc/jena/org/apache/jena/graph/Node.html). The graph with variables can match target rdf graph partitioned by [graph partition algorithm](../../partition/spark/algo) in parallel and generate solution mappings.

### Usage

#### Get input files
```scala
val ntpath = "..."    //path of RDF N-Triple file
val sppath = "..."    //path of SPARQL query file
```

#### Initialize Spark session
```scala
val session = SparkSession.builder().master("local[*]").appName("sparql to graphx").getOrCreate()
val graph = LoadGraph.apply (NTripleReader.load (session, ntPath))    // Load input N-Triple file and get as graph
val sp = new SparqlParser(spPath)   // Create an object to parser the sparql query syntax
```

#### Use a pipeline to generate the query result in sequence
```scala
var intermediate = Array[Map[Node, Node]]()
sp.getOps.foreach{
  case op: ResultOp => intermediate = op.execute(intermediate)
  case op: PatternOp => intermediate = op.execute(intermediate, graph, session)
}
val reuslt = intermediate
result.foreach(println(_))
```

### Sparql Examples

#### Example: BGP Matching<a name="BGP"></a>
```sparql
PREFIX tw: <http://twitter/>
SELECT ?user ?follower
WHERE {
    ?user tw:follows tw:user1 .
    tw:user1 tw:follows ?follower .
}
```

### Including Optional Values

#### Example: [Optional Pattern Matching](https://www.w3.org/TR/sparql11-query/#OptionalMatching)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name ?age
WHERE {
    ?user foaf:age ?age .
    OPTIONAL { ?user foaf:name ?name . }
}
```

#### Example: [Constraints in Optional Pattern Matching](https://www.w3.org/TR/sparql11-query/#OptionalAndConstraints)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT DISTINCT ?user ?age
WHERE {
    ?user tw:follows ?follower .
    ?follower foaf:name ?name
    FILTER regex(?name, "Di")
    OPTIONAL { ?user foaf:age ?age . FILTER (?age>25) }
}
```

#### Example: [Multiple Optional Graph Patterns](https://www.w3.org/TR/sparql11-query/#MultipleOptionals)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT ?name ?age
WHERE {
    ?user tw:follows tw:user7
    OPTIONAL { ?user foaf:age ?age . }
    OPTIONAL { ?user foaf:name ?name . }
}
```
### Matching Alternatives

#### Example: [UNION](https://www.w3.org/TR/sparql11-query/#alternatives)
```sparql
PREFIX tw: <http://twitter/>
SELECT ?user
WHERE {
    { ?user tw:follows tw:user7 . }
    UNION { ?user tw:follows tw:user2 . }
}
```

#### Example: Constraints in Union Pattern Matching
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?user ?age
WHERE {
    {   ?user foaf:age ?age .
        FILTER(?age>25)
    }
    UNION {
        ?user foaf:age ?age .
        FILTER(?age<20)
    }
}
```

### [Negation](https://www.w3.org/TR/sparql11-query/#negation)
Execute operations of expressions by the keyword FILTER.

#### Example: Filter Regex<a name="Regex"></a>
```sparql
PREFIX tw: <http://twitter/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?user ?follower
WHERE {
    ?user foaf:name ?name .
    FILTER regex(?name, "Ali")
}
```
Note: Currently only support FILTER regex expression without flags argument.

#### Example: Filter Compare<a name="Compare"></a>
```sparql
PREFIX tw: <http://twitter/>
PREFIX ex: <http://example.org/>
SELECT ?user ?age
WHERE {
    ?user tw:follows tw:user7 .
    ?user ex:age ?age .
    FILTER (?age <= 12)
}
```
Note: Support operators include Equals(=), Not Equals(!=), GreaterThanOrEqual(>=), GreaterThan(>), LessThanOrEqual(<=), LessThan(<).

#### Example: Filtering Pattern<a name="Pattern"></a>
```sparql
PREFIX tw: <http://twitter/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?user
WHERE {
    ?user tw:follows tw:user7 .
    FILTER EXISTS { ?user foaf:name ?name }
}
```
AND
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT ?name WHERE {
    ?user foaf:name ?name
    FILTER NOT EXISTS {
        ?user  tw:follows ?follower .
        ?follower foaf:name "Diana" .
    }
}
```

#### Example: [Filter Bound](https://www.w3.org/TR/sparql11-query/#func-bound)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?user ?age
WHERE {
    ?user foaf:age ?age .
    OPTIONAL { ?user foaf:name ?name . }
    FILTER (bound(?name))
}
```

#### Example: [MINUS](https://www.w3.org/TR/sparql11-query/#neg-minus)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT ?name WHERE {
    ?user foaf:name ?name
    MINUS {
        ?user  tw:follows ?follower .
        ?follower foaf:name "Diana" .
    }
}
```

### [Property Paths](https://www.w3.org/TR/sparql11-query/#propertypaths)

### Assignment

#### Example: [BIND](https://www.w3.org/TR/sparql11-query/#bind)

#### Example: [VALUES](https://www.w3.org/TR/sparql11-query/#inline-data)

### Aggregates

#### Example: [GROUP BY](https://www.w3.org/TR/sparql11-query/#groupby)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT DISTINCT ?user (AVG(?age) as ?ages) (MAX(?age) as ?max) (MIN(?age) as ?min)
WHERE {
    ?user tw:follows ?follower .
    ?follower foaf:age ?age .
} GROUP BY ?user
```
Note: The example is produced by grouping solutions according to the GROUP BY expression. Currently support group by 
simple variables, and support aggregate operation as COUNT, SUM, MIN, MAX, AVG.

#### Example: [HAVING](https://www.w3.org/TR/sparql11-query/#having)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT DISTINCT ?user (AVG(?age) as ?ages)
WHERE {
    ?user tw:follows ?follower .
    ?follower foaf:age ?age .
} GROUP BY ?user
HAVING (?ages>15)
```

### Solution Sequences and Modifiers
Sequence modifiers are applied to operate on unordered solutions generated by BGP match.

#### Example: [Order By](https://www.w3.org/TR/sparql11-query/#modOrderBy)<a name="OrderBy"></a>
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT DISTINCT ?user ?age
WHERE {
    ?user tw:follows ?follower .
    ?user foaf:age ?age .
} ORDER BY ?age
```
Note: support write functions for the variables, such as ORDER BY xsd:double(str(?age)).

#### Example: [Projection](https://www.w3.org/TR/sparql11-query/#modProjection)<a name="Projection"></a>
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name
WHERE {
    ?user foaf:name ?name .
}
```
Note: Support using * to select all variables and select required variables.

#### Example: [DISTINCT](https://www.w3.org/TR/sparql11-query/#modDuplicates)<a name="Distinct"></a>
```sparql
PREFIX tw: <http://twitter/>
SELECT DISTINCT ?user
WHERE {
    ?user tw:follows ?follower .
}
```

#### Example: [REDUCED](https://www.w3.org/TR/sparql11-query/#modDuplicates)<a name="Reduced"></a>
```sparql
PREFIX tw: <http://twitter/>
SELECT REDUCED ?user
WHERE {
    ?user tw:follows ?follower .
}
```

#### Example: [SLICE](https://www.w3.org/TR/sparql11-query/#modOffset)<a name="Offset"></a>
```sparql
PREFIX tw: <http://twitter/>
SELECT DISTINCT ?user
WHERE {
    ?user tw:follows ?follower .
} LIMIT 3
OFFSET 2
```

#### Example: [DESCRIBE](https://www.w3.org/TR/sparql11-query/#describe)
Returns a single result RDF graph containing RDF data about resources.
```sparql
PREFIX tw: <http://twitter/>
DESCRIBE ?user
WHERE { ?user tw:follows tw:user3 . }
```

### Future Work
1. Cover other operators of SPARQL as DESCRIBE, CONSTRUCT and ASK, even Property Paths.
2. Manipulate the result as RDD and output the result as DataFrame and DataSets.

package net.sansa_stack.query.spark.dof

object Tests {
  val sparqlQueries = List(
    // 0
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  ?X rdf:type ub:Person.
  ?X ?Y ?Z.
}
""",
    // 1
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X rdf:type ub:Person.
}
""",
    // 2
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?M
WHERE {
  ?X rdf:type ub:Person.
  ?X ub:hobby ub:Car.
  ?X ub:mbox ?M.
  ?X rdf:name ?Y.
}
  """,
    // 3
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?Y ?Z
WHERE {
  ?Y ub:friendOf ?X.
  ?X ub:age ?Z.
}
""",
    // 4
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?M ?Y
WHERE {
  ?X rdf:type ub:Person.
  ?X ub:hobby ub:Car.
  ?X ub:mbox ?M.
  ?X rdf:name ?Y.
}
  """,
    // 5
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  ?X ub:hobby ub:Car.
  ?X1 ?Y ?Z.
}
""",
    // 6
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?M ?Z
WHERE {
  ?Y ub:age ?Z.
  ?Y ub:mbox ?M.
}
""",
    // 7
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  ?Y ub:friendOf ?X.
  ?Y ub:age ?Z.
  ?Y ub:mbox ?M.
}
""",
    // 8
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  ?Y ub:friendOf ?X.
  ?Y ub:age ?Z.
  ?X ub:mbox ?M.
}
""",
    // 9
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  ?X ?Y ?Z.
}
""",
    // 10
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y
WHERE {
  ?X ub:mbox ?Y.
  ?X rdf:type ub:Person.
}
""",
    // 11
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y
WHERE {
  ?X ub:friendOf ?Y.
  ?Y ub:friendOf ?X.
}
""",
    // 12
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X1 ?Y ?Z
WHERE {
  ?X1 rdf:type ub:Person.
  ?X ?Y ?Z.
}
""",
    // 13
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?M ?Y1
WHERE {
  ?X rdf:type ub:Person.
  ?X ub:hobby ub:Car.
  ?X ub:mbox ?M.
  ?X rdf:name ?Y.
  ?X ?Z ?Y1.
}
  """,
    // 14
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y
WHERE {
  ?X rdf:type ub:Person.
  ?X ?Y ?Z.
}
""",
    // 15
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  ?X ?Y ?Z.
}
""",
    // 16 Union
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?Y
WHERE {
  { ?X rdf:name ?Y }
  UNION
  { ?X ub:age ?Y }
}
""",
    // 17 Union
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?Y ?M
WHERE {
  { ?X rdf:name ?Y }
  UNION
  { ?X ub:mbox ?M }
}
""",
    // 18
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  <http://www.Department0.University0.edu/UndergraduateStudent129> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse> ?X.
}
""",
    // 19
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> .
  ?X rdf:type ub:GraduateStudent .
}
    """,
    // 20
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  ?Z rdf:type ub:Department .
  ?Z ub:subOrganizationOf ?Y .
  ?Y rdf:type ub:University .
  ?X ub:undergraduateDegreeFrom ?Y .
  ?X ub:memberOf ?Z .
  ?X rdf:type ub:GraduateStudent .
}
    """,
    // 21 TODO: Duplicate as #22
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X rdf:type ub:Publication
}
    """,
    // 22 LUBM full uni3
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X rdf:type ub:Publication
}
""",
    // 23 OPTIONAL
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?M
WHERE {
  ?X rdf:type ub:Person.
  ?X ub:mbox ?M.
  OPTIONAL {
  ?X ub:hobby ub:Car.
  }
  }
""",
    // 24 watdiv uni4
    """
PREFIX schema: <http://schema.org/>
PREFIX rev: <http://purl.org/stuff/rev#>
SELECT ?v0 ?v1
WHERE {
  ?v0 schema:caption ?v1 .
}
    """,
    // 25 watdiv uni4
    """
PREFIX schema: <http://schema.org/>
PREFIX rev: <http://purl.org/stuff/rev#>
SELECT ?v0 ?v1 ?v2 ?v3
WHERE {
  ?v0 schema:caption ?v1 .
  ?v0 schema:text ?v2 .
  ?v0 schema:contentRating ?v3 .
}
    """,
    // 26 watdiv uni4
    """
PREFIX schema: <http://schema.org/>
PREFIX rev: <http://purl.org/stuff/rev#>
SELECT ?v0 ?v1 ?v6 ?v8
WHERE {
  ?v0 schema:caption ?v1 .
  ?v0 rev:hasReview ?v4 .
  ?v4 rev:title ?v5 .
  ?v4 rev:reviewer ?v6 .
  ?v7 schema:actor ?v6 .
  ?v7 schema:language ?v8 .
}
    """,
    // 27  dbpedia dataset TODO: check on cluster not so many Underarm
    """
PREFIX ont: <http://dbpedia.org/ontology/>
PREFIX res: <http://dbpedia.org/resource/>
SELECT ?v0
WHERE {
  res:Underarm ont:wikiPageWikiLink ?v0.
}
""",
    // 28  dbpedia dataset TODO: check on cluster a lot of Alanis_Morissette
    """
PREFIX terms: <http://purl.org/dc/terms/>
PREFIX res: <http://dbpedia.org/resource/>
SELECT ?v0
WHERE {
  res:Alanis_Morissette terms:subject ?v0.
}
""",
    // 21
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X
WHERE {
  ?X rdf:type ub:Person.
  ?X ub:age 23.
}
""",
    // 22
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?X1
WHERE {
  ?X rdf:type ub:Person.
  ?X1 rdf:name ub:Mary.
}
""",
    // 0
    """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
SELECT ?X ?Y ?Z
WHERE {
  {?X rdf:type ub:Person.
  ?X ?Y ?Z.
  FILTER (bound(?X))
} UNION {
   ?R rdf:type ub:Person.
  } OPTIONAL {
  ?T rdf:type ub:Person.
  }
  }
""")
}


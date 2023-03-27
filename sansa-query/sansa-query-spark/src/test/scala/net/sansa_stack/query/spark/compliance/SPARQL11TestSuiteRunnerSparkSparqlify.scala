package net.sansa_stack.query.spark.compliance

import net.sansa_stack.query.spark.api.domain.QueryEngineFactory
import org.scalatest.DoNotDiscover
import org.scalatest.tags.Slow
import net.sansa_stack.query.spark.ontop.QueryEngineFactoryOntop
import net.sansa_stack.query.spark.sparqlify.QueryEngineFactorySparqlify


/**
 * SPARQL 1.1 test suite runner for Ontop-based SPARQL-to-SQL implementation on Apache Spark.
 *
 *
 * @author Lorenz Buehmann
 */
@DoNotDiscover
@Slow
class SPARQL11TestSuiteRunnerSparkSparqlify
  extends SPARQL11TestSuiteRunnerSpark {

    override lazy val IGNORED_URIS = {
        // AGGREGATES
        Set("agg-err-02").map(aggregatesManifest + _) ++
          // BINDINGS
          Set("values8", "values5").map(bindingsManifest + _) ++ // TODO: fix it (UNDEF involves the notion of COMPATIBILITY when joining)
          // FUNCTIONS
          Set("bnode01", "bnode02", // Sorting by IRI is not supported by the SI
              //  "hours", "day", // the SI does not preserve the original timezone
              "if01", "if02", // not supported in SPARQL transformation
              "in01", "in02",
              "iri01", // not supported in H2 transformation
              //        "md5-01", "md5-02", // The SI does not support IRIs as ORDER BY conditions
              "plus-1", "plus-2",
              "tz", "timezone",
              "sha1-01", "sha1-02", // SHA1 is not supported in H2
              "sha512-01", "sha512-02", // SHA512 is not supported in H2
              "strdt01", "strdt02", "strdt03",
              "strlang01", "strlang02", "strlang03",
              "struuid01", "uuid01" // some tests that work on an empty model which we do not support in Spark query as the mappings would be empty (could be handled but
              // most likely will never happen)
          ).map(functionsManifest + _) ++
          // CONSTRUCT not supported yet
          Set("constructwhere01", "constructwhere02", "constructwhere03", // problem importing dataset
              "constructwhere04").map(constructManifest + _) ++
          // Projection cannot be cast to Reduced in rdf4j
          // CSV
          Set("tsv01", "tsv02", // different format for number and not supported custom datatype
              "tsv03").map(csvTscResManifest + _) ++
          // GROUPING
          Set("group04").map(groupingManifest + _) ++ // Multi-typed COALESCE as grouping condition TODO: support it
          // NEGATION not supported yet
          Set("subset-by-exclusion-nex-1", "temporal-proximity-by-exclusion-nex-1", "subset-01", "subset-02", "set-equals-1", "subset-03", "exists-01", "exists-02", // DISABLED DUE TO ORDER OVER IRI
              "full-minuend", "partial-minuend", // TODO: enable it
              "full-minuend-modified", "partial-minuend-modified").map(negationManifest + _) ++
          // EXISTS not supported yet
          Set("exists01", "exists02", "exists03", "exists04", "exists05").map(existsManifest + _) ++
          // PROPERTY PATH
          Set("pp02", // wrong result, unexpected binding // Not supported: ArbitraryLengthPath
              "pp06", "pp12", "pp14", "pp16", "pp21", "pp23", "pp25", // Not supported: ZeroLengthPath
              "pp28a", "pp34", "pp35", "pp36", "pp37").map(propertyPathManifest + _) ++
          // SERVICE not supported yet
          Set("service1", // no loading of the dataset
              "service2", "service3", "service4a", "service5", "service6", "service7").map(serviceManifest + _) ++
          // SUBQUERY
          // Quad translated as a triple. TODO: fix it
          Set("subquery02", "subquery04", // EXISTS is not supported yet
              "subquery10", // ORDER BY IRI (for supported by the SI)
              "subquery11", // unbound variable: Var TODO: fix it
              "subquery12", "subquery13", // missing results (TODO: fix)
              "subquery14").map(subqueryManifest + _)
    }

  override def getEngineFactory: QueryEngineFactory = new QueryEngineFactorySparqlify(spark)
}

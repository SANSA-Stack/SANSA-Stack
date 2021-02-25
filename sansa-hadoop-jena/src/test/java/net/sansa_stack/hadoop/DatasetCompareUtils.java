package net.sansa_stack.hadoop;

import com.google.common.collect.Sets;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.sparql.resultset.ResultSetCompare;
import org.junit.Assert;

import java.io.PrintStream;
import java.util.Set;

// TODO Move this class to a better place - e.g. jena-sparql-api-utils
public class DatasetCompareUtils {
    /**
     * Check two datasets for isomorphism using comparison by value.
     * Internally converts the datasets into result sets with ?g ?s ?p ?o) bindings
     * and compares them using {@link ResultSetCompare#equalsByValue(org.apache.jena.query.ResultSet, org.apache.jena.query.ResultSet)}
     *
     * <b>Does not scale to large datasets. For large number of named graphs use
     * {@link DatasetCompareUtils#assertIsIsomorphicByGraph(Dataset, Dataset)}</b>
     *
     * @param expected
     * @param actual
     * @param compareByValue 'false' tests for equivalence of terms whereas 'true' tests for that of values
     * @param out The outputstream to which to display any differences
     * @return
     */
    public static boolean isIsomorphic(
            Dataset expected,
            Dataset actual,
            boolean compareByValue,
            PrintStream out,
            Lang lang) {
        boolean result;

        String everything = "SELECT ?g ?s ?p ?o { { GRAPH ?g { ?s ?p ?o } } UNION { ?s ?p ?o } }";
//		String everything = "SELECT ?o { { { GRAPH ?g { ?s ?p ?o } } UNION { ?s ?p ?o } } FILTER(isNumeric(?o))}";
        try (QueryExecution qea = QueryExecutionFactory.create(everything, expected);
             QueryExecution qeb = QueryExecutionFactory.create(everything, actual)) {

            ResultSetRewindable rsa = ResultSetFactory.copyResults(qea.execSelect());
            ResultSetRewindable rsb = ResultSetFactory.copyResults(qeb.execSelect());

            result = compareByValue
                    ? ResultSetCompare.equalsByValue(rsa, rsb)
                    : ResultSetCompare.equalsByTerm(rsa, rsb);

            if (!result) {
                rsa.reset();
                rsb.reset();
                out.println("Expected:");
                ResultSetMgr.write(out, rsa, lang);
                out.println("Actual:");
                ResultSetMgr.write(out, rsb, lang);
            }
        }

        return result;
    }


    public static void assertIsIsomorphicByGraph(Dataset ds1, Dataset ds2) {

    /*

    val a = Sets.newHashSet(ds1.asDatasetGraph().find())
    val b = Sets.newHashSet(ds2.asDatasetGraph().find())

    val diff = new SetDiff[Quad](a, b)

    System.err.println("Excessive")
    for(x <- diff.getAdded.asScala) {
      System.err.println("  " + x)
    }

    System.err.println("Missing")
    for(x <- diff.getRemoved.asScala) {
      System.err.println("  " + x)
    }

    System.err.println("Report done")
    */

/*
    System.err.println("Dataset 1");
    RDFDataMgr.write(System.err, ds1, RDFFormat.TRIG_PRETTY);
    System.err.println("Dataset 2");
    RDFDataMgr.write(System.err, ds2, RDFFormat.TRIG_PRETTY);
    System.err.println("Datasets printed");
*/
        // compare default graphs first
        Assert.assertTrue("default graphs do not match", ds1.getDefaultModel().getGraph().isIsomorphicWith(ds2.getDefaultModel().getGraph()));

        // then compare the named graphs
        Set<String> allNames = Sets.union(Sets.newHashSet(ds1.listNames()), Sets.newHashSet(ds2.listNames()));

        for (String g : allNames) {

            Assert.assertTrue("graph <" + g + "> not found in first dataset", ds1.containsNamedModel(g));
            Assert.assertTrue("graph <" + g + "> not found in second dataset", ds2.containsNamedModel(g));

            Graph g1 = ds1.getNamedModel(g).getGraph();
            Graph g2 = ds2.getNamedModel(g).getGraph();

            Assert.assertEquals("size of graph <" + g + "> not the same in both datasets", g1.size(), g2.size());
            // Isomorphism check may fail with stack overflow execution if datasets
            // become too large
            // assert(g1.isIsomorphicWith(g2), s"graph <$g> not isomorph")
        }
    }
}

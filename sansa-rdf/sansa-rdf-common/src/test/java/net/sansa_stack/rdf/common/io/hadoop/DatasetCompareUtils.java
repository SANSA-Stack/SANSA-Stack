package net.sansa_stack.rdf.common.io.hadoop;

import org.apache.jena.query.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.sparql.resultset.ResultSetCompare;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;

// TODO Move this class to a better place - e.g. jena-sparql-api-utils
public class DatasetCompareUtils {
    /**
     * Check two datasets for isomorphism using comparison by value.
     * Internally converts the datasets into result sets with ?g ?s ?p ?o) bindings
     * and compares them using {@link ResultSetCompare#equalsByValue(org.apache.jena.query.ResultSet, org.apache.jena.query.ResultSet)}
     *
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

}

package net.sansa_stack.integration.test;

import org.aksw.commons.util.exception.ExceptionUtilsAksw;
import org.aksw.jena_sparql_api.delay.extra.Delayer;
import org.aksw.jena_sparql_api.delay.extra.DelayerDefault;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;

import java.util.function.Supplier;

/**
 * An ad-hoc util class to await results from a sparql endpoint which may yet have to start up
 */
public class AwaitUtils {

    /** Keep trying to execute a SPARQL SELECT query at the given sparql endpoint url and return the number of obtained bindings */
    public static long countResultBindings(String sparqlEndpointUrl, String queryString, Supplier<Boolean> continationCondition) {
        Delayer delayer = DelayerDefault.createFromNow(1000);
        long resultSetSize = -1;
        // NOTE May need more than 3 minutes on older systems?
        for (int i = 0; i < 180; ++i) {

            boolean doContinue = Boolean.valueOf(true).equals(continationCondition.get());
            if (!doContinue) {
                break;
            }

            // SparqlQueryConnectionWithReconnect.create(() -> RDFConnectionFactory.connect(sparklfyUrl));
            // System.out.println("Testing");
            try (RDFConnection conn = RDFConnectionFactory.connect(sparqlEndpointUrl)) {
                resultSetSize = ResultSetFormatter.consume(conn.query(queryString).execSelect());
                // System.out.println("Count: " + resultSetSize);
                break;
            } catch(Exception e) {
                ExceptionUtilsAksw.rethrowUnless(e,
                        ExceptionUtilsAksw::isConnectionRefusedException,
                        ExceptionUtilsAksw::isBrokenPipeException);
            }

            try {
                delayer.doDelay();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return resultSetSize;
    }
}

package net.sansa_stack.integration.test;

import org.aksw.commons.util.exception.ExceptionUtilsAksw;
import org.aksw.jena_sparql_api.delay.extra.Delayer;
import org.aksw.jena_sparql_api.delay.extra.DelayerDefault;
import org.apache.http.NoHttpResponseException;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;

import java.net.SocketException;
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

        int n = 180;
        for (int i = 0; i < n; ++i) {

            boolean doContinue = Boolean.valueOf(true).equals(continationCondition.get());
            if (!doContinue) {
                throw new RuntimeException("Retry aborted because condition no longer satisfied");
            }

            // SparqlQueryConnectionWithReconnect.create(() -> RDFConnectionFactory.connect(sparklfyUrl));
            // System.out.println("Testing");
            try (RDFConnection conn = RDFConnectionFactory.connect(sparqlEndpointUrl)) {
                resultSetSize = ResultSetFormatter.consume(conn.query(queryString).execSelect());

                return resultSetSize;
                // System.out.println("Count: " + resultSetSize);
            } catch(Exception e) {
                ExceptionUtilsAksw.rethrowUnless(e,
                        ExceptionUtilsAksw::isConnectionRefusedException,
                        ExceptionUtilsAksw::isBrokenPipeException,
                        ExceptionUtilsAksw.isRootCauseInstanceOf(NoHttpResponseException.class),
                        ExceptionUtilsAksw.isRootCauseInstanceOf(SocketException.class));
            }

            try {
                delayer.doDelay();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        throw new RuntimeException("Retry aborted after " + n + " unsuccessful attempts");
    }
}

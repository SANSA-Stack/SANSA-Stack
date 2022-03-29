package net.sansa_stack.spark.io.rdf.output;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;
import java.util.function.Function;

/** Interface that captures common operations on RDD&lt;T@gt;
 *
 * @param <T>
 */
public interface RddRdfOps<T> {
    /** Whether the operations provided by this class are based on quads */
    boolean usesQuads();

    void sendRecordToStreamRDF(T record, StreamRDF streamRDF);
    JavaRDD<Triple> convertToTriple(JavaRDD<T> rdd);
    JavaRDD<Quad> convertToQuad(JavaRDD<T> rdd);
    JavaRDD<Node> convertToNode(JavaRDD<T> rdd);
    Function<? super T, Comparable<?>> getKeyFunction();
}

package net.sansa_stack.spark.io.rdf.output;

import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;

public interface RddRdfDispatcher<T> {
    void sendRecordToStreamRDF(T record, StreamRDF streamRDF);
    JavaRDD<Triple> convertToTriple(JavaRDD<T> rdd);
    JavaRDD<Quad> convertToQuad(JavaRDD<T> rdd);
}

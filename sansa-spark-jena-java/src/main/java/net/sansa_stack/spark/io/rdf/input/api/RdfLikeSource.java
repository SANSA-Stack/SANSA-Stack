package net.sansa_stack.spark.io.rdf.input.api;

import org.apache.jena.riot.system.PrefixMap;

public interface RdfLikeSource {
    /** At present this creates a model holding an RDF sample based on a file's starting bytes.
     * May be changed to PrefixMap
     */
    PrefixMap peekDeclaredPrefixes();
}

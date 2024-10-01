package net.sansa_stack.spark.io.rdf.input.api;

import org.apache.jena.riot.system.PrefixMap;

public interface RdfLikeSource {
    /**
     * Return the prefixes declared on this source.
     */
    PrefixMap peekDeclaredPrefixes();
}

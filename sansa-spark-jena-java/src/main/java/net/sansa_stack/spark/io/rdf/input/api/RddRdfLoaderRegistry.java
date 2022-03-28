package net.sansa_stack.spark.io.rdf.input.api;

import org.apache.jena.riot.Lang;

public interface RddRdfLoaderRegistry {
    /**
     * Registration where the (underlying hadoop inputformat-based) loader loads
     * items of type X and the result type is also X
     */
    <T> void register(Lang lang, Class<T> targetType, RddRdfLoader<T> loader);

    /**
     * Registration where the (underlying hadoop inputformat-based) loader loads
     * items of type X (such as triples)
     * but the result is mapped to type Y (such as quads)
     */
    <T, X> void registerMapped(Lang lang, Class<T> targetType, RddRdfLoader<X> loader);

    /**
     * Search the registry for a loader that can
     * supply a resource of language 'lang' as records of type rdfType.
     *
     * Returns null if no such loader is found.
     */
    <T> RddRdfLoader<T> find(Lang lang, Class<T> rdfType);
}

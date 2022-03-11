package net.sansa_stack.spark.io.rdf.input.api;

import org.apache.jena.riot.Lang;

public interface RddRdfLoaderRegistry {
    <T> void register(Lang lang, Class<T> targetType, RddRdfLoader<T> loader);

    // Registration where the loader loads items of type X (such as triples)
    // but the result is of type Y (such as quads)
    <T, X> void register2(Lang lang, Class<T> targetType, RddRdfLoader<X> loader);

    <T> RddRdfLoader<T> find(Lang lang, Class<T> rdfType);
}

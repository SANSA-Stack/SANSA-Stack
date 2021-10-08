package net.sansa_stack.rdf.spark.io.input.api;

import org.apache.jena.riot.Lang;

public interface RddRdfLoaderRegistry {
    <T> void register(Lang lang, Class<T> targetType, RddRdfLoader<T> loader);

    public <T, X> void register2(Lang lang, Class<T> targetType, RddRdfLoader<X> loader);

    <T> RddRdfLoader<T> find(Lang lang, Class<T> rdfType);

}

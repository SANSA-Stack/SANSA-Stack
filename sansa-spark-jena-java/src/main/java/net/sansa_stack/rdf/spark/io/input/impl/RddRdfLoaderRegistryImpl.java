package net.sansa_stack.rdf.spark.io.input.impl;

import net.sansa_stack.hadoop.format.jena.trig.FileInputFormatRdfTrigQuad;
import org.apache.jena.ext.com.google.common.collect.HashBasedTable;
import org.apache.jena.ext.com.google.common.collect.Table;
import org.apache.jena.graph.Triple;
import org.apache.jena.hadoop.rdf.io.input.nquads.NQuadsInputFormat;
import org.apache.jena.hadoop.rdf.io.input.ntriples.NTriplesInputFormat;
import org.apache.jena.hadoop.rdf.types.QuadWritable;
import org.apache.jena.hadoop.rdf.types.TripleWritable;
import org.apache.jena.query.Dataset;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Quad;

import net.sansa_stack.hadoop.format.jena.trig.FileInputFormatRdfTrigDataset;
import net.sansa_stack.hadoop.format.jena.turtle.FileInputFormatRdfTurtleTriple;
import net.sansa_stack.rdf.spark.io.input.api.RddRdfLoader;
import net.sansa_stack.rdf.spark.io.input.api.RddRdfLoaderRegistry;

public class RddRdfLoaderRegistryImpl
    implements RddRdfLoaderRegistry
{
    private static RddRdfLoaderRegistry INSTANCE = null;

    public static RddRdfLoaderRegistry get() {
        if (INSTANCE == null) {
            synchronized (RddRdfLoaderRegistryImpl.class) {
                if (INSTANCE == null) {
                    INSTANCE = new RddRdfLoaderRegistryImpl();

                    loadDefaults(INSTANCE);
                }
            }
        }

        return INSTANCE;
    }

    public static void loadDefaults(RddRdfLoaderRegistry registry) {
        registry.register(
                Lang.TRIG,
                Dataset.class,
                (context, path) -> RddRdfLoader.createRdd(context, path, Dataset.class, FileInputFormatRdfTrigDataset.class));

        registry.register(
                Lang.TRIG,
                Quad.class,
                (context, path) -> RddRdfLoader.createRdd(context, path, Quad.class, FileInputFormatRdfTrigQuad.class));

        registry.register(
                Lang.TURTLE,
                Triple.class,
                (context, path) -> RddRdfLoader.createRdd(context, path, Triple.class, FileInputFormatRdfTurtleTriple.class));

//        registry.register(
//                Lang.NTRIPLES,
//                Triple.class,
//                (context, path) -> RddRdfLoader.createRdd(context, path, Triple.class, FileInputFormatTurtleTriple.class));
        registry.register2(
                Lang.NTRIPLES,
                Quad.class,
                (context, path) -> RddRdfLoader.createJavaRdd(context, path, TripleWritable.class, NTriplesInputFormat.class).map(TripleWritable::get).rdd());

        registry.register2(
                Lang.NQUADS,
                Quad.class,
                (context, path) -> RddRdfLoader.createJavaRdd(context, path, QuadWritable.class, NQuadsInputFormat.class).map(QuadWritable::get).rdd());


    }


    protected Table<Lang, Class<?>, RddRdfLoader<?>> registry = HashBasedTable.create();

    @Override
    public <T> void register(Lang lang, Class<T> targetType, RddRdfLoader<T> loader) {
        registry.put(lang, targetType, loader);
    }

    @Override
    public <T, X> void register2(Lang lang, Class<T> targetType, RddRdfLoader<X> loader) {
        registry.put(lang, targetType, loader);
    }


    @SuppressWarnings("unchecked")
    @Override
    public <T> RddRdfLoader<T> find(Lang lang, Class<T> rdfType) {
        RddRdfLoader<?> tmp = registry.get(lang, rdfType);
        return (RddRdfLoader<T>)tmp;
    }

    public RddRdfLoaderRegistryImpl() {
        super();
    }


}

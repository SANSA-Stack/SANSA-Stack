package net.sansa_stack.spark.io.rdf.input.impl;

import net.sansa_stack.hadoop.format.jena.nquads.FileInputFormatRdfNQuads;
import net.sansa_stack.hadoop.format.jena.ntriples.FileInputFormatRdfNTriples;
import net.sansa_stack.hadoop.format.jena.trig.FileInputFormatRdfTrigDataset;
import net.sansa_stack.hadoop.format.jena.trig.FileInputFormatRdfTrigQuad;
import net.sansa_stack.hadoop.format.jena.turtle.FileInputFormatRdfTurtleTriple;
import net.sansa_stack.spark.io.rdf.input.api.RddRdfLoader;
import net.sansa_stack.spark.io.rdf.input.api.RddRdfLoaderRegistry;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.apache.jena.ext.com.google.common.collect.HashBasedTable;
import org.apache.jena.ext.com.google.common.collect.Table;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Quad;

/**
 * A registry for RddRdfLoaders that can supply input of a specific {@link Lang}
 * to an RDD of a requested type (Triples, Quads, Datasets, ett).
 */
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
                DatasetOneNg.class,
                RddRdfLoaders.create(DatasetOneNg.class, FileInputFormatRdfTrigDataset.class));

        registry.register(
                Lang.TRIG,
                Quad.class,
                RddRdfLoaders.create(Quad.class, FileInputFormatRdfTrigQuad.class));

        registry.register(
                Lang.TURTLE,
                Triple.class,
                RddRdfLoaders.create(Triple.class, FileInputFormatRdfTurtleTriple.class));

//        registry.register(
//                Lang.NTRIPLES,
//                Triple.class,
//                (context, path) -> RddRdfLoader.createRdd(context, path, Triple.class, FileInputFormatTurtleTriple.class));
        registry.register(
                Lang.NTRIPLES,
                Triple.class,
                RddRdfLoaders.create(Triple.class, FileInputFormatRdfNTriples.class));

        registry.register(
                Lang.NQUADS,
                Quad.class,
                RddRdfLoaders.create(Quad.class, FileInputFormatRdfNQuads.class));
    }


    protected Table<Lang, Class<?>, RddRdfLoader<?>> registry = HashBasedTable.create();

    @Override
    public <T> void register(Lang lang, Class<T> targetType, RddRdfLoader<T> loader) {
        registry.put(lang, targetType, loader);
    }

    @Override
    public <T, X> void registerMapped(Lang lang, Class<T> targetType, RddRdfLoader<X> loader) {
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

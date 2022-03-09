package net.sansa_stack.spark.io.rdf.input.impl;

import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceCollection;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfQuadsOps;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfTriplesOps;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.LangBuilder;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.jena.riot.WebContent.contentTypeRDFThrift;

public class RdfSourceCollectionImpl
    implements RdfSourceCollection
{
    // Pseudo lang when members use mixed languages
    // Two constants for eventual use with RDFLanguages.isQuadLang()

    public static final Lang MIXED_TRIPLE = LangBuilder.create("mixed-triple", "mixed-triple")
            // .addFileExtensions("rt")
            .build();

    public static final Lang MIXED_QUAD = LangBuilder.create("mixed-quad", "mixed-quad")
            // .addFileExtensions("rt")
            .build();


    protected SparkSession sparkSession;
    protected Collection<RdfSource> members;

    public void add(RdfSource rdfSource) {
        members.add(rdfSource);
    }

    @Override
    public Collection<RdfSource> getMembers() {
        return members;
    }

    @Override
    public boolean containsQuadLangs() {
        boolean result = members.stream().anyMatch(rdfSource -> {
            boolean r = false;
            Lang lang = rdfSource.getLang();
            if (RDFLanguages.isQuads(lang)) {
                r = true;
            }
            return r;
        });
        return result;
    }

    @Override
    public Lang getLang() {
        Set<Lang> langs = members.stream().map(RdfSource::getLang).collect(Collectors.toSet());

        Lang result = langs.size() == 1
                ? langs.iterator().next()
                : containsQuadLangs()
                    ? MIXED_QUAD
                    : MIXED_TRIPLE;

        return result;
    }

    public static <I, T> RDD<T> union(
            SparkSession sparkSession, Collection<I> members, Function<I, RDD<T>> mapper) {
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        RDD<T> result = (RDD<T>)jsc.union(
                members.stream()
                        .map(mapper::apply)
                        .collect(Collectors.toList())
                        .toArray(new JavaRDD[0])).rdd();
        return result;
    }

    @Override
    public RDD<Triple> asTriples() {
        return union(sparkSession, members, RdfSource::asTriples);
    }

    @Override
    public RDD<Quad> asQuads() {
        return union(sparkSession, members, RdfSource::asQuads);
    }

    @Override
    public RDD<Model> asModels() {
        return union(sparkSession, members, RdfSource::asModels);
    }

    @Override
    public RDD<DatasetOneNg> asDatasets() {
        return union(sparkSession, members, RdfSource::asDatasets);
    }
}

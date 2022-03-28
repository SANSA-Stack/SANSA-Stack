package net.sansa_stack.spark.io.rdf.input.impl;

import net.sansa_stack.spark.io.rdf.input.api.RddRdfLoader;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFromResource;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfNamedModelsOps;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfQuadsOps;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfTriplesOps;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.apache.hadoop.fs.Path;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdfSourceFromResourceImpl
    implements RdfSourceFromResource
{
    private static final Logger logger = LoggerFactory.getLogger(RdfSourceFromResourceImpl.class);

    // protected FileSystem fileSystem;
    protected SparkSession sparkSession;
    protected Path path;
    protected Lang lang;
    // protected RddRdfLoaderRegistry registry;

    public RdfSourceFromResourceImpl(SparkSession sparkSession, Path path, Lang lang) {
        super();
        // this.fileSystem = fileSystem;
        this.sparkSession = sparkSession;
        this.path = path;
        this.lang = lang;
    }

    @Override
    public Lang getLang() {
        return lang;
    }

    @Override
    public RDD<Triple> asTriples() {
        RDD<Triple> result;
        RddRdfLoader<Triple> loader = RddRdfLoaderRegistryImpl.get().find(lang, Triple.class);

        if (loader != null) {
            result = loader.load(sparkSession.sparkContext(), path.toString());
        } else {
            if (RDFLanguages.isTriples(lang)) {
                throw new RuntimeException("No triple loader registered for " + lang);
            } else {
                result = asQuads().toJavaRDD().map(Quad::asTriple).rdd();
            }
        }

        return result;
    }

    @Override
    public RDD<Model> asModels() {
        RDD<Model> result;
        RddRdfLoader<Model> loader = RddRdfLoaderRegistryImpl.get().find(lang, Model.class);

        if (loader != null) {
            result = loader.load(sparkSession.sparkContext(), path.toString());
        } else {
            if (RDFLanguages.isTriples(lang)) {
                result = JavaRddOfTriplesOps.groupBySubjects(asTriples().toJavaRDD())
                        .values().rdd();
            } else {
                result = JavaRddOfQuadsOps.groupByNamedGraph(asQuads().toJavaRDD())
                        .values().rdd();
            }
        }

        return result;
    }


    @Override
    public RDD<Quad> asQuads() {
        RDD<Quad> result;
        RddRdfLoader<Quad> loader = RddRdfLoaderRegistryImpl.get().find(lang, Quad.class);

        if (loader != null) {
            result = loader.load(sparkSession.sparkContext(), path.toString());
        } else {
            if (RDFLanguages.isTriples(lang)) {
                result = asTriples().toJavaRDD().map(t -> new Quad(Quad.defaultGraphNodeGenerated, t)).rdd();
            } else {
                throw new RuntimeException("No quad loader registered for " + lang);
            }
        }

        return result;
    }

    @Override
    public RDD<DatasetOneNg> asDatasets() {
        RDD<DatasetOneNg> result;
        RddRdfLoader<DatasetOneNg> loader = RddRdfLoaderRegistryImpl.get().find(lang, DatasetOneNg.class);

        if (loader != null) {
            result = loader.load(sparkSession.sparkContext(), path.toString());
        } else {
            if (RDFLanguages.isTriples(lang)) {
                // TODO groupTriples by subject
                result = JavaRddOfNamedModelsOps.mapToDatasets(JavaRddOfTriplesOps.groupBySubjects(asTriples().toJavaRDD())).rdd();
                // result = asModels().toJavaRDD().map(DatasetFactory::wrap).rdd();
            } else {
                result = JavaRddOfNamedModelsOps.mapToDatasets(JavaRddOfQuadsOps.groupByNamedGraph(asQuads().toJavaRDD())).rdd();
            }
        }

        return result;
    }

    public static <T> RddRdfLoader<T> requireLoader(Lang lang, Class<T> clazz) {
        RddRdfLoader<T> loader = RddRdfLoaderRegistryImpl.get().find(lang, clazz);
        if (loader == null) {
            throw new RuntimeException("No quad loader registered for " + lang + " as " + clazz);
        }
        return loader;
    }

    @Override
    public Model peekDeclaredPrefixes() {
        Model result;

        String pathStr = path.toString();
        if (RDFLanguages.isTriples(lang)) {
            RddRdfLoader<Triple> loader = requireLoader(lang, Triple.class);
            result = loader.peekPrefixes(sparkSession.sparkContext(), pathStr);
        } else if (RDFLanguages.isQuads(lang)) {
            RddRdfLoader<Quad> loader = requireLoader(lang, Quad.class);
            result = loader.peekPrefixes(sparkSession.sparkContext(), pathStr);
        } else {
            // TODO We should extend to result sets - but probably that has to be
            // a different infrastructure than RdfSource (or not?)
            logger.warn("Lang is neither triples nor quads; returning empty set of prefixes");
            result = ModelFactory.createDefaultModel();
        }

        return result;
    }


//    public RDD<Graph> asGraphs() {
//        RDD<DatasetGraph> result;
//        RddRdfLoader<DatasetGraph> loader = RddRdfLoaderRegistryImpl.get().find(lang, Graph.class);
//
//        if (loader != null) {
//            result = loader.load(sparkSession.sparkContext(), path.toString());
//        } else {
//            if (RDFLanguages.isTriples(lang)) {
//                result = asTriples().toJavaRDD().tre  .rdd();
//            } else {
//                result = asDatasetGraphs().toJavaRDD().flatMap(dsg -> dsg.listgraphs()).rdd();
//            }
//        }
//
//        return result;
//    }



//    public RDD<DatasetGraph> asDatasetGraphs() {
//        RDD<DatasetGraph> result;
//        RddRdfLoader<DatasetGraph> loader = RddRdfLoaderRegistryImpl.get().find(lang, DatasetGraph.class);
//
//        if (loader != null) {
//            result = loader.load(sparkSession.sparkContext(), path.toString());
//        } else {
//            if (RDFLanguages.isTriples(lang)) {
//                result = asGraphs().toJavaRDD().map(DatasetGraphFactory::wrap).rdd();
//            } else {
//                result = asQuads().toJavaRDD().map(DatasetFactory::wrap).rdd();
//            }
//        }
//
//        return result;
//    }


}

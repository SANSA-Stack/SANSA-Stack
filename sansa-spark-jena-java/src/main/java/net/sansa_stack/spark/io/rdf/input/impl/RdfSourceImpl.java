package net.sansa_stack.spark.io.rdf.input.impl;

import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfNamedModelsOps;
import org.apache.hadoop.fs.Path;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import net.sansa_stack.spark.io.rdf.input.api.RddRdfLoader;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfQuadsOps;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfTriplesOps;

public class RdfSourceImpl
    implements RdfSource
{
    // protected FileSystem fileSystem;
    protected SparkSession sparkSession;
    protected Path path;
    protected Lang lang;

    // protected RddRdfLoaderRegistry registry;

    public RdfSourceImpl(SparkSession sparkSession, Path path, Lang lang) {
        super();
        // this.fileSystem = fileSystem;
        this.sparkSession = sparkSession;
        this.path = path;
        this.lang = lang;
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
    public RDD<Dataset> asDatasets() {
        RDD<Dataset> result;
        RddRdfLoader<Dataset> loader = RddRdfLoaderRegistryImpl.get().find(lang, Dataset.class);

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

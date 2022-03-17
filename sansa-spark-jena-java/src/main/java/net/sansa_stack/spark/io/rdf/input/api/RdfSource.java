package net.sansa_stack.spark.io.rdf.input.api;

import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.rdd.RDD;

import net.sansa_stack.hadoop.core.RecordReaderGenericBase;

/**
 * Abstraction of a source of RDF. Provides methods to access the data
 * as plain triples / quads or aggregates of subject-/graph-grouped models and datasets.
 *
 * Graph and DatasetGraph are not part of the interface because those
 * would require additional kryo registrations which does not seem worth it.
 *
 * Note: Ideally the methods {@link #asModels()} and {@link #asDatasets()}
 * should only aggregate <b>consecutive</b> triples / quads
 * into such objects. This is the case with {@link InputFormat}s based on
 * {@link RecordReaderGenericBase}.
 * However, several fallback implementations aggregate across
 * the whole RDD which leads to somewhat inconsistent behavior.
 *
 * For this reason, the {@link RdfSourceFactory} should be extended with
 * methods that provide more control over the semantics of implementations of
 * this interface.
 *
 *
 * @author Claus Stadler
 *
 */
public interface RdfSource {
    /** Get the language of the source; may have been probed for by an RdfSourceFactory */
    Lang getLang();

    RDD<Triple> asTriples();
    RDD<Quad> asQuads();
    RDD<Model> asModels();

    /** A stream of datasets having one named graph each */
    RDD<DatasetOneNg> asDatasets();


    Model peekDeclaredPrefixes();
}

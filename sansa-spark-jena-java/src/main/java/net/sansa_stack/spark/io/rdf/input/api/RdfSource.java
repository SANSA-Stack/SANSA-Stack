package net.sansa_stack.spark.io.rdf.input.api;

import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.rdd.RDD;

/**
 * An RdfSource is a NodeTupleSource with tuple size either 3 or 4.
 */
public interface RdfSource
    extends NodeTupleSource
{
    /** Whether this source is based on a quad model */
    boolean usesQuads();

    RDD<Triple> asTriples();
    RDD<Quad> asQuads();
    RDD<Model> asModels();

    /** A stream of datasets having one named graph each */
    RDD<DatasetOneNg> asDatasets();

    /** At present this creates a model holding an RDF sample based on a file's starting bytes.
     * May be changed to PrefixMap
     */
    Model peekDeclaredPrefixes();
}

package net.sansa_stack.spark.io.rdf.input.api;

/** Source of fixed size tuples of RDF nodes */
public interface NodeTupleSource
    extends RdfLikeSource
{
    int getComponentCount();
    // RDD<Node[]> asNodeArrays();
    // RDD<List<Node>> asNodeLists();
}

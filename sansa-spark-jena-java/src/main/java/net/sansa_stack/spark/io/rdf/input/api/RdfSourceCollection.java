package net.sansa_stack.spark.io.rdf.input.api;

import java.util.Collection;

public interface RdfSourceCollection
    extends RdfSource
{
    boolean isEmpty();
    void add(RdfSource rdfSource);

    Collection<RdfSource> getMembers();
    boolean containsQuadLangs();
}

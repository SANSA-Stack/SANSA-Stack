package net.sansa_stack.spark.io.rdf.input.api;

import java.util.Collection;

public interface RdfSourceCollection
    extends RdfSource
{
    Collection<RdfSource> getMembers();
    boolean containsQuadLangs();
}

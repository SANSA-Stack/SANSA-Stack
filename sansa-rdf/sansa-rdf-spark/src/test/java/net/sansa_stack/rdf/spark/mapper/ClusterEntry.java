package net.sansa_stack.rdf.spark.mapper;

import org.aksw.jenax.annotation.reprogen.IriNs;
import org.aksw.jenax.annotation.reprogen.ResourceView;
import org.apache.jena.rdf.model.Resource;

@ResourceView
public interface ClusterEntry
    extends Resource
{
    @IriNs("eg")
    Resource getItem();
    ClusterEntry setItem(Resource res);

    @IriNs("eg")
    Integer getValue();
    ClusterEntry setValue(Integer value);
}

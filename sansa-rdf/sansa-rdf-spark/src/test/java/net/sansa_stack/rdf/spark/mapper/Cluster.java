package net.sansa_stack.rdf.spark.mapper;

import org.aksw.jenax.annotation.reprogen.IriNs;
import org.aksw.jenax.annotation.reprogen.ResourceView;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

import java.util.Set;

@ResourceView
public interface Cluster
    extends Resource
{
    @IriNs("eg")
    Resource getConfig();
    Cluster setConfig(Resource config);

/*
    @IriNs("eg")
    String getName();
    Cluster setName(String name);
*/
    @IriNs("eg")
    RDFNode getClusterTarget();
    Cluster setClusterTarget(RDFNode node);

    @IriNs("eg")
    Set<ClusterEntry> getMembers();
}

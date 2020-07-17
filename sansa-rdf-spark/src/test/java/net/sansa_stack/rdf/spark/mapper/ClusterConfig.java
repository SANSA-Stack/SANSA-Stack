package net.sansa_stack.rdf.spark.mapper;

import org.aksw.jena_sparql_api.mapper.annotation.IriNs;
import org.aksw.jena_sparql_api.mapper.annotation.IriType;
import org.aksw.jena_sparql_api.mapper.annotation.ResourceView;
import org.apache.jena.rdf.model.Resource;

@ResourceView
public interface ClusterConfig
        extends Resource
{
    @IriNs("eg")
    @IriType
    String getAlgorithmIri();
    ClusterConfig getAlgorithmIri(String iri);

    @IriNs("eg")
    Integer getThreshold();
    ClusterConfig setThreshold(Integer value);
}

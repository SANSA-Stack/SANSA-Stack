package net.sansa_stack.rdf.spark.mapper;

import org.aksw.jena_sparql_api.mapper.annotation.IriNs;
import org.aksw.jena_sparql_api.mapper.annotation.IriType;
import org.aksw.jena_sparql_api.mapper.annotation.RdfTypeNs;
import org.aksw.jena_sparql_api.mapper.annotation.ResourceView;
import org.apache.jena.rdf.model.Resource;

@ResourceView
@RdfTypeNs("eg")
public interface LevenshteinMatcherConfig
        extends Resource
{
    @IriNs("eg")
    @IriType
    String getOnProperty();
    LevenshteinMatcherConfig setOnProperty(String iri);

    @IriNs("eg")
    Integer getThreshold();
    LevenshteinMatcherConfig setThreshold(Integer value);
}


@ResourceView
@RdfTypeNs("eg")
interface Labeled
        extends Resource
{
    @IriNs("eg")
    @IriType
    String getLabel();
    Labeled setLabel(String iri);

}

package net.sansa_stack.rdf.spark.mapper;

import org.aksw.jenax.annotation.reprogen.IriNs;
import org.aksw.jenax.annotation.reprogen.IriType;
import org.aksw.jenax.annotation.reprogen.RdfTypeNs;
import org.aksw.jenax.annotation.reprogen.ResourceView;
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

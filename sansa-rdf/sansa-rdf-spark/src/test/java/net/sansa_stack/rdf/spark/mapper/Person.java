package net.sansa_stack.rdf.spark.mapper;

import org.aksw.jena_sparql_api.mapper.annotation.HashId;
import org.aksw.jena_sparql_api.mapper.annotation.Iri;
import org.aksw.jena_sparql_api.mapper.annotation.ResourceView;
import org.apache.jena.rdf.model.Resource;

@ResourceView
public interface Person extends Resource {
    @HashId
    @Iri("http://xmlns.com/foaf/0.1/firstName")
    String getFirstName();
    Person setFirstName(String firstName);

    @HashId
    @Iri("http://xmlns.com/foaf/0.1/givenName")
    String getLastName();
    Person setLastName(String lastName);
}

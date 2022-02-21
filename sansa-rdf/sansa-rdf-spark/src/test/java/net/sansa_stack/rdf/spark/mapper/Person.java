package net.sansa_stack.rdf.spark.mapper;

import org.aksw.jenax.annotation.reprogen.HashId;
import org.aksw.jenax.annotation.reprogen.Iri;
import org.aksw.jenax.annotation.reprogen.ResourceView;
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

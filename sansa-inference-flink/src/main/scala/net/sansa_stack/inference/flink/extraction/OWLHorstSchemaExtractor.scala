package net.sansa_stack.inference.flink.extraction

import org.apache.jena.vocabulary.{OWL2, RDFS}

/**
  * An extractor of the schema for OWL Horst.
  *
  * Currently, it's supports the extraction of triples `(s,p,o)` with `p` being
  *
  *  - rdfs:subClassOf
  *  - rdfs:subPropertyOf
  *  - rdfs:domain
  *  - rdfs:range
  *  - owl:equivalentProperty
  *  - owl:equivalentClass
  *  - owl:inverseOf
  *  - owl:someValuesFrom
  *  - owl:allValuesFrom
  *  - owl:hasValue
  *  - owl:onProperty
  *
  *  or `o` being
  *
  *  - owl:TransitiveProperty
  *  - owl:FunctionalProperty
  *  - owl:InverseFunctionalProperty
  *  - owl:SymmetricProperty
  *
  * @author Lorenz Buehmann
  */
class OWLHorstSchemaExtractor()
    extends SchemaExtractor()(
      Set(
        RDFS.subClassOf,
        RDFS.subPropertyOf,
        RDFS.domain,
        RDFS.range,
        OWL2.equivalentProperty,
        OWL2.equivalentClass,
        OWL2.inverseOf,
        OWL2.someValuesFrom,
        OWL2.allValuesFrom,
        OWL2.hasValue,
        OWL2.onProperty
      ).map(p => p.getURI)
    )(
      Set(
        OWL2.TransitiveProperty,
        OWL2.FunctionalProperty,
        OWL2.InverseFunctionalProperty,
        OWL2.SymmetricProperty
      ).map(p => p.getURI)
    ) {}

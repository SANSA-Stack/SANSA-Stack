package net.sansa_stack.query.spark.ontop;

import it.unibz.inf.ontop.com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.dbschema.DBParameters;
import it.unibz.inf.ontop.exception.MetaMappingExpansionException;
import it.unibz.inf.ontop.spec.mapping.MappingAssertion;
import it.unibz.inf.ontop.spec.mapping.pp.impl.MetaMappingExpander;

public class MetaMappingExpanderSilent implements MetaMappingExpander {
    @Override
    public ImmutableList<MappingAssertion> transform(ImmutableList<MappingAssertion> mappings,
                                                     DBParameters dbParameters) throws MetaMappingExpansionException {
        return mappings;
    }
}

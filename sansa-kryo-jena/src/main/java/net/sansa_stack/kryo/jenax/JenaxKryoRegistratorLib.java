package net.sansa_stack.kryo.jenax;

import com.esotericsoftware.kryo.Kryo;
import org.aksw.jena_sparql_api.rdf.model.ext.dataset.api.NodesInDataset;
import org.aksw.jena_sparql_api.rdf.model.ext.dataset.api.RDFNodeInDataset;
import org.aksw.jena_sparql_api.rdf.model.ext.dataset.impl.GraphNameAndNode;
import org.aksw.jena_sparql_api.rdf.model.ext.dataset.impl.LiteralInDatasetImpl;
import org.aksw.jena_sparql_api.rdf.model.ext.dataset.impl.ResourceInDatasetImpl;

/**
 * Additional serializers for jena related classes which are however not part of the official jena.
 */
public class JenaxKryoRegistratorLib {
    public static void registerClasses(Kryo kryo) {
        kryo.register(NodesInDataset.class, new NodesInDatasetSerializer());
        kryo.register(GraphNameAndNode.class, new GraphNameAndNodeSerializer());

        kryo.register(ResourceInDatasetImpl.class, new RDFNodeInDatasetSerializer<>(RDFNodeInDataset::asResource));
        kryo.register(LiteralInDatasetImpl.class, new RDFNodeInDatasetSerializer<>(RDFNodeInDataset::asLiteral));
    }
}

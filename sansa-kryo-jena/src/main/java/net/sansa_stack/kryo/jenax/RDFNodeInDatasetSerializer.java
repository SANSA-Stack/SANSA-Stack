package net.sansa_stack.kryo.jenax;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.aksw.jenax.arq.dataset.api.RDFNodeInDataset;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;

import java.util.function.Function;

/**
 * Serializer for {@link RDFNodeInDataset}.
 *
 * @author Claus Stadler 2021-10-09
 */
public class RDFNodeInDatasetSerializer<T extends RDFNodeInDataset>
        extends Serializer<T> {

    protected Function<? super RDFNodeInDataset, ? extends T> fn;

    public RDFNodeInDatasetSerializer(Function<? super RDFNodeInDataset, ? extends T> fn) {
        super();
        this.fn = fn;
    }

    public void write(Kryo kryo, Output output, T rdfNodeInDataset) {
        kryo.writeClassAndObject(output, rdfNodeInDataset.asNode());
        kryo.writeObject(output, rdfNodeInDataset.getGraphName());
        kryo.writeClassAndObject(output, rdfNodeInDataset.getDataset());
    }

    public T read(Kryo kryo, Input input, Class<T> clazz) {
        Node node = (Node)kryo.readClassAndObject(input);
        String graphName = kryo.readObject(input, String.class);
        Dataset dataset = (Dataset)kryo.readClassAndObject(input);
        RDFNodeInDataset tmp = RDFNodeInDataset.create(dataset, graphName, node);
        T result = fn.apply(tmp);
        return result;
    }
}
